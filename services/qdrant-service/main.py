# qdrant_service.py
import functools
import os
import uuid
import tempfile
import aiofiles
import httpx
import asyncio
import hashlib
from fastapi import HTTPException
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data

# Опциональные зависимости
try:
    from sentence_transformers import SentenceTransformer
except Exception:
    SentenceTransformer = None

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qmodels
except Exception:
    QdrantClient = None
    qmodels = None

# Для извлечения PDF (если доступно)
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

# -------------------------
# Конфиг по окружению
# -------------------------
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "documents")
EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))
CHUNK_SIZE_CHARS = int(os.getenv("CHUNK_SIZE_CHARS", "3000"))
CHUNK_OVERLAP_CHARS = int(os.getenv("CHUNK_OVERLAP_CHARS", "500"))
MAX_DOWNLOAD_SIZE = int(os.getenv("MAX_DOWNLOAD_SIZE_BYTES", str(200 * 1024 * 1024)))  # 200MB
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "60"))

# -------------------------
# Сервис
# -------------------------
class QdrantService(BaseService):
    """
    Сервис для работы с Qdrant: скачивание файлов/извлечение текста -> чанкинг -> эмбеддинги -> upsert в Qdrant.
    Также умеет выполнять поиск по запросу (embedding-based search) и возвращать найденные чанки.
    """

    def __init__(self):
        super().__init__("qdrant-service", "1.0")

        # httpx клиент (async) для скачивания и вызова внешних сервисов
        self.client = httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT)

        # Хранения кеша модели
        cache_dir = os.getenv("MODEL_CACHE_DIR", "./model_cache")

        # Загрузка эмбеддинговой модели (локально, blocking)
        if SentenceTransformer is None:
            raise RuntimeError("sentence-transformers is required but not installed. Install sentence-transformers.")
        print(f"🔄 Loading embedding model: {EMBEDDING_MODEL_NAME}")
        # Добавляем аргумент cache_folder
        self.embed_model = SentenceTransformer(
            EMBEDDING_MODEL_NAME, 
            cache_folder=cache_dir
        )
        print("✅ Embedding model loaded")
        print(f"✅ Embedding model loaded: {EMBEDDING_MODEL_NAME}")

        # Qdrant client (sync) - оборачиваем в executor для async вызовов
        if QdrantClient is None:
            raise RuntimeError("qdrant-client is required but not installed. Install qdrant-client.")
        print(f"🔄 Connecting to Qdrant at {QDRANT_HOST}:{QDRANT_PORT}")
        if QDRANT_API_KEY:
            self.qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, api_key=QDRANT_API_KEY)
        else:
            self.qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        # Создаем коллекцию если не существует
        try:
            self._ensure_collection_sync()
            print(f"✅ Qdrant collection '{QDRANT_COLLECTION}' ready")
        except Exception as e:
            print(f"❌ Failed to ensure Qdrant collection: {e}")
            raise

        # Лимиты/параметры
        self.chunk_size = CHUNK_SIZE_CHARS
        self.chunk_overlap = CHUNK_OVERLAP_CHARS
        self.embedding_batch = EMBEDDING_BATCH_SIZE

        # Константы поддерживаемых файлов для скачивания
        self.ALLOWED_EXTENSIONS = {".pdf", ".txt",} #".md", ".docx", ".rtf", ".html", ".htm"}
        self.MIME_TO_EXTENSION = {
            "application/pdf": ".pdf",
            "text/plain": ".txt",
            "text/markdown": ".md",
            "application/msword": ".doc",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
            "text/rtf": ".rtf",
            "text/html": ".html",
            "application/octet-stream": ".bin"
        }


    def _can_handle_task_type(self, task_type: str) -> bool:
        supported = [
            "index_document",
            "index_text",
            "search",
            "reindex_document",
        ]
        return task_type in supported

    def _health_handler(self):
        try:
            return {
                "status": "ok",
                "service": self.service_name,
                "qdrant": {"host": QDRANT_HOST, "port": QDRANT_PORT, "collection": QDRANT_COLLECTION},
                "embedding_model": EMBEDDING_MODEL_NAME
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    async def _validate_task(self, task_message: TaskMessage):
        if task_message.data.payload_type == PayloadType.TEXT:
            text = task_message.data.payload.get("text", "")
            if not text or not text.strip():
                raise HTTPException(status_code=400, detail="Text is required for indexing")
            
        elif task_message.data.payload_type == PayloadType.FILE:
            # ожидаем file_url
            file_url = task_message.data.payload.get("file_url", "")
            if not file_url:
                raise HTTPException(status_code=400, detail="file_url is required for index_document")
            
        else:
            raise HTTPException(status_code=400, detail="Unsupported task_type")

    # -------------------------
    # Логика обработки задачи 
    # ------------------------- 
    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        task_type = task_message.data.task_type
        payload_type = task_message.data.payload_type
        if payload_type == PayloadType.FILE:
            return await self._handle_index_document(task_message)
        elif payload_type == PayloadType.TEXT and task_type == "index_text":
            return await self._handle_index_text(task_message)
        elif payload_type == PayloadType.TEXT:
            return await self._handle_search(task_message)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown task_type: {task_type} or/and no support type payload{payload_type}")

    # ---------------------------------------------------------
    # Обработка
    # ---------------------------------------------------------
    async def _process_text_content(self, text: str, doc_id: str, owner: str, origin_url: Optional[str] = None) -> Dict[str, Any]:
        """Общая логика для обработки текста: чанкинг -> эмбеддинг -> upsert"""
        if not text or not text.strip():
             raise HTTPException(status_code=400, detail="Empty text content")

        # Чанкинг
        chunks = self._chunk_text(text, chunk_size=self.chunk_size, overlap=self.chunk_overlap)
        checksum = hashlib.sha256(text.encode("utf-8")).hexdigest()
        
        points = []
        previews = [] # Для эмбеддинга собираем тексты отдельно
        
        for idx, (chunk_text, start_offset) in enumerate(chunks):
            # Приведение типов
            chunk_text_str = str(chunk_text)
            
            qdrant_point_id = str(uuid.uuid4())
            original_chunk_id = f"{doc_id}::chunk::{idx}::{uuid.uuid4().hex}"

            payload_meta = {
                "doc_id": doc_id,
                "owner": owner,
                "offset": start_offset,
                "chunk_index": idx,
                "checksum": checksum,
                "text_preview": chunk_text_str[:500], # Magic number -> constant
                "source_id": original_chunk_id
            }
            if origin_url:
                payload_meta["origin_url"] = origin_url

            points.append({"id": qdrant_point_id, "text": chunk_text_str, "payload": payload_meta})
            previews.append(chunk_text_str)

        # Эмбеддинг (batch processing)
        embeddings = await self._embed_texts(previews)

        # Сборка для Qdrant
        q_points = []
        for p, emb in zip(points, embeddings):
            vec = emb.tolist() if hasattr(emb, "tolist") else list(emb)
            q_points.append({"id": p["id"], "vector": vec, "payload": p["payload"]})

        # Upsert
        upsert_result = await self._qdrant_upsert(q_points)
        
        return {
            "doc_id": doc_id,
            "chunks_count": len(q_points),
            "upsert_result": upsert_result
        }

    # ---------------------------------------------------------
    # Refactored Handlers
    # ---------------------------------------------------------
    async def _handle_index_document(self, task_message: TaskMessage) -> Data:
        payload = task_message.data.payload
        file_url = payload.get("file_url")
        owner = payload.get("owner", "unknown")
        doc_id = payload.get("doc_id") or f"doc-{uuid.uuid4().hex}"

        temp_path = await self._download_file(file_url)
        try:
            text = await self._extract_text_from_file(temp_path)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

        result = await self._process_text_content(text, doc_id, owner, origin_url=file_url)

        return Data(
            payload_type=PayloadType.TEXT,
            task_type="index_document",
            payload={**result, "task": "index_document"},
            execution_metadata={"service": self.service_name}
        )

    async def _handle_index_text(self, task_message: TaskMessage) -> Data:
        payload = task_message.data.payload
        text = payload.get("text", "")
        owner = payload.get("owner", "unknown")
        doc_id = payload.get("doc_id") or f"doc-{uuid.uuid4().hex}"

        result = await self._process_text_content(text, doc_id, owner)

        return Data(
            payload_type=PayloadType.TEXT,
            task_type="index_text",
            payload={**result, "task": "index_text"},
            execution_metadata={"service": self.service_name}
        )


    # -------------------------
    # Search flow
    # -------------------------
    async def _handle_search(self, task_message: TaskMessage) -> Data:
        payload = task_message.data.payload
        query = payload.get("text", "").strip()
        top_k = int(payload.get("top_k", 3))

        # 1. Валидация запроса
        if not query:
            raise HTTPException(status_code=400, detail="Search query cannot be empty.")

        # 2. Эмбеддинг
        try:
            q_embs = await self._embed_texts([query])
            q_emb = q_embs[0]
        except Exception as e:
             raise HTTPException(status_code=500, detail=f"Embedding generation failed: {str(e)}")

        if not self._is_valid_embedding(q_emb):
             raise HTTPException(status_code=400, detail="Generated embedding is invalid (zero vector).")

        # 3. Поиск в Qdrant
        search_results = await self._qdrant_search(
            vector=q_emb,
            top=top_k,
            score_threshold=0.3
        )

        # 4. Формирование ответа (текст + метаданные)
        formatted_text, sources_meta = self._format_results(search_results, query)
        
        # Логика ответа: даже если ничего не найдено, возвращаем Data, но с пустым текстом
        # или сообщением, чтобы пайплайн не падал с ошибкой, а LLM знала, что контекста нет.
        
        final_payload = {
            "text": formatted_text if search_results else "No relevant context found.",
            "query": query, # Возвращаем запрос для контекста
            "found_count": len(search_results)
        }

        # Метаданные отправляем в execution_metadata
        exec_meta = {
            "service": self.service_name,
            "model": EMBEDDING_MODEL_NAME,
            "sources": sources_meta # Список словарей с деталями (url, doc_id, score)
        }

        return Data(
            payload_type=PayloadType.TEXT,
            task_type="search_result",
            payload=final_payload,
            execution_metadata=exec_meta
        )

    def _format_results(self, results: List[Dict[str, Any]], query: str) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Форматирует результаты для LLM: структурированный текст с метаданными источников
        """
        if not results:
            return (
                f"No relevant documents found for query: '{query}'. "
                "Possible reasons: documents not indexed, query too specific, or low relevance threshold.",
                []
            )

        # Формируем контекстный текст для LLM
        context_parts = []
        sources_metadata = []

        # Сначала собираем все источники для ссылок
        sources_index = {}
        for i, hit in enumerate(results):
            payload = hit["payload"]
            source_key = f"{payload.get('doc_id')}_{payload.get('chunk_index')}"
            
            if source_key not in sources_index:
                sources_index[source_key] = {
                    "id": len(sources_index) + 1,
                    "url": payload.get("origin_url"),
                    "doc_id": payload.get("doc_id"),
                    "owner": payload.get("owner")
                }
            
            sources_metadata.append({
                "source_id": sources_index[source_key]["id"],
                "score": round(hit["score"], 4),
                "text_preview": payload.get("text_preview", "")[:200],
                "chunk_index": payload.get("chunk_index")
            })

        # Формируем основной контекст
        context_parts.append(f"Found {len(results)} relevant fragments for query: '{query}'")
        context_parts.append("\nSources:")
        
        # Добавляем список источников
        for source in sources_index.values():
            source_ref = f"[{source['id']}]"
            if source["url"]:
                source_ref += f" URL: {source['url']}"
            else:
                source_ref += f" Document ID: {source['doc_id']}"
            source_ref += f" (Owner: {source['owner']})"
            context_parts.append(source_ref)
        
        context_parts.append("\nRelevant content fragments:")
        
        # Добавляем фрагменты с ссылками на источники
        for i, hit in enumerate(results):
            payload = hit["payload"]
            source_key = f"{payload.get('doc_id')}_{payload.get('chunk_index')}"
            source_id = sources_index[source_key]["id"]
            
            fragment = (
                f"Fragment #{i+1} (Relevance: {hit['score']:.3f}, Source: [{source_id}]):\n"
                f"{payload.get('text_preview', '').strip()}"
            )
            context_parts.append(fragment)

        final_text = "\n".join(context_parts)
        return final_text, sources_metadata

    def _is_valid_embedding(self, emb) -> bool:
        """Проверка, что эмбеддинг не вырожденный (не нулевой вектор)"""
        import numpy as np
        if hasattr(emb, "tolist"):
            emb = emb.tolist()
        return np.linalg.norm(emb) > 0.1  # Минимальная длина вектора

    # -------------------------
    # Утилиты
    # -------------------------
    async def _download_file(self, url: str) -> str:
        """
        Скачивает файл во временное хранилище.
        Основной оркестратор процесса загрузки.
        """
        self._validate_url_security(url)
        original_filename = self._extract_filename_from_url(url)
        
        try:
            tmp_path = await self._stream_file_to_temp(url, original_filename)
            self._validate_downloaded_file(tmp_path)
            return tmp_path
        except Exception as e:
            # Гарантированная очистка при любой ошибке
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def _validate_url_security(self, url: str):
        """Проверяет URL на попытки SSRF атак"""
        parsed = httpx.URL(url)
        blocked_hosts = {"127.0.0.1", "localhost", "0.0.0.0"}
        blocked_suffixes = {".local", ".internal", ".localhost"}
        
        host = parsed.host.lower()
        if host in blocked_hosts or any(host.endswith(suffix) for suffix in blocked_suffixes):
            raise HTTPException(status_code=400, detail="Local network downloads are forbidden")

    def _extract_filename_from_url(self, url: str) -> str:
        """Извлекает имя файла из URL"""
        path = httpx.URL(url).path
        return os.path.basename(path) or "downloaded_file"

    async def _stream_file_to_temp(self, url: str, original_filename: str) -> str:
        """
        Скачивает файл и сохраняет во временное хранилище.
        Возвращает путь к временному файлу.
        """
        async with self.client.stream("GET", url, timeout=DOWNLOAD_TIMEOUT) as response:
            response.raise_for_status()
            
            file_ext = self._determine_file_extension(
                original_filename,
                response.headers.get("content-type", ""),
                response.headers.get("content-disposition", "")
            )
            
            tmp_path = self._create_temp_file_path(file_ext)
            total_size = await self._write_response_to_file(response, tmp_path)
            
            self._log_download_details(original_filename, file_ext, total_size)
            return tmp_path

    def _determine_file_extension(self, filename: str, content_type: str, content_disposition: str) -> str:
        """
        Определяет расширение файла по нескольким источникам в порядке приоритета:
        1. Content-Disposition header
        2. Content-Type header
        3. Имя файла из URL
        """
        # Приоритет 1: Content-Disposition
        if "filename=" in content_disposition:
            disp_filename = self._parse_content_disposition_filename(content_disposition)
            if disp_filename:
                ext = os.path.splitext(disp_filename)[1].lower()
                if ext in self.ALLOWED_EXTENSIONS:
                    return ext
        
        # Приоритет 2: Content-Type
        mime_type = content_type.split(";")[0].strip().lower()
        if mime_type in self.MIME_TO_EXTENSION:
            ext = self.MIME_TO_EXTENSION[mime_type]
            if ext in self.ALLOWED_EXTENSIONS:
                return ext
        
        # Приоритет 3: Имя файла из URL
        ext = os.path.splitext(filename)[1].lower()
        if ext in self.ALLOWED_EXTENSIONS:
            return ext
        
        return ".bin"  # Безопасный fallback

    def _parse_content_disposition_filename(self, header: str) -> str:
        """Парсит имя файла из заголовка Content-Disposition"""
        try:
            # Извлекаем часть после 'filename='
            filename_part = header.split("filename=")[-1].strip()
            # Удаляем обрамляющие кавычки и пробелы
            return filename_part.strip('"\' ')
        except Exception:
            return ""

    def _create_temp_file_path(self, extension: str) -> str:
        """Создает уникальный путь для временного файла"""
        tmp_dir = tempfile.gettempdir()
        unique_name = f"qdrant_{uuid.uuid4().hex}{extension}"
        return os.path.join(tmp_dir, unique_name)

    async def _write_response_to_file(self, response: httpx.Response, file_path: str) -> int:
        """Записывает содержимое ответа в файл с контролем размера"""
        total_size = 0
        async with aiofiles.open(file_path, "wb") as f:
            async for chunk in response.aiter_bytes():
                total_size += len(chunk)
                if total_size > MAX_DOWNLOAD_SIZE:
                    await f.close()
                    raise HTTPException(
                        status_code=413,
                        detail=f"File exceeds maximum size of {MAX_DOWNLOAD_SIZE // (1024*1024)} MB"
                    )
                await f.write(chunk)
        return total_size

    def _validate_downloaded_file(self, file_path: str):
        """Проверяет валидность скачанного файла"""
        if not os.path.exists(file_path):
            raise HTTPException(status_code=500, detail="File creation failed")
        
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            os.unlink(file_path)
            raise HTTPException(status_code=400, detail="Downloaded file is empty")
        
        if file_size > MAX_DOWNLOAD_SIZE:
            os.unlink(file_path)
            raise HTTPException(status_code=413, detail="File size exceeds limit after download")

    def _log_download_details(self, original_name: str, ext: str, size: int):
        """Логирует детали загрузки"""
        print(f"📥 Downloaded: {original_name} -> {ext} ({size} bytes)")

    async def _extract_text_from_file(self, file_path: str) -> str:
        """
        Извлекает текст из файла в зависимости от формата.
        Поддерживаемые форматы: PDF, TXT
        """
        ext = Path(file_path).suffix.lower()
        
        if ext == ".pdf" and fitz is not None:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self._sync_extract_pdf_text, file_path)
        
        elif ext in (".txt", ".text"):
            try:
                async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                    return await f.read()
            except UnicodeDecodeError:
                # Попытка чтения с fallback-кодировкой
                async with aiofiles.open(file_path, "r", encoding="latin-1") as f:
                    return await f.read()
        
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported file format: {ext}. Supported formats: .pdf, .txt"
        )

    def _sync_extract_pdf_text(self, file_path: str) -> str:
        """Извлекает текст из PDF файла с использованием PyMuPDF"""
        try:
            with fitz.open(file_path) as doc:
                return "\n".join(page.get_text("text") for page in doc)
        except Exception as e:
            self.logger.error(f"PDF extraction failed for {file_path}: {str(e)}")
            raise HTTPException(
                status_code=422,
                detail=f"Failed to extract text from PDF: {str(e)}"
            )

    def _chunk_text(self, text: str, chunk_size: int = 1000, overlap: int = 200) -> List[Tuple[str, int]]:
        """
        Разбивает текст на чанки с умным разделением по естественным границам.
        
        Args:
            text: Исходный текст
            chunk_size: Максимальный размер чанка в символах
            overlap: Количество символов перекрытия между чанками
        
        Returns:
            Список кортежей (текст_чанка, начальная_позиция_в_исходном_тексте)
        """
        if not text.strip():
            return []
        
        chunks = []
        start = 0
        text_length = len(text)
        
        while start < text_length:
            # Определяем конец текущего чанка
            end = min(start + chunk_size, text_length)
            
            # Ищем лучшее место для разделения, если не в конце текста
            if end < text_length:
                end = self._find_smart_split_point(text, start, end)
            
            # Добавляем чанк, если он не пустой
            chunk = text[start:end].strip()
            if chunk:
                chunks.append((chunk, start))
            
            # Проверка завершения
            if end >= text_length:
                break
            
            # Вычисляем начало следующего чанка с учетом перекрытия
            next_start = end - overlap
            # Защита от зацикливания и отрицательных значений
            if next_start <= start:
                next_start = end
            start = next_start
        
        return chunks

    def _find_smart_split_point(self, text: str, start: int, end: int) -> int:
        """
        Находит оптимальную точку разделения текста в диапазоне [start, end].
        Приоритет разделителей: абзацы > предложения > пробелы.
        """
        # Зона поиска: последние 20% текущего чанка
        search_zone_start = max(start, end - int((end - start) * 0.2))
        
        # Приоритетные разделители от более значимых к менее
        separators = ["\n\n", "\n", ". ", "! ", "? ", "; ", " "]
        
        for sep in separators:
            # Ищем разделитель справа налево в зоне поиска
            pos = text.rfind(sep, search_zone_start, end)
            if pos != -1:
                return pos + len(sep)  # Возвращаем позицию после разделителя
        
        return end  # Если разделитель не найден, разделяем жестко


    async def _embed_texts(self, texts: List[str]) -> List[Any]:
        """
        Главный метод эмбеддинга. Координирует процесс.
        """
        norm_texts = self._normalize_inputs(texts)
        loop = asyncio.get_event_loop()
        embeddings = []

        for i in range(0, len(norm_texts), self.embedding_batch):
            batch = norm_texts[i : i + self.embedding_batch]
            batch_embeddings = await self._process_embedding_batch(loop, batch, i)
            embeddings.extend(batch_embeddings)
            
        return embeddings

    def _normalize_inputs(self, texts: List[Any]) -> List[str]:
        """Приводит входные данные к строкам."""
        norm_texts = []
        for i, t in enumerate(texts):
            if isinstance(t, str):
                norm_texts.append(t)
            else:
                try:
                    # Лучше использовать logger вместо print
                    print(f"qdrant: coercing input[{i}] type {type(t).__name__} to str")
                    norm_texts.append(str(t))
                except Exception as e:
                    raise RuntimeError(f"Invalid input for embedding at index {i}: {e}")
        return norm_texts

    async def _process_embedding_batch(self, loop, batch: List[str], start_idx: int) -> List[Any]:
        """
        Пытается обработать батч целиком. При ошибке переходит к поштучной обработке.
        """
        # Частичная функция для запуска в executor
        func = functools.partial(
            self.embed_model.encode, 
            batch, 
            convert_to_numpy=True, 
            show_progress_bar=False
        )

        try:
            return await loop.run_in_executor(None, func)
        except Exception as e:
            print(f"Batch embedding failed at idx {start_idx}: {e}. Switching to fallback.")
            return await self._process_batch_fallback(loop, batch, start_idx)

    async def _process_batch_fallback(self, loop, batch: List[str], start_idx: int) -> List[Any]:
        """
        Медленный режим: обрабатывает элементы по одному, чтобы найти битый элемент.
        """
        results = []
        for j, item in enumerate(batch):
            func_single = functools.partial(
                self.embed_model.encode, 
                [item], # encode ожидает список или строку, но для consistency передаем список
                convert_to_numpy=True, 
                show_progress_bar=False
            )
            try:
                # Результат encode для списка — это список векторов. Берем [0] или extend
                emb = await loop.run_in_executor(None, func_single)
                results.extend(emb)
            except Exception as e:
                global_idx = start_idx + j
                print(f"Single encode failed at index {global_idx}")
                raise RuntimeError(f"Embedding failed for item {global_idx} (len={len(item)})") from e
        return results
    
    def _sync_upsert_safe(self, points_batch: List[Dict]) -> int:
        """Синхронный upsert с обработкой исключений"""
        try:
            q_points = self._prepare_points_for_upsert(points_batch)
            self._execute_upsert(q_points)
            return len(q_points)
        except Exception as e:
            self._handle_upsert_error(e, points_batch)
            raise

    def _prepare_points_for_upsert(self, points_batch: List[Dict]) -> List:
        """Подготавливает точки в формате Qdrant"""
        q_points = []
        for point in points_batch:
            q_point = self._convert_single_point(point)
            if q_point:
                q_points.append(q_point)
        return q_points

    def _convert_single_point(self, point: Dict):
        """Конвертирует одну точку в формат Qdrant"""
        # Преобразование вектора
        vector = point["vector"]
        vector_list = self._convert_vector_to_list(vector)
        if not vector_list:
            return None

        # Создание точки в правильном формате
        return qmodels.PointStruct(
            id=str(point["id"]),
            vector=vector_list,
            payload=point.get("payload", {})
        )

    def _convert_vector_to_list(self, vector: Any) -> Optional[List]:
        """Конвертирует вектор в список"""
        try:
            if hasattr(vector, "tolist"):
                return vector.tolist()
            elif isinstance(vector, list):
                return [float(x) for x in vector]
            else:
                return None
        except (ValueError, TypeError):
            return None

    def _execute_upsert(self, q_points: List):
        """Выполняет upsert в Qdrant"""
        if not q_points:
            print("⚠️ No valid points to upsert")
            return

        print(f"📤 Upserting {len(q_points)} points to collection '{QDRANT_COLLECTION}'")
        
        self.qdrant.upsert(
            collection_name=QDRANT_COLLECTION,
            points=q_points,
            wait=True
        )
        
        print(f"✅ Upsert successful: {len(q_points)} points")

    def _handle_upsert_error(self, error: Exception, points_batch: List[Dict]):
        """Обрабатывает ошибки при upsert"""
        print(f"❌ Upsert failed: {str(error)}")
        import traceback
        traceback.print_exc()
        
        # Детальная отладка ошибки
        if self._is_vector_error(error):
            self._debug_vector_issues(points_batch)

    def _is_vector_error(self, error: Exception) -> bool:
        """Определяет, связана ли ошибка с векторами"""
        error_msg = str(error).lower()
        return "wrong input data" in error_msg or "vectors" in error_msg

    def _debug_vector_issues(self, points_batch: List[Dict]):
        """Отладочная информация по проблемам с векторами"""
        print("🔍 Vector validation details:")
        for i, point in enumerate(points_batch[:3]):  # Показываем только первые 3
            self._print_point_debug_info(point, i)

    def _print_point_debug_info(self, point: Dict, index: int):
        """Выводит отладочную информацию по одной точке"""
        vector = point.get("vector")
        print(f"  Point #{index}:")
        print(f"    ID: {point.get('id')}")
        print(f"    Vector type: {type(vector)}")
        
        if hasattr(vector, "__len__"):
            print(f"    Vector length: {len(vector)}")
        else:
            print("    Vector length: unknown")
            
        if hasattr(vector, "shape"):
            print(f"    Vector shape: {vector.shape}")

    async def _upsert_points_one_by_one(self, loop, points: List[Dict]) -> int:
        """Резервный метод: upsert по одной точке"""
        success_count = 0
        for point in points:
            try:
                result = await loop.run_in_executor(
                    None,
                    functools.partial(self._sync_upsert_safe, [point])
                )
                success_count += result
            except Exception as e:
                print(f"❌ Single point upsert failed for {point.get('id')}: {e}")
        return success_count
 
    async def _qdrant_upsert(self, points: List[Dict[str, Any]], batch_size: int = 100) -> Dict[str, Any]:
        """Upsert с полной валидацией и восстановлением при ошибках"""
        
        validated_points = self._validate_points(points)
        if not validated_points:
            return {"upserted": 0, "error": "No valid points to upsert"}

        success_count = await self._process_batches(validated_points, batch_size)
        return {"upserted": success_count}

    def _validate_points(self, points: List[Dict]) -> List[Dict]:
        """Валидация точек данных"""
        validated_points = []
        for point in points:
            try:
                validated_point = self._validate_single_point(point)
                if validated_point:
                    validated_points.append(validated_point)
            except Exception as e:
                print(f"❌ Point validation failed: {e}")
                continue
        return validated_points

    def _validate_single_point(self, point: Dict) -> Optional[Dict]:
        """Валидация одной точки"""
        # Проверяем обязательные поля
        if not point.get("id"):
            point["id"] = str(uuid.uuid4())
            
        if not point.get("vector"):
            print(f"⚠️ Skipping point without vector: {point.get('id')}")
            return None
            
        if not isinstance(point["vector"], list) or len(point["vector"]) == 0:
            print(f"⚠️ Skipping point with invalid vector: {point.get('id')}")
            return None
            
        # Проверяем размер вектора
        expected_size = self.embed_model.get_sentence_embedding_dimension()
        if len(point["vector"]) != expected_size:
            print(f"⚠️ Vector size mismatch for point {point.get('id')}: {len(point['vector'])} != {expected_size}")
            return None
            
        return point

    async def _process_batches(self, validated_points: List[Dict], batch_size: int) -> int:
        """Обработка точек батчами"""
        loop = asyncio.get_event_loop()
        success_count = 0

        for i in range(0, len(validated_points), batch_size):
            batch = validated_points[i:i + batch_size]
            batch_success = await self._process_single_batch(loop, batch, i)
            success_count += batch_success
            
        return success_count

    async def _process_single_batch(self, loop, batch: List[Dict], batch_index: int) -> int:
        """Обработка одного батча"""
        try:
            result = await loop.run_in_executor(
                None, 
                functools.partial(self._sync_upsert_safe, batch)
            )
            print(f"✅ Successfully upserted batch {batch_index//len(batch) + 1}: {result} points")
            await asyncio.sleep(0.1)
            return result
        except Exception as e:
            print(f"❌ Batch upsert failed at index {batch_index}: {e}")
            # Fallback: upsert по одному
            return await self._upsert_points_one_by_one(loop, batch)


    #! ТЕСТОВЫЙ МЕТОД ДЛЯ ПРОВЕРКИ ВСЕХ ДОКУМЕНТОВ ИЗ КДРАНТ
    def _debug_get_all_documents(self, limit: int = 5):
        """Получить информацию о всех документах в коллекции"""
        try:
            # Получаем уникальные doc_id
            # Используем scroll для получения всех точек
            all_points = []
            next_page = None
            while len(all_points) < limit * 10:  # Берем с запасом
                scroll_result = self.qdrant.scroll(...)
                points = scroll_result.points
                next_page = scroll_result.next_page_offset
                all_points.extend(points)
                if next_page is None:
                    break
            
            # Группируем по doc_id
            from collections import defaultdict
            docs = defaultdict(list)
            for point in all_points:
                doc_id = point.payload.get("doc_id")
                if doc_id:
                    docs[doc_id].append(point)
            
            # Формируем результат
            result = []
            for doc_id, points in list(docs.items())[:limit]:  # Ограничиваем количество
                if not points:
                    continue
                    
                # Сортируем чанки по позиции
                chunks = sorted(points, key=lambda x: x.payload.get('offset', 0))
                last_chunk = chunks[-1]
                last_words = last_chunk.payload.get('text_preview', '').strip().split()[-3:]
                
                result.append({
                    "doc_id": doc_id,
                    "total_chunks": len(points),
                    "first_chunk_preview": chunks[0].payload.get('text_preview', '')[:50] + "...",
                    "last_chunk_preview": last_chunk.payload.get('text_preview', '')[:50] + "...",
                    "last_words": " ".join(last_words),
                    "total_characters": sum(len(p.payload.get('text_preview', '')) for p in points)
                })
            
            return result
            
        except Exception as e:
            print(f"❌ DEBUG ERROR: {str(e)}")
            return [{"error": str(e)}]




    async def _qdrant_search(self, vector: Any, top: int = 6, score_threshold: float = 0.3) -> List[Dict[str, Any]]:
        """
        Правильный семантический поиск для qdrant-client >= 1.7.0
        """
        loop = asyncio.get_event_loop()

        def _sync_query():
            # Конвертируем вектор в правильный формат
            q_vec = vector.tolist() if hasattr(vector, "tolist") else [float(x) for x in vector]
            
            # === ПРАВИЛЬНЫЙ ВЫЗОВ ДЛЯ СОВРЕМЕННОГО QDRANT ===
            resp = self.qdrant.query_points(
                collection_name=QDRANT_COLLECTION,
                query=q_vec,  
                using=None,   
                limit=top,
                with_payload=True,
                score_threshold=score_threshold,  # ФИЛЬТРАЦИЯ ПО РЕЛЕВАНТНОСТИ
                with_vectors=False
            )
            
            # === ПРАВИЛЬНАЯ ОБРАБОТКА РЕЗУЛЬТАТОВ ===
            hits = []
            for point in resp.points:
                hits.append({
                    "id": str(point.id),
                    "score": float(point.score),
                    "payload": point.payload or {},
                })
                # Отладка для каждого результата
                preview = point.payload.get('text_preview', '')[:100] if point.payload else ''
                print(f"  📌 Hit (score={point.score:.4f}): {preview}...")
            
            print(f"✅ Found {len(hits)} relevant results (score_threshold={score_threshold})")

            #! ДЕБАГ МОМЕНТ СЛЕДУЕТ УБРАТЬ ЭТО
            print("\n" + "="*60)
            print("🐞 DEBUG ALL DOCUMENTS (first 5)")
            print("="*60)
            docs = self._debug_get_all_documents(limit=5)
            
            for i, doc in enumerate(docs):
                print(f"\n📄 Document #{i+1}: {doc.get('doc_id', 'N/A')}")
                print(f"   Chunks: {doc.get('total_chunks', 'N/A')}")
                print(f"   First: '{doc.get('first_chunk_preview', '')}'")
                print(f"   Last:  '{doc.get('last_chunk_preview', '')}'")
                print(f"   Final words: '{doc.get('last_words', '')}'")
        
            print("="*60 + "\n")

            return hits

        return await loop.run_in_executor(None, _sync_query)
    
    def _ensure_collection_sync(self):
        """Создание коллекции"""
        try:
            try:
                self.qdrant.get_collection(collection_name=QDRANT_COLLECTION)
                print(f"✅ Collection '{QDRANT_COLLECTION}' already exists")
                return
            except Exception as e:
                # Пропускаем, если ошибка "not found", иначе рейзим
                if "not found" not in str(e).lower() and "404" not in str(e):
                    print(f"⚠️ Collection check warning: {str(e)}")

            vector_size = self.embed_model.get_sentence_embedding_dimension()
            print(f"🔄 Creating collection '{QDRANT_COLLECTION}' with vector size {vector_size}")

            # ИСПРАВЛЕННАЯ КОНФИГУРАЦИЯ
            self.qdrant.create_collection(
                collection_name=QDRANT_COLLECTION,
                vectors_config=qmodels.VectorParams(
                    size=vector_size, 
                    distance=qmodels.Distance.COSINE
                ),
                # Убираем default_segment_number=1.
                # Оставляем пустым или дефолтным. Это снизит риск коррупции при сбоях.
                hnsw_config=qmodels.HnswConfigDiff(
                    m=16,
                    ef_construct=100,
                )
            )
            print("✅ Collection created successfully")
            
        except Exception as e:
            print(f"❌ Collection setup failed: {e}")
            raise

    def close(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.client.aclose())


# Создаем и запускаем сервис при старте
service = QdrantService()

if __name__ == "__main__":
    service.run()
