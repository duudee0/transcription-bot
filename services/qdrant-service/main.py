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
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
QDRANT_HOST = os.getenv("QDRANT_HOST", "127.0.0.1")
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

        # Загрузка эмбеддинговой модели (локально, blocking)
        if SentenceTransformer is None:
            raise RuntimeError("sentence-transformers is required but not installed. Install sentence-transformers.")
        print(f"🔄 Loading embedding model: {EMBEDDING_MODEL_NAME}")
        self.embed_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
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

        #! ТЕСТ ЭМБЕДИНГА
        # print("\n💾 TESTING EMBEDING MODEL ")
        # print(self.embed_model.encode(["hi","i"]))

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

    # -------------------------
    # Indexing flows
    # -------------------------
    async def _handle_index_document(self, task_message: TaskMessage) -> Data:
        payload = task_message.data.payload
        file_url = payload.get("file_url")
        owner = payload.get("owner", "unknown")

        # скачиваем и извлекаем текст
        temp_path = await self._download_file(file_url)
        try:
            text = await self._extract_text_from_file(temp_path)

        finally:
            # удаляем файл
            if os.path.exists(temp_path):
                os.unlink(temp_path)

        if not text or not text.strip():
            raise HTTPException(status_code=400, detail="No text extracted from document")

        # Чанкинг
        chunks = self._chunk_text(text, chunk_size=self.chunk_size, overlap=self.chunk_overlap)

        # Создаём мета для документа
        doc_id = payload.get("doc_id") or f"doc-{uuid.uuid4().hex}"
        # вычисляем checksum всего текста
        checksum = hashlib.sha256(text.encode("utf-8")).hexdigest()

        # Подготовка точек для индексации
        points = []
        for idx, (chunk_text, start_offset) in enumerate(chunks):
            # Сохраняем оригинальный читаемый id, но НЕ используем его как point id в Qdrant
            original_chunk_id = f"{doc_id}::chunk::{idx}::{uuid.uuid4().hex}"
            # Генерируем валидный для Qdrant id (UUID string)
            qdrant_point_id = str(uuid.uuid4())

            # Приводим текст чанка к строке (страх от нестрок)
            chunk_text_str = chunk_text if isinstance(chunk_text, str) else str(chunk_text)

            payload_meta = {
                "doc_id": doc_id,
                "owner": owner,
                "offset": start_offset,
                "chunk_index": idx,
                "origin_url": file_url,
                "checksum": checksum,
                "text_preview": chunk_text_str[:500],
                "source_id": original_chunk_id  # сохраняем читабельный id
            }
            points.append({"id": qdrant_point_id, "text": chunk_text_str, "payload": payload_meta})

        # Получаем embeddings батчами (blocking)
        embeddings = await self._embed_texts([p["text"] for p in points])

        # Формируем запись для Qdrant (id уже валидны)
        q_points = []
        for p, emb in zip(points, embeddings):
            vec = emb.tolist() if hasattr(emb, "tolist") else list(emb)
            q_points.append({"id": p["id"], "vector": vec, "payload": p["payload"]})

        # Upsert в Qdrant
        upserted = await self._qdrant_upsert(q_points)

        return Data(
            payload_type=PayloadType.TEXT,
            task_type="index_document",
            payload={
                "task": "index_document",
                "doc_id": doc_id,
                "chunks_indexed": len(q_points),
                "upsert_result": upserted
            },
            execution_metadata={"service": self.service_name}
        )


    async def _handle_index_text(self, task_message: TaskMessage) -> Data:
        payload = task_message.data.payload
        text = payload.get("text", "")
        owner = payload.get("owner", "unknown")
        doc_id = payload.get("doc_id") or f"doc-{uuid.uuid4().hex}"

        chunks = self._chunk_text(text, chunk_size=self.chunk_size, overlap=self.chunk_overlap)

        points = []
        for idx, (chunk_text, start_offset) in enumerate(chunks):
            original_chunk_id = f"{doc_id}::chunk::{idx}::{uuid.uuid4().hex}"
            qdrant_point_id = str(uuid.uuid4())
            chunk_text_str = chunk_text if isinstance(chunk_text, str) else str(chunk_text)

            payload_meta = {
                "doc_id": doc_id,
                "owner": owner,
                "offset": start_offset,
                "chunk_index": idx,
                "text_preview": chunk_text_str[:500],
                "source_id": original_chunk_id
            }
            points.append({"id": qdrant_point_id, "text": chunk_text_str, "payload": payload_meta})

        embeddings = await self._embed_texts([p["text"] for p in points])

        q_points = []
        for p, emb in zip(points, embeddings):
            vec = emb.tolist() if hasattr(emb, "tolist") else list(emb)
            q_points.append({"id": p["id"], "vector": vec, "payload": p["payload"]})

        upserted = await self._qdrant_upsert(q_points)

        return Data(
            payload_type=PayloadType.TEXT,
            task_type="index_text",
            payload={
                "task": "index_text",
                "doc_id": doc_id,
                "chunks_indexed": len(q_points),
                "upsert_result": upserted
            },
            execution_metadata={"service": self.service_name}
        )

    # -------------------------
    # Search flow
    # -------------------------
    async def _handle_search(self, task_message: TaskMessage) -> Data:
        payload = task_message.data.payload
        query = payload.get("text", "").strip()  # Обязательно .strip()!
        top_k = int(payload.get("top_k", 6))
        
        # === КРИТИЧЕСКАЯ ПРОВЕРКА: ЗАПРЕЩАЕМ ПУСТЫЕ ЗАПРОСЫ ===
        if not query:
            raise HTTPException(
                status_code=400,
                detail="Search query cannot be empty. Please provide a meaningful question."
            )
        
        print(f"🔍 Processing search query: '{query}'")  # Для отладки
        
        # Получаем embedding для запроса
        q_embs = await self._embed_texts([query])
        q_emb = q_embs[0]
        
        # === ВАЛИДАЦИЯ ЭМБЕДДИНГА ===
        if not self._is_valid_embedding(q_emb):
            raise HTTPException(
                status_code=400,
                detail="Failed to generate meaningful embedding for the query. Try rephrasing."
            )
        
        # Поиск в Qdrant с фильтрацией по минимальному скору
        search_results = await self._qdrant_search(
            vector=q_emb, 
            top=top_k,
            score_threshold=0.3  # Минимальное сходство для релевантных результатов
        )
        
        # Если нет релевантных результатов — возвращаем пустой ответ с подсказкой
        if not search_results:
            return Data(
                payload_type=PayloadType.TEXT,
                task_type="search",
                payload={
                    "task": "search",
                    "query": query,
                    "results": [],
                    "message": "No relevant results found. Try rephrasing your question."
                },
                execution_metadata={"service": self.service_name}
            )
        
        return Data(
            payload_type=PayloadType.TEXT,
            task_type="search",
            payload={
                "task": "search",
                "query": query,
                "results": search_results
            },
            execution_metadata={"service": self.service_name}
        )

    def _is_valid_embedding(self, emb) -> bool:
        """Проверка, что эмбеддинг не вырожденный (не нулевой вектор)"""
        import numpy as np
        if hasattr(emb, "tolist"):
            emb = emb.tolist()
        return np.linalg.norm(emb) > 0.1  # Минимальная длина вектора

    # -------------------------
    # Утилиты: скачивание, извлечение, чарнки, эмбеддинги, qdrant ops
    # -------------------------
    async def _download_file(self, url: str) -> str:
        """Скачивает файл в temp и возвращает путь"""
        # Простая защита: запрещаем локальные адреса (SSRF)
        parsed = httpx.URL(url)
        if parsed.host in ("127.0.0.1", "localhost"):
            raise HTTPException(status_code=400, detail="Localhost downloads are forbidden")

        tmp_dir = tempfile.gettempdir()
        tmp_name = f"qdrant_{uuid.uuid4().hex}.pdf"
        tmp_path = os.path.join(tmp_dir, tmp_name)

        try:
            async with self.client.stream("GET", url, timeout=DOWNLOAD_TIMEOUT) as response:
                response.raise_for_status()
                total = 0
                async with aiofiles.open(tmp_path, "wb") as f:
                    async for chunk in response.aiter_bytes():
                        total += len(chunk)
                        if total > MAX_DOWNLOAD_SIZE:
                            await f.close()
                            raise HTTPException(status_code=400, detail="File too large")
                        await f.write(chunk)
        except httpx.RequestError as e:
            raise HTTPException(status_code=400, detail=f"Failed to download: {str(e)}")
        return tmp_path

    async def _extract_text_from_file(self, file_path: str) -> str:
        """Попытка извлечь текст из файла: pdf -> text, txt -> decode"""
        # Если PDF и установлен fitz (PyMuPDF)
        ext = Path(file_path).suffix.lower()
        if ext in (".pdf",) and fitz is not None:
            # blocking -> run in executor
            loop = asyncio.get_event_loop()
            text = await loop.run_in_executor(None, self._sync_extract_pdf_text, file_path)
            return text
        else:
            # пробуем прочитать как текст
            # try:
            #     async with aiofiles.open(file_path, "rb") as f:
            #         data = await f.read()
            #         try:
            #             return data.decode("utf-8")
            #         except Exception:
            #             try:
            #                 return data.decode("latin-1")
            #             except Exception:
            #                 return ""
            # except Exception:
            return ""

    def _sync_extract_pdf_text(self, file_path: str) -> str:
        """Синхронная извлечь текст из pdf через PyMuPDF"""
        try:
            doc = fitz.open(file_path)
            parts = []
            for page in doc:
                parts.append(page.get_text("text"))
            return "\n".join(parts)
        except Exception:
            return ""

    def _chunk_text(self, text: str, chunk_size: int = 3000, overlap: int = 500) -> List[Tuple[str, int]]:
        """
        Разбивает текст на чанки по символам с перекрытием.
        Возвращает список (chunk_text, start_offset)
        """
        if not text:
            return []
        length = len(text)
        chunks = []
        start = 0
        while start < length:
            end = start + chunk_size
            chunk = text[start:end]
            chunks.append((chunk, start))
            if end >= length:
                break
            # сдвигаем на chunk_size - overlap
            start = end - overlap
        return chunks

    async def _embed_texts(self, texts: List[str]) -> List[Any]:
        """
        Надёжный батчевый эмбеддинг: проверяем вход, логируем проблему и используем именованные аргументы.
        Возвращает список эмбеддингов (numpy arrays / list).
        """
        # 1) Нормализуем вход: приводим всё к строкам (и логируем случаи приведения)
        norm_texts = []
        for i, t in enumerate(texts):
            if isinstance(t, str):
                norm_texts.append(t)
            else:
                try:
                    s = str(t)
                    print("qdrant: embed_texts coerced input[%d] of type %s to str", i, type(t).__name__)
                    norm_texts.append(s)
                except Exception as e:
                    print("qdrant: cannot coerce input[%d] to str: %s", i, e)
                    raise RuntimeError(f"Invalid input type for embedding at index {i}: {type(t)}") from e

        loop = asyncio.get_event_loop()
        embeddings = []

        for i in range(0, len(norm_texts), self.embedding_batch):
            batch = norm_texts[i:i + self.embedding_batch]
            # Логируем кратко содержимое батча (первые 3 элементов) для отладки
            print("qdrant: embedding batch start_idx=%d size=%d sample_types=%s", i, len(batch),
                        [type(x).__name__ for x in batch[:3]])

            # Вызываем encode в executor с именованными аргументами
            func = functools.partial(self.embed_model.encode,
                                    batch,
                                    convert_to_numpy=True,
                                    show_progress_bar=False)
            try:
                emb = await loop.run_in_executor(None, func)
                embeddings.extend(emb)
            except Exception as exc:
                # Подробный лог перед попыткой поэлементного fallback
                print("qdrant: embedding failed on batch starting at %d: %s", i, exc)
                print("qdrant: problematic batch preview: %s", [repr(x)[:300] for x in batch[:10]])

                # fallback: попробовать эмбеддить элементы по одному, чтобы найти проблемный
                for j, single in enumerate(batch):
                    try:
                        func_single = functools.partial(self.embed_model.encode,
                                                        [single],
                                                        convert_to_numpy=True,
                                                        show_progress_bar=False)
                        single_emb = await loop.run_in_executor(None, func_single)
                        embeddings.extend(single_emb)
                    except Exception as e2:
                        print("qdrant: single encode failed at global_index=%d: %s", i + j, e2)
                        # Поднимаем понятную ошибку с индексом проблемного элемента
                        raise RuntimeError(f"Embedding failed for item index {i + j}: type={type(single).__name__}, repr={repr(single)[:500]}") from e2
        return embeddings

    async def _qdrant_upsert(self, points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Upsert points в Qdrant (использует sync client в executor).
        points: [{"id":..., "vector":[...], "payload": {...}}, ...]
        """
        loop = asyncio.get_event_loop()
        def _sync_upsert():
            # prepare qdrant points
            q_points = []
            for p in points:
                q_points.append(qmodels.PointStruct(id=p["id"], vector=p["vector"], payload=p["payload"]))
            self.qdrant.upsert(collection_name=QDRANT_COLLECTION, points=q_points)
            return {"upserted": len(q_points)}
        return await loop.run_in_executor(None, _sync_upsert)





    #! ТЕСТОВЫЙ МЕТОД ДЛЯ ПРОВЕРКИ ВСЕХ ДОКУМЕНТОВ ИЗ КДРАНТ
    def _debug_get_all_documents(self, limit: int = 5):
        """Получить информацию о всех документах в коллекции"""
        try:
            # Получаем уникальные doc_id
            # Используем scroll для получения всех точек
            all_points = []
            next_page = None
            while len(all_points) < limit * 10:  # Берем с запасом
                points, next_page = self.qdrant.scroll(
                    collection_name=QDRANT_COLLECTION,
                    limit=100,
                    offset=next_page,
                    with_payload=["doc_id", "text_preview", "offset"]
                )
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
            q_vec = [float(x) for x in vector.tolist()] if hasattr(vector, "tolist") else [float(x) for x in vector]
            
            # === ПРАВИЛЬНЫЙ ВЫЗОВ ДЛЯ СОВРЕМЕННОГО QDRANT ===
            resp = self.qdrant.query_points(
                collection_name=QDRANT_COLLECTION,
                query=q_vec,  # ✅ ПРАВИЛЬНО ДЛЯ query_points()
                using=None,   # ✅ None для default вектора (или имя если несколько векторов)
                limit=top,
                with_payload=True,
                score_threshold=0.3,  # ✅ ФИЛЬТРАЦИЯ ПО РЕЛЕВАНТНОСТИ
                with_vectors=False
            )
            
            # === ПРАВИЛЬНАЯ ОБРАБОТКА РЕЗУЛЬТАТОВ ===
            hits = []
            for pt in resp.points:  # ✅ resp.points вместо resp.result
                # Отладка: показываем preview для каждого результата
                preview = pt.payload.get('text_preview', '')
                print(f"🔍 Hit (score={pt.score}): {preview}...")
                
                hits.append({
                    "id": pt.id,
                    "score": pt.score,  # Для косинуса: 0.0-1.0
                    "payload": pt.payload,
                })
            
            print(f"✅ Found {len(hits)} relevant results (score_threshold=0.3)")
            # 🔥 НОВЫЙ РЕЖИМ: ПРОВЕРКА ВСЕХ ДОКУМЕНТОВ
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
        """Синхронно создаёт коллекцию если не существует (используется в init)"""
        # Проверка наличия коллекции
        collections = self.qdrant.get_collections().collections
        names = [c.name for c in collections]
        if QDRANT_COLLECTION not in names:
            # создаём collection с размером вектора из модели
            vector_size = self.embed_model.get_sentence_embedding_dimension()
            self.qdrant.create_collection(
                collection_name=QDRANT_COLLECTION,
                vectors_config=qmodels.VectorParams(size=vector_size, distance=qmodels.Distance.COSINE)
            )

    def close(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.client.aclose())


# Создаем и запускаем сервис при старте
service = QdrantService()

if __name__ == "__main__":
    service.run()
