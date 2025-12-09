# xtts_silero_pkg_service.py
import os
import re
import uuid
import asyncio
import threading
import shutil
from pathlib import Path
from typing import Optional, List, Dict

import httpx
import aiofiles
from fastapi import HTTPException

# --- важное: задать переменные окружения перед импортом silero_tts ---
SILERO_CACHE_DIR = Path(os.getenv("SILERO_CACHE_DIR", "/app/silero_cache"))
SILERO_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Подстраховка: XDG_CACHE_HOME часто используется библиотеками для кеша.
os.environ.setdefault("XDG_CACHE_HOME", str(SILERO_CACHE_DIR))
os.environ.setdefault("SILERO_CACHE_DIR", str(SILERO_CACHE_DIR))

# --- теперь импортируем silero_tts (он может импортировать torch внутри себя) ---
try:
    from silero_tts import SileroTTS
except ImportError:
    try:
        from silero_tts.silero_tts import SileroTTS
    except ImportError:
        raise ImportError("Cannot import SileroTTS from 'silero-tts' package")

# Ваши общие модули (не менять)
from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data

# Конфиг (env)
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/app/audio_outputs"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "http://localhost:8000")
DEFAULT_LANGUAGE = os.getenv("SILERO_DEFAULT_LANGUAGE", "ru").lower()
DEFAULT_RU_SPEAKER = os.getenv("SILERO_RU_SPEAKER", "xenia")
DEFAULT_EN_SPEAKER = os.getenv("SILERO_EN_SPEAKER", "bengali_male")
USE_CUDA = os.getenv("USE_CUDA", "False").lower() == "true"
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "60"))
XTTS_TIMEOUT = int(os.getenv("XTTS_TIMEOUT", "300"))

# Опции кэширования / предзагрузки
PRELOAD_ON_START = os.getenv("SILERO_PRELOAD", "False").lower() in ("1", "true", "yes")
PRELOAD_LANGS = [l.strip().lower() for l in os.getenv("SILERO_PRELOAD_LANGS", "ru,en").split(",") if l.strip()]
# Внешняя папка кеша (используется для отображения/диагностики)
CACHE_DIR = SILERO_CACHE_DIR

# HTTP client for downloads (kept for parity)
_client = httpx.AsyncClient(timeout=XTTS_TIMEOUT)


class SileroService(BaseService):
    """
    Silero TTS service с локальным кешем моделей, preloading-ом и diagnostics-endpoints.
    """
    def __init__(self):
        super().__init__("xtts-v2-service-silero-pkg", "1.0")

        self.output_dir = OUTPUT_DIR
        self.service_base_url = SERVICE_BASE_URL
        self.timeout = XTTS_TIMEOUT
        self.download_timeout = DOWNLOAD_TIMEOUT

        # cache SileroTTS instances per language
        self._tts_objects: Dict[str, SileroTTS] = {}
        # lock for thread-safety during creation / preload
        self._tts_lock = threading.Lock()

        # cache dir for diagnostics
        self._cache_dir = CACHE_DIR

        # register admin endpoints if app exists
        try:
            # routes are simple and only registered if BaseService exposes self.app (FastAPI)
            self.app.post("/admin/preload")(self._http_preload)
            self.app.get("/admin/cache_status")(self._http_cache_status)
            self.app.get("/admin/cache_info")(self._http_cache_info)
            print("✅ Registered admin endpoints: /admin/preload, /admin/cache_status, /admin/cache_info")
        except Exception:
            print("⚠️ Could not register admin endpoints (no self.app). You can still call methods programmatically.")

        # Optionally start preloading in background thread
        if PRELOAD_ON_START:
            t = threading.Thread(target=self._preload_models_sync, daemon=True)
            t.start()
            print(f"ℹ️ SILERO_PRELOAD enabled. Preloading langs: {PRELOAD_LANGS}")

                # mount static files if BaseService provides FastAPI app
        try:
            # используем явный импорт StaticFiles (чтобы не полагаться на __import__ hack)
            from fastapi.staticfiles import StaticFiles
            self.app.mount("/audio", StaticFiles(directory=self.output_dir), name="audio")
            print(f"✅ Mounted /audio -> {self.output_dir}")
        except AttributeError:
            # BaseService может не иметь self.app на момент инициализации — это нормально
            print("⚠️ BaseService has no self.app; static /audio not auto-mounted")


    # ---------------------------
    # Diagnostics HTTP handlers
    # ---------------------------
    async def _http_preload(self):
        """
        HTTP handler: запускает предзагрузку в фоне (не блокирует запрос).
        Возвращает job id (uuid).
        """
        job_id = uuid.uuid4().hex
        t = threading.Thread(target=self._preload_models_sync, args=(job_id,), daemon=True)
        t.start()
        return {"status": "started", "job_id": job_id, "langs": PRELOAD_LANGS}

    async def _http_cache_status(self):
        """
        Возвращает список языков, для которых уже созданы in-memory экземпляры TTS,
        и путь к кеш директорий.
        """
        return {
            "models_in_memory": list(self._tts_objects.keys()),
            "cache_dir": str(self._cache_dir),
        }

    async def _http_cache_info(self):
        """
        Возвращает краткую информацию о содержимом кеш-директории: несколько последних файлов.
        """
        try:
            files = sorted([p for p in self._cache_dir.rglob("*") if p.is_file()],
                           key=lambda p: p.stat().st_mtime, reverse=True)[:50]
            return {"cache_dir": str(self._cache_dir), "recent_files": [str(p.relative_to(self._cache_dir)) for p in files]}
        except Exception as e:
            return {"error": str(e)}

    # ---------------------------
    # Preload & cache helpers
    # ---------------------------
    def _preload_models_sync(self, job_id: Optional[str] = None):
        """
        Синхронная предзагрузка (вызывается из фонового потока).
        Создаёт экземпляры SileroTTS для языков из PRELOAD_LANGS и пытается зарегистрировать найденные артефакты.
        """
        print(f"🔁 Preload job started: {job_id} langs={PRELOAD_LANGS}")
        for lang in PRELOAD_LANGS:
            try:
                # вызываем get_or_create — он безопасен для потоков благодаря lock
                tts = self._get_or_create_tts(language=lang, speaker=None)
                # best-effort: зарегистрировать артефакты (копирование в cache/lang_<lang>)
                self._register_model_artifacts(tts, lang)
                print(f"✅ Preloaded lang {lang}")
            except Exception as e:
                print(f"⚠️ Preload lang {lang} failed: {e}")
        print(f"🔁 Preload job finished: {job_id}")

    def _register_model_artifacts(self, tts_obj, lang: str):
        """
        Best-effort: пытается найти пути/атрибуты внутри tts_obj, которые содержат пути к файлам,
        и скопировать/зафиксировать их в CACHE_DIR/lang_<lang>.
        Не обязательная операция — служит для визуальной уверенности, что артефакты на диске.
        """
        candidates = []
        # типичные имена атрибутов, где могут храниться пути
        attr_names = ["model_path", "model_dir", "weights_path", "bundle_path", "repo_path", "package_path", "local_dir", "checkpoint_path"]

        for name in attr_names:
            try:
                val = getattr(tts_obj, name, None)
            except Exception:
                val = None
            if not val:
                continue
            try:
                p = Path(val)
                if p.exists():
                    candidates.append(p)
            except Exception:
                continue

        # если не нашли — проверим кеш директорию на свежие файлы и покажем их
        if not candidates:
            try:
                recent = sorted(self._cache_dir.rglob("*"), key=lambda p: p.stat().st_mtime, reverse=True)[:10]
                if recent:
                    print(f"ℹ️ No explicit model attrs; recent files in cache_dir: {[str(p) for p in recent[:5]]}")
            except Exception:
                pass
            return

        target_root = self._cache_dir / f"lang_{lang}"
        target_root.mkdir(parents=True, exist_ok=True)
        for p in candidates:
            try:
                if p.is_dir():
                    dest = target_root / p.name
                    if not dest.exists():
                        shutil.copytree(p, dest)
                else:
                    dest = target_root / p.name
                    if not dest.exists():
                        shutil.copy2(p, dest)
            except Exception as e:
                print(f"⚠️ Could not copy artifact {p} -> {target_root}: {e}")

    # ---------------------------
    # BaseService API (preserved)
    # ---------------------------
    def _can_handle_task_type(self, task_type: str) -> bool:
        return task_type in {"text_to_speech", "generate_audio", "tts_generation"}

    def _health_handler(self):
        return {
            "status": "ok",
            "service": self.service_name,
            "default_language": DEFAULT_LANGUAGE,
            "models_in_memory": list(self._tts_objects.keys()),
            "cache_dir": str(self._cache_dir),
        }

    async def _validate_task(self, task_message: TaskMessage):
        if task_message.data.payload_type != PayloadType.TEXT:
            raise HTTPException(status_code=400, detail="Unsupported payload_type (expected TEXT)")
        if "text" not in task_message.data.payload:
            raise HTTPException(status_code=400, detail="'text' is required in payload")

    def _detect_language_simple(self, text: str) -> str:
        clean_text = re.sub(r'[^a-zA-Zа-яА-ЯёЁ]', '', text)
        if not clean_text:
            return DEFAULT_LANGUAGE
        cyrillic_count = len(re.findall(r'[а-яА-ЯёЁ]', clean_text))
        latin_count = len(re.findall(r'[a-zA-Z]', clean_text))
        if cyrillic_count > latin_count:
            return "ru"
        elif latin_count > cyrillic_count:
            return "en"
        else:
            return DEFAULT_LANGUAGE

    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        text = task_message.data.payload.get("text")
        explicit_language = task_message.data.payload.get("language")
        if explicit_language:
            language = explicit_language.lower()
        else:
            language = self._detect_language_simple(text)
            print(f"🔍 Auto-detected language: {language} for text: {text[:50]}...")

        speaker = task_message.data.payload.get("speaker")
        speaker_audio_url = task_message.data.payload.get("speaker_audio_url")

        if speaker_audio_url:
            print("⚠️ speaker_audio_url provided but voice cloning is not supported; ignoring.")

        audio_url = await self._generate_audio(text=text, language=language, speaker=speaker)
        return Data(
            payload_type=PayloadType.AUDIO,
            payload={
                "task": "tts_generation",
                "audio_url": audio_url,
                "original_text": text,
                "model_used": "silero-tts (pip)",
                "language": language,
                "language_auto_detected": explicit_language is None,
            },
            execution_metadata={"task_type": "text_to_speech", "service": self.service_name},
        )

    # ---------------------------
    # Internal: create/get SileroTTS instance (thread-safe)
    # ---------------------------
    def _get_or_create_tts(self, language: str, speaker: Optional[str]) -> SileroTTS:
        """
        Возвращает существующий экземпляр или создаёт новый.
        Использует lock, чтобы предотвратить гонки при одновременном создании.
        """
        lang = language.split("_")[0]

        with self._tts_lock:
            if lang in self._tts_objects:
                return self._tts_objects[lang]

            chosen_speaker = speaker or (DEFAULT_RU_SPEAKER if lang.startswith("ru") else DEFAULT_EN_SPEAKER)

            try:
                if lang.startswith("ru"):
                    sample_rate = int(os.getenv("SILERO_SAMPLE_RATE_RU", "24000"))
                elif lang.startswith("en"):
                    sample_rate = int(os.getenv("SILERO_SAMPLE_RATE_EN", "24000"))
                else:
                    sample_rate = int(os.getenv("SILERO_SAMPLE_RATE", "24000"))
            except Exception:
                sample_rate = 24000

            # model_id best-effort
            try:
                model_id = SileroTTS.get_latest_model(lang)
            except Exception:
                model_id = None

            kwargs = {
                "model_id": model_id or ("v5_ru" if lang.startswith("ru") else "v3_en"),
                "language": lang,
                "speaker": chosen_speaker,
                "sample_rate": sample_rate,
                "device": "cuda" if USE_CUDA else "cpu",
            }

            # Попытка создать экземпляр; если параметр cache-dir поддерживается, мы пробуем разные имена
            cache_arg_names = ["cache_dir", "model_dir", "models_dir", "repo_dir", "download_root", "local_dir"]
            last_exc = None

            # 1) Попробовать без cache-аргументов
            try:
                tts = SileroTTS(**kwargs)
                self._tts_objects[lang] = tts
                # Попытка зарегистрировать артефакты
                self._register_model_artifacts(tts, lang)
                print(f"✅ SileroTTS created for lang={lang} model={kwargs['model_id']}")
                return tts
            except TypeError as e:
                last_exc = e
            except Exception as e:
                print(f"⚠️ Creating SileroTTS w/o cache-arg raised: {e}")

            # 2) Попробовать с разными именами аргумента для кеша
            for arg_name in cache_arg_names:
                try:
                    params = dict(kwargs)
                    params[arg_name] = str(self._cache_dir)
                    tts = SileroTTS(**params)
                    self._tts_objects[lang] = tts
                    self._register_model_artifacts(tts, lang)
                    print(f"✅ SileroTTS created for lang={lang} with {arg_name}={self._cache_dir}")
                    return tts
                except TypeError as e:
                    last_exc = e
                    continue
                except Exception as e:
                    print(f"⚠️ SileroTTS({arg_name}) failed: {e}")
                    last_exc = e
                    continue

            # окончательная попытка, если всё упало — пробросим исключение
            try:
                tts = SileroTTS(**kwargs)
                self._tts_objects[lang] = tts
                self._register_model_artifacts(tts, lang)
                return tts
            except Exception as e:
                raise RuntimeError(f"Failed to create SileroTTS instance for lang={lang}: {e}. Last error: {last_exc}")

    # ---------------------------
    # Internal: TTS call helpers (unchanged)
    # ---------------------------
    def _try_tts_call(self, tts_obj: SileroTTS, method_name: str, text: str, out_path: str) -> bool:
        method = getattr(tts_obj, method_name, None)
        if not callable(method):
            return False
        signatures = [
            lambda: method(text, out_path),
            lambda: method(text=text, file=out_path),
            lambda: method(text=text, out_path=out_path)
        ]
        for signature in signatures:
            try:
                signature()
                return True
            except TypeError:
                continue
            except Exception:
                return False
        return False

    def _find_working_tts_method(self, tts_obj: SileroTTS, text: str, out_path: str) -> bool:
        method_names = ["tts", "synthesize", "synth", "save", "speak"]
        return any(self._try_tts_call(tts_obj, method_name, text, out_path)
                   for method_name in method_names)

    def _execute_tts_sync(self, tts_obj: SileroTTS, text: str, out_path: str) -> None:
        if not self._find_working_tts_method(tts_obj, text, out_path):
            raise RuntimeError("Unable to call SileroTTS.tts with current package version. Check silero-tts API.")

    # ---------------------------
    # Generate audio
    # ---------------------------
    async def _generate_audio(self, text: str, language: str, speaker: Optional[str] = None) -> str:
        tts_obj = self._get_or_create_tts(language, speaker)
        out_filename = f"tts_{uuid.uuid4().hex}.wav"
        out_path = str(self.output_dir / out_filename)
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                self._execute_tts_sync,
                tts_obj, text, out_path
            )
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=f"silero-tts synthesis failed: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error during TTS synthesis: {e}")

        public_url = f"{self.service_base_url.rstrip('/')}/audio/{out_filename}"
        return public_url

    # ---------------------------
    # Optional: download external audio (kept)
    # ---------------------------
    async def _download_audio(self, audio_url: str) -> str:
        temp_dir = Path(os.getenv("TMPDIR", "/tmp"))
        temp_filename = f"tts_speaker_{uuid.uuid4().hex}.audio"
        temp_filepath = str(temp_dir / temp_filename)
        try:
            async with _client.stream("GET", audio_url, timeout=self.download_timeout) as r:
                r.raise_for_status()
                async with aiofiles.open(temp_filepath, "wb") as f:
                    async for chunk in r.aiter_bytes():
                        if not chunk:
                            continue
                        await f.write(chunk)
            return temp_filepath
        except httpx.RequestError as e:
            raise HTTPException(status_code=400, detail=f"Failed to download speaker audio: {e}")

    # ---------------------------
    # Close resources
    # ---------------------------
    async def close(self):
        try:
            await _client.aclose()
        except Exception:
            pass
        # intentionally keep cache on disk; only clear in-memory map
        self._tts_objects.clear()

    def shutdown(self):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.close())
        except Exception:
            pass


service = SileroService()

if __name__ == "__main__":
    try:
        service.run()
    except KeyboardInterrupt:
        print("Received interrupt signal, shutting down...")
    finally:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                close_task = loop.create_task(service.close())
                loop.run_until_complete(close_task)
            else:
                asyncio.run(service.close())
        except Exception as e:
            print(f"Error during shutdown: {e}")
