# xtts_silero_pkg_service.py
import os
import re
import uuid
import asyncio
import functools
from pathlib import Path
from typing import Optional

import httpx
import aiofiles
from fastapi import HTTPException
from fastapi.staticfiles import StaticFiles

# Требуемый официальный пакет — silero-tts
# Точная точка импорта может зависеть от версии пакета; этот импорт работает для распространённых релизов.
# Если у вас другой пакет-нейм, замените на корректный (но пакет silero-tts должен быть установлен).
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

# HTTP client for downloads (kept for parity)
_client = httpx.AsyncClient(timeout=XTTS_TIMEOUT)


class SileroService(BaseService):
    """
    Minimal Silero TTS service using official pip package 'silero-tts'.
    - only silero-tts used (no torch imports here)
    - default language: ru
    - optional payload field 'speaker' to select named speaker (if model supports)
    """

    def __init__(self):
        super().__init__("xtts-v2-service-silero-pkg", "1.0")

        self.output_dir = OUTPUT_DIR
        self.service_base_url = SERVICE_BASE_URL
        self.timeout = XTTS_TIMEOUT
        self.download_timeout = DOWNLOAD_TIMEOUT

        # mount static files if BaseService provides FastAPI app
        try:
            self.app.mount("/audio", StaticFiles(directory=self.output_dir), name="audio")
            print(f"✅ Mounted /audio -> {self.output_dir}")
        except AttributeError:
            print("⚠️ BaseService has no self.app; static /audio not auto-mounted")

        # cache SileroTTS instances per language
        self._tts_objects = {}  # lang -> SileroTTS instance

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
            "models_cached": list(self._tts_objects.keys()),
        }

    async def _validate_task(self, task_message: TaskMessage):
        if task_message.data.payload_type != PayloadType.TEXT:
            raise HTTPException(status_code=400, detail="Unsupported payload_type (expected TEXT)")
        if "text" not in task_message.data.payload:
            raise HTTPException(status_code=400, detail="'text' is required in payload")

    def _detect_language_simple(self, text: str) -> str:
            """
            Простое определение языка на основе статистики символов.
            Считаем кириллические и латинские символы.
            """
            # Очищаем текст от лишних символов, оставляем только буквы
            clean_text = re.sub(r'[^a-zA-Zа-яА-ЯёЁ]', '', text)
            
            if not clean_text:
                return DEFAULT_LANGUAGE  # fallback
                
            # Считаем кириллические и латинские символы
            cyrillic_count = len(re.findall(r'[а-яА-ЯёЁ]', clean_text))
            latin_count = len(re.findall(r'[a-zA-Z]', clean_text))
            
            # Определяем язык по преобладающим символам
            if cyrillic_count > latin_count:
                return "ru"
            elif latin_count > cyrillic_count:
                return "en"
            else:
                # Если равное количество - используем дефолтный
                return DEFAULT_LANGUAGE

    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        text = task_message.data.payload.get("text")
        
        # Определяем язык: если не указан явно - определяем автоматически
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
                "language_auto_detected": explicit_language is None,  # показываем, был ли язык определен автоматически
            },
            execution_metadata={"task_type": "text_to_speech", "service": self.service_name},
        )

    # ---------------------------
    # Internal: create/get SileroTTS instance
    # ---------------------------
    def _get_or_create_tts(self, language: str, speaker: Optional[str]):
        """
        Создаёт и кеширует SileroTTS объект для указанного языка.
        Явно передаём sample_rate (по умолчанию 24000) — это устраняет ошибку "Sample rate None is not supported".
        Можно переопределить через переменные окружения:
        SILERO_SAMPLE_RATE_RU, SILERO_SAMPLE_RATE_EN
        """
        lang = language.split("_")[0]
        if lang in self._tts_objects:
            return self._tts_objects[lang]

        chosen_speaker = speaker or (DEFAULT_RU_SPEAKER if lang.startswith("ru") else DEFAULT_EN_SPEAKER)

        # выбор sample_rate: сначала ENV, иначе безопасный дефолт 24000
        try:
            if lang.startswith("ru"):
                sample_rate = int(os.getenv("SILERO_SAMPLE_RATE_RU", "24000"))
            elif lang.startswith("en"):
                sample_rate = int(os.getenv("SILERO_SAMPLE_RATE_EN", "24000"))
            else:
                sample_rate = int(os.getenv("SILERO_SAMPLE_RATE", "24000"))
        except Exception:
            sample_rate = 24000

        # попытка получить актуальный model_id (если пакет это поддерживает)
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

        # Создаём экземпляр (в некоторых версиях сигнатура может чуть отличаться)
        try:
            tts = SileroTTS(**kwargs)
        except TypeError:
            # на случай другой сигнатуры: пробуем минимальный набор аргументов
            tts = SileroTTS(model_id=kwargs["model_id"], language=kwargs["language"], speaker=kwargs["speaker"],
                            sample_rate=kwargs["sample_rate"], device=kwargs["device"])

        self._tts_objects[lang] = tts
        print(f"✅ silero-tts instance created for lang={lang}, model_id={kwargs['model_id']}, speaker={chosen_speaker}, sr={sample_rate}")
        return tts
    
    # ---------------------------
    # Internal: TTS method helpers
    # ---------------------------
    def _try_tts_call(self, tts_obj: SileroTTS, method_name: str, text: str, out_path: str) -> bool:
        """Пытается вызвать конкретный метод TTS с разными сигнатурами"""
        method = getattr(tts_obj, method_name, None)
        if not callable(method):
            return False
        
        # Пробуем разные сигнатуры вызова
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
        """Находит рабочий метод TTS среди возможных вариантов"""
        method_names = ["tts", "synthesize", "synth", "save", "speak"]
        return any(self._try_tts_call(tts_obj, method_name, text, out_path) 
                for method_name in method_names)

    def _execute_tts_sync(self, tts_obj: SileroTTS, text: str, out_path: str) -> None:
        """Синхронное выполнение TTS в executor"""
        if not self._find_working_tts_method(tts_obj, text, out_path):
            raise RuntimeError(
                "Unable to call SileroTTS.tts with current package version. "
                "Check silero-tts API."
            )

    # ---------------------------
    # Internal: generate audio (uses SileroTTS.tts synchronously inside executor)
    # ---------------------------
    async def _generate_audio(self, text: str, language: str, speaker: Optional[str] = None) -> str:
        """Генерация аудио с использованием SileroTTS"""
        # Create or reuse silero object
        tts_obj = self._get_or_create_tts(language, speaker)

        # Generate output file path
        out_filename = f"tts_{uuid.uuid4().hex}.wav"
        out_path = str(self.output_dir / out_filename)

        # Execute TTS in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None, 
                self._execute_tts_sync, 
                tts_obj, text, out_path
            )
        except RuntimeError as e:
            raise HTTPException(
                status_code=500, 
                detail=f"silero-tts synthesis failed: {e}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=500, 
                detail=f"Unexpected error during TTS synthesis: {e}"
            )

        public_url = f"{self.service_base_url.rstrip('/')}/audio/{out_filename}"
        return public_url

    # ---------------------------
    # Optional helper: download audio (kept for parity)
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
        # Очищаем TTS объекты
        self._tts_objects.clear()

    def shutdown(self):
        """Синхронное закрытие для использования в обработчиках сигналов"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.close())
        except Exception:
            pass


# создаём сервис (как в вашей структуре)
service = SileroService()

if __name__ == "__main__":
    try:
        service.run()
    except KeyboardInterrupt:
        print("Received interrupt signal, shutting down...")
    finally:
        # Явное закрытие ресурсов
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Если цикл работает, планируем закрытие
                close_task = loop.create_task(service.close())
                loop.run_until_complete(close_task)
            else:
                asyncio.run(service.close())
        except Exception as e:
            print(f"Error during shutdown: {e}")