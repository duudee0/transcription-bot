import os
import uuid
import tempfile
import functools
import asyncio
from pathlib import Path
from typing import Optional

import aiofiles
import httpx
from fastapi import HTTPException
from fastapi.staticfiles import StaticFiles

# Импорты ваших общих модулей — убедитесь, что пути верны в вашем проекте
from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data

# TTS
from TTS.api import TTS


class XTTSv2Service(BaseService):
    """
    XTTS-v2 Service (updated):
      - configurable model via TTS_MODEL_NAME
      - optional DEFAULT_SPEAKER_PATH for a fallback speaker WAV
      - safe blocking-call handling via run_in_executor + functools.partial
      - safe HTTP client close (close() method) and synchronous __del__
    """

    def __init__(self):
        super().__init__("xtts-v2-service", "1.0")

        # --- Config ---
        self.use_cuda = os.getenv("USE_CUDA", "False").lower() == "true"
        self.output_dir = Path(os.getenv("OUTPUT_DIR", "/app/audio_outputs"))
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.service_base_url = os.getenv("SERVICE_BASE_URL", "http://localhost:8000")
        self.timeout = int(os.getenv("XTTS_TIMEOUT", "300"))
        self.download_timeout = int(os.getenv("DOWNLOAD_TIMEOUT", "60"))

        # HTTP async client for downloads
        self.client = httpx.AsyncClient(timeout=self.timeout)

        # Mount static files if BaseService provides self.app (FastAPI)
        try:
            self.app.mount("/audio", StaticFiles(directory=self.output_dir), name="audio")
            print(f"✅ Mounted static audio directory at /audio (serving from {self.output_dir})")
        except AttributeError:
            print("⚠️ Warning: 'self.app' not found in BaseService. StaticFiles not mounted automatically.")

        # Model selection (env)
        self.model_name = os.getenv("TTS_MODEL_NAME", "tts_models/en/vctk/vits")
        # Default local speaker WAV (optional). If file not found -> None (use model default)
        self.default_speaker_path = os.getenv("DEFAULT_SPEAKER_PATH", "/app/default_speakers/default_speaker.wav")
        if not self.default_speaker_path or not os.path.exists(self.default_speaker_path):
            # If the expected file isn't present, we won't fail; we'll use model default voice.
            self.default_speaker_path = None

        # Load model
        print(f"🔄 Loading TTS model '{self.model_name}' (gpu={self.use_cuda}) ...")
        try:
            self.model = TTS(self.model_name, gpu=self.use_cuda)

            if hasattr(self.model, "speakers"):
                print("Available speakers:", getattr(self.model, "speakers"))
            else:
                print("Model does not expose 'speakers' list")

        except FileNotFoundError as e:
            # Typical cause: missing system phonemizer (espeak/espeak-ng)
            msg = (
                f"TTS model init failed: {e}\n"
                "Likely missing system phonemizer backend (espeak or espeak-ng).\n"
                "Install espeak-ng/espeak in the container (e.g. apt-get install espeak-ng) "
                "or choose a model that doesn't require external phonemizer."
            )
            print("❌", msg)
            # re-raise as RuntimeError so container fails early and logs show the cause
            raise RuntimeError(msg) from e
        except Exception as e:
            print("❌ Failed to initialize TTS model:", e)
            raise

        print(f"✅ TTS model '{self.model_name}' loaded successfully")

    # ---------------------------
    # Public helpers / framework hooks
    # ---------------------------
    def _can_handle_task_type(self, task_type: str) -> bool:
        supported = {"text_to_speech", "generate_audio", "tts_generation"}
        return task_type in supported

    def _health_handler(self):
        return {
            "status": "ok",
            "service": self.service_name,
            "model": self.model_name,
            "model_loaded": self.model is not None,
            "cuda_enabled": self.use_cuda,
        }

    # ---------------------------
    # Validation / processing
    # ---------------------------
    async def _validate_task(self, task_message: TaskMessage):
        """Validate incoming task. 'speaker_audio_url' is optional (fallback used if absent)."""
        if task_message.data.payload_type != PayloadType.TEXT:
            txt = f"Unsupported payload type: {task_message.data.payload_type}. Expected TEXT"
            print(txt)
            raise HTTPException(status_code=400, detail=txt)

        if "text" not in task_message.data.payload:
            txt = "'text' is required in payload"
            print(txt)
            raise HTTPException(status_code=400, detail=txt)

        # NOTE: speaker_audio_url is optional. If you need strict voice cloning, validate presence here.

    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        """Process the text -> speech task and return Data with audio URL."""
        text_to_speak = task_message.data.payload.get("text")
        speaker_source = task_message.data.payload.get("speaker_audio_url")  # may be None
        language = task_message.data.payload.get("language", "ru")

        # If not provided, try default speaker file (if available)
        if not speaker_source and self.default_speaker_path:
            speaker_source = self.default_speaker_path

        generated_audio_url = await self._generate_audio(
            text=text_to_speak,
            speaker_audio_url=speaker_source,
            language=language,
        )

        # Return payload. Use AUDIO_URL if your Data/PayloadType supports it; if not, switch to AUDIO.
        return Data(
            payload_type=PayloadType.AUDIO,
            payload={
                "task": "tts_generation",
                "audio_url": generated_audio_url,
                "original_text": text_to_speak,
                "model_used": self.model_name,
                "language": language,
            },
            execution_metadata={
                "task_type": "text_to_speech",
                "service": self.service_name,
                "model": self.model_name,
            },
        )

    # ---------------------------
    # Internal: generate / download
    # ---------------------------
    async def _generate_audio(self, text: str, speaker_audio_url: Optional[str], language: str) -> str:
        """
        Generate audio file and return public URL.

        speaker_audio_url can be:
          - HTTP/HTTPS URL -> will be downloaded temporarily
          - local filesystem path -> used as-is
          - None -> model default speaker (speaker_wav=None)
        """
        temp_speaker_file = None
        speaker_wav_for_model = None

        try:
            # Determine speaker_wav_for_model
            if speaker_audio_url:
                if isinstance(speaker_audio_url, str) and speaker_audio_url.startswith(("http://", "https://")):
                    temp_speaker_file = await self._download_audio(speaker_audio_url)
                    speaker_wav_for_model = temp_speaker_file
                elif os.path.exists(speaker_audio_url):
                    speaker_wav_for_model = speaker_audio_url
                else:
                    # Unknown / invalid path -> log and fallback to model default
                    print(f"⚠️ Speaker source not found or unsupported: {speaker_audio_url}. Using model default voice.")
                    speaker_wav_for_model = None
            else:
                speaker_wav_for_model = None

            # Prepare output path
            output_filename = f"tts_{uuid.uuid4().hex}.wav"
            output_filepath = self.output_dir / output_filename

            # Use functools.partial to pass named args into run_in_executor
            func = functools.partial(
                self.model.tts_to_file,
                text,
                speaker='p225', #speaker_wav_for_model,
                #language=language,
                file_path=str(output_filepath),
            )

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, func)

            # Make public URL (StaticFiles mount must be configured in the service/run env)
            public_url = f"{self.service_base_url.rstrip('/')}/audio/{output_filename}"
            return public_url

        except Exception as e:
            # Bubble up as HTTPException so caller sees reasonable status
            raise HTTPException(status_code=500, detail=f"TTS generation failed: {str(e)}")
        finally:
            # Cleanup downloaded temp file if present
            if temp_speaker_file and os.path.exists(temp_speaker_file):
                try:
                    os.unlink(temp_speaker_file)
                except Exception:
                    pass

    async def _download_audio(self, audio_url: str) -> str:
        """Download audio to temp file and return local path."""
        try:
            temp_dir = tempfile.gettempdir()
            temp_filename = f"tts_speaker_{uuid.uuid4().hex}.audio"
            temp_filepath = os.path.join(temp_dir, temp_filename)

            async with self.client.stream("GET", audio_url, timeout=self.download_timeout) as response:
                response.raise_for_status()
                async with aiofiles.open(temp_filepath, "wb") as f:
                    async for chunk in response.aiter_bytes():
                        if not chunk:
                            continue
                        await f.write(chunk)

            return temp_filepath

        except httpx.RequestError as e:
            txt = f"Failed to download speaker audio from URL: {str(e)}"
            print(txt)
            raise HTTPException(status_code=400, detail=txt)

    # ---------------------------
    # Close resources
    # ---------------------------
    async def close(self):
        """Асинхронно закрывает HTTP клиента (вызывайте при controlled shutdown)."""
        try:
            if getattr(self, "client", None):
                await self.client.aclose()
        except Exception:
            pass

    def __del__(self):
        """
        Синхронный финализатор — стараемся аккуратно закрыть httpx клиент.
        Не делаем async __del__ (оно не awaited Python'ом).
        """
        try:
            client = getattr(self, "client", None)
            if not client:
                return

            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                # планируем закрытие в текущем loop (не блокируем)
                try:
                    asyncio.create_task(client.aclose())
                except Exception:
                    pass
            else:
                # создаём временный loop и закрываем синхронно
                try:
                    new_loop = asyncio.new_event_loop()
                    new_loop.run_until_complete(client.aclose())
                    new_loop.close()
                except Exception:
                    pass
        except Exception:
            pass


# ---------------------------
# Script entrypoint
# ---------------------------
# Создаём сервис (при импорте модуля создастся экземпляр)
service = XTTSv2Service()


if __name__ == "__main__":
    # BaseService.run() — оставляем поведение как у вас было (предполагается синхронный запуск).
    # Если BaseService.run() блокирует и контролирует lifecycle, оно должно
    # вызывать service.close() при остановке; если нет — можно ловить KeyboardInterrupt.
    try:
        service.run()
    except KeyboardInterrupt:
        # Попытка корректно закрыть
        try:
            asyncio.run(service.close())
        except Exception:
            pass
        raise
