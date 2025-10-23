import aiofiles
import httpx
from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data
from fastapi import HTTPException
import os
import tempfile
import asyncio
from pathlib import Path
import whisper
import uuid

class WhisperService(BaseService):
    """Whisper Service для транскрибации аудио"""
    
    def __init__(self):
        super().__init__("whisper-service", "1.0")
        
        # Конфигурация модели Whisper
        self.model_size = os.getenv("WHISPER_MODEL", "base")  # tiny, base, small, medium, large
        self.timeout = int(os.getenv("WHISPER_TIMEOUT", "300"))
        self.download_timeout = int(os.getenv("DOWNLOAD_TIMEOUT", "60"))
        
        # Создаем асинхронный клиент httpx
        self.client = httpx.AsyncClient(timeout=self.timeout)
        
        # Загружаем модель Whisper (блокирующая операция, но делаем при инициализации)
        print(f"🔄 Loading Whisper model: {self.model_size}")
        self.model = whisper.load_model(self.model_size)
        print(f"✅ Whisper model {self.model_size} loaded successfully")

    def _can_handle_task_type(self, task_type: str) -> bool:
        """Определяет, может ли сервис обработать тип задачи"""
        supported_task_types = [
            "transcribe_audio",
            "speech_to_text",
            "audio_transcription"
        ]
        return task_type in supported_task_types

    def _health_handler(self):
        """Проверка здоровья сервиса"""
        try:
            return {
                "status": "ok",
                "service": self.service_name,
                "model": self.model_size,
                "model_loaded": self.model is not None
            }
        except Exception as e:
            return {
                "status": "error",
                "service": self.service_name,
                "error": str(e)
            }

    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задачи для транскрибации"""
        if task_message.data.payload_type != PayloadType.AUDIO:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported payload type: {task_message.data.payload_type}. Expected AUDIO_URL"
            )
        
        audio_url = task_message.data.payload.get("audio_url", "")
        if not audio_url:
            raise HTTPException(status_code=400, detail="audio_url is required")

    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        """Логика обработки задачи транскрибации"""
        audio_url = task_message.data.payload.get("audio_url", "")
        language = task_message.data.payload.get("language", "auto")
        
        # Скачиваем и транскрибируем аудио
        transcription = await self._transcribe_audio(audio_url, language)
        
        return Data(
            payload_type=PayloadType.TEXT,
            payload={
                "task": "audio_transcription",
                "original_audio_url": audio_url,
                "text": transcription,
                "model_used": f"whisper-{self.model_size}",
                "language": language
            },
            execution_metadata={
                "task_type": "transcribe_audio",
                "service": "whisper-service",
                "model": f"whisper-{self.model_size}"
            }
        )
    
    async def _transcribe_audio(self, audio_url: str, language: str = "auto") -> str:
        """Скачивает аудио и транскрибирует его"""
        temp_file = None
        try:
            # Скачиваем аудио файл
            temp_file = await self._download_audio(audio_url)
            
            # Транскрибируем (блокирующая операция - запускаем в thread pool)
            loop = asyncio.get_event_loop()
            
            if language == "auto":
                result = await loop.run_in_executor(
                    None, 
                    self.model.transcribe, 
                    temp_file
                )
            else:
                result = await loop.run_in_executor(
                    None, 
                    self.model.transcribe, 
                    temp_file, 
                    language
                )
            
            return result["text"].strip()
            
        except Exception as e:
            raise HTTPException(
                status_code=500, 
                detail=f"Transcription failed: {str(e)}"
            )
        finally:
            # Удаляем временный файл
            if temp_file and os.path.exists(temp_file):
                os.unlink(temp_file)
    
    async def _download_audio(self, audio_url: str) -> str:
        """Скачивает аудио файл во временный файл"""
        try:
            # Создаем временный файл
            temp_dir = tempfile.gettempdir()
            temp_filename = f"whisper_{uuid.uuid4().hex}.audio"
            temp_filepath = os.path.join(temp_dir, temp_filename)
            
            # Скачиваем файл асинхронно
            async with self.client.stream('GET', audio_url, timeout=self.download_timeout) as response:
                response.raise_for_status()
                
                # Асинхронная запись файла
                async with aiofiles.open(temp_filepath, 'wb') as f:
                    async for chunk in response.aiter_bytes():
                        await f.write(chunk)
            
            return temp_filepath
            
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=400, 
                detail=f"Failed to download audio from URL: {str(e)}"
            )

    async def __del__(self):
        """Закрываем клиент при завершении"""
        await self.client.aclose()


# Создаем и запускаем сервис
service = WhisperService()

if __name__ == "__main__":
    service.run()