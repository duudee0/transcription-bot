from enum import Enum
import os
from typing import Dict, List, Optional

class Config:
    """Конфигурация приложения."""
    
    # Telegram
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    
    # Wrapper API
    WRAPPER_URL = os.getenv("WRAPPER_URL", "http://wrapper:8003")
    WRAPPER_HOST_DOCKER = os.getenv("WRAPPER_HOST_DOCKER", "wrapper")
    
    # Server
    BOT_HOST = os.getenv("BOT_HOST", "0.0.0.0")
    BOT_PORT = int(os.getenv("BOT_PORT", 9000))
    BOT_CALLBACK_HOST_DOCKER = os.getenv("BOT_CALLBACK_HOST_DOCKER", "telegram-bot")
    
    # Task settings
    DEFAULT_TIMEOUT = int(os.getenv("DEFAULT_TIMEOUT", 600))
    
    # Настройки логирования
    LOG_LEVEL: str = "INFO"
    LOG_FILE: Optional[str] = "logs/app.log"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Available services for user selection
    LLM_SERVICES = {
        "local-llm": "🚀 Локальная модель (llama2:7b)",
        "gigachat-service": "🧠 GigaChat",
        "qwen": "🔮 Qwen (Open Router)",
        "yandex-gpt": "💤 Yandex GPT"
    }
    
    AUDIO_SERVICES = {
        "whisper": "🎤 Whisper (транскрибация)",
        "voiceover": "🔊 Озвучка текста",
    }
    
    # Predefined service chains for common tasks
    SERVICE_CHAINS = {
        "voice_chat": ["whisper", "local-llm", "voiceover"],
        "text_analysis": ["local-llm"],
        "comprehensive_analysis": ["gigachat-service", "llm-service"],
    }
    
    # Task types with metadata
    TASK_TYPES = {
        "text_analysis": {
            "name": "📝 Анализ текста",
            "description": "Обработка текстовых запросов",
            "input_type": "text",
            "needs_service_selection": True,
            "service_type": "llm"
        },
        "voice_transcription": {
            "name": "🎤 Транскрибация", 
            "description": "Преобразование аудио в текст",
            "input_type": "audio",
            "needs_service_selection": False,
            "default_service": "whisper"
        },
        "text_to_speech": {
            "name": "🔊 Текст в речь",
            "description": "Озвучивание текста",
            "input_type": "text", 
            "needs_service_selection": False,
            "default_service": "voiceover"
        },
        "voice_chat": {
            "name": "💬 Голосовой чат",
            "description": "Общение через голосовые сообщения",
            "input_type": "audio",
            "needs_service_selection": True,
            "service_type": "llm",
            "is_chain": True
        },
        "custom_chain": {
            "name": "⚙️ Кастомная цепочка",
            "description": "Создание своей цепочки сервисов",
            "input_type": "text",
            "needs_service_selection": True,
            "service_type": "custom"
        }
    }

    @property
    def callback_url(self):
        """URL для callback от wrapper к боту - КАК В ВАШЕМ КОДЕ"""
        return f"http://{self.BOT_CALLBACK_HOST_DOCKER}:{self.BOT_PORT}/client/webhook"
    
# Команды для клавиатуры
class TextCommands(str, Enum):
    ANALYZE_TEXT = "📝 Анализ текста"
    TRANSCRIBATION = "🎤 Транскрибация"
    TEXT_TO_SPEECH = "🔊 Текст в речь"
    VOICE_CHAT = "💬 Голосовой чат"
    CUSTOM_CHAIN = "⚙️ Кастомная цепочка"
    MY_TASK = "📊 Мои задачи"
    HELP = "ℹ️ Помощь"
    CANCEL = "❌ Отмена"

config = Config()