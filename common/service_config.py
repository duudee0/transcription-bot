# common/service_config.py
"""
ГЛОБАЛЬНЫЙ РЕЕСТР СЕРВИСОВ
Только базовые URL сервисов, без привязки к типам задач
"""

from typing import Dict, List, Optional

# Простой реестр: service_name -> base_url
SERVICE_REGISTRY: Dict[str, str] = {
    "local-llm": "http://local-llm:8000", # Локальная модель
    "llm-service": "http://llm-service:8000", # Просто для теста нет функционала
    "gigachat-service": "http://gigachat-service:8000", # гигачат
    "whisper": "http://whisper:8000", # Транскрибация
    "transcribe_audio": "http://whisper:8000", 
    "qwen": "http://qwen:8000", # еще одна LLM
    "voiceover" :"http://voiceover:8000" # Озвучка
}

MULTI_SERVICE_CHAINS = {
    "comprehensive_analysis": ["gigachat-service", "llm-service"], # Тест (считает количество слов которое написал gigachat)
    "voice_question": ["whisper", "local-llm"], # Переводит голосовое сообщение в запрос для llm
    "text_to_speech": ["local-llm", "voiceover"],  # Озвучивает текст от модели
    "voice_to_voice": ["whisper", "local-llm", "voiceover"] # Запрос голосом для ллм и ответ голосом пользователю
}

def get_service_url(service_name: str) -> Optional[str]:
    """Получить URL сервиса по имени"""
    return SERVICE_REGISTRY.get(service_name)

def get_all_services() -> Dict[str, str]:
    """Получить реестр всех сервисов"""
    return SERVICE_REGISTRY.copy()

def get_chain_for_task(task_type: str) -> Optional[List[str]]:
    """Получить список сервисов для цепочки задач."""
    return MULTI_SERVICE_CHAINS.get(task_type)