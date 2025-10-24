# common/service_config.py
"""
ГЛОБАЛЬНЫЙ РЕЕСТР СЕРВИСОВ
Только базовые URL сервисов, без привязки к типам задач
"""

from typing import Dict, List, Optional

# Простой реестр: service_name -> base_url
SERVICE_REGISTRY: Dict[str, str] = {
    "local-llm": "http://local-llm:8000", # Локальная модель
    "llm-service": "http://llm-service:8000", # Просто для теста
    "gigachat-service": "http://gigachat-service:8000", # гигачат
    "whisper": "http://whisper:8000",
    "transcribe_audio": "http://whisper:8000",
    "qwen": "http://qwen:8000"
}

MULTI_SERVICE_CHAINS = {
    "comprehensive_analysis": ["gigachat-service", "llm-service"],
    "voice_question": ["whisper", "local-llm"],
    "text_to_speech": ["llm-service", "voice-service"], 
    "content_creation": ["gigachat-service", "image-service"],
    "full_processing": ["llm-service", "gigachat-service", "image-service"]
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