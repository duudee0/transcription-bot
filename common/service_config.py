# common/service_config.py
"""
ГЛОБАЛЬНЫЙ РЕЕСТР СЕРВИСОВ
Только базовые URL сервисов, без привязки к типам задач
"""

from typing import Dict, Optional

# Простой реестр: service_name -> base_url
SERVICE_REGISTRY: Dict[str, str] = {
    "local-llm": "http://local-llm:8000", # Локальная модель
    "llm-service": "http://llm-service:8000", # Просто для теста
    "gigachat-service": "http://gigachat-service:8000", # гигачат
    # "image-service": "http://image-service:8000",
    # "voice-service": "http://voice-service:8000",
    # "worker": "http://worker:8080",
    # "wrapper": "http://wrapper:8000",
    # "text-analyzer": "http://text-analyzer:8000",
    # "animation-generator": "http://animation-generator:8000"
}

def get_service_url(service_name: str) -> Optional[str]:
    """Получить URL сервиса по имени"""
    return SERVICE_REGISTRY.get(service_name)

def get_all_services() -> Dict[str, str]:
    """Получить реестр всех сервисов"""
    return SERVICE_REGISTRY.copy()