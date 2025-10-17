# common/service_config.py
"""
ЦЕНТРАЛИЗОВАННАЯ КОНФИГУРАЦИЯ СЕРВИСОВ
Изменяйте здесь при добавлении новых сервисов
"""

# Mapping: task_type -> service_name
TASK_TO_SERVICE = {
    "analyze_text": "llm-service",
    "promt_response": "llm-service",
    "generate_response": "gigachat-service", 
    "process_image": "image-service",
}

# Базовые URL сервисов
SERVICE_URLS = {
    "llm-service": "http://llm-service:8000",
    "gigachat-service": "http://gigachat-service:8000", 
    "image-service": "http://image-service:8000", # Тестировать нерабочий сервис
}

# Полные конфиги (для обратной совместимости)
SERVICE_CONFIGS = {
    task_type: {
        "service_name": service_name,
        "base_url": SERVICE_URLS[service_name],
        "endpoint": "/api/v1/process"
    }
    for task_type, service_name in TASK_TO_SERVICE.items()
}

def get_service_config(task_type: str):
    """Получить конфиг для типа задачи"""
    return SERVICE_CONFIGS.get(task_type)

def get_service_url(service_name: str):
    """Получить URL сервиса по имени"""
    return SERVICE_URLS.get(service_name)