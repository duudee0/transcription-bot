from pydantic import BaseModel, Field
from uuid import uuid4, UUID
from datetime import datetime
from typing import Any, Dict, Optional, List
from enum import Enum

"""
МОДЕЛИ ДАННЫХ ДЛЯ МИКРОСЕРВИСНОЙ АРХИТЕКТУРЫ

Этот файл содержит общие модели данных (Pydantic-модели) для всех сервисов в системе.
Все контейнеры используют эти модели для обеспечения типобезопасности и согласованности данных.

Основные принципы:
- Все сообщения между сервисами должны наследоваться от BaseMessage
- Каждое сообщение имеет уникальный ID для сквозной трассировки
- Система использует JSON-сериализацию для обмена сообщениями
"""

class MessageType(str, Enum):
    """
    Типы сообщений в системе.
    
    Используется для маршрутизации и определения обработчиков сообщений.
    """
    TASK = "task"          # Задача для обработки воркером
    RESULT = "result"      # Результат выполнения задачи
    ERROR = "error"        # Сообщение об ошибке
    STATUS = "status"      # Статус сервиса (health check, мониторинг)
    VECTOR_OPERATION = "vector_operation"  # Новый тип


class BaseMessage(BaseModel):
    """
    БАЗОВАЯ МОДЕЛЬ СООБЩЕНИЯ
    
    Все сообщения в системе наследуются от этой модели.
    Содержит общие метаданные для трассировки и маршрутизации.
    """
    # Уникальный идентификатор сообщения (генерируется автоматически)
    message_id: UUID = Field(default_factory=uuid4)
    
    # Тип сообщения (определяет, как его обрабатывать)
    message_type: MessageType
    
    # Сервис-отправитель сообщения
    source_service: str
    
    # Сервис-получатель (опционально, для прямой маршрутизации)
    target_service: Optional[str] = None
    
    # Время создания сообщения (генерируется автоматически)
    timestamp: datetime = Field(default_factory=datetime.now)
    
    # Версия формата сообщения (для обратной совместимости)
    version: str = "1.0"


class TaskData(BaseModel):
    """
    ДАННЫЕ ЗАДАЧИ
    
    Содержит информацию о задаче, которую нужно выполнить.
    """
    # Тип задачи (определяет, какой воркер должен её обработать)
    task_type: str  # Примеры: "process_image", "analyze_text", "generate_report"
    
    # Входные данные для задачи (произвольный словарь)
    input_data: Dict[str, Any]  # Пример: {"image_url": "http://...", "text": "Hello"}
    
    # Параметры выполнения задачи (настройки обработки)
    parameters: Dict[str, Any] = Field(default_factory=dict)  # Пример: {"quality": "high", "timeout": 30}


class TaskMessage(BaseMessage):
    """
    СООБЩЕНИЕ-ЗАДАЧА
    
    Отправляется, когда нужно выполнить какую-либо работу.
    Передается между сервисами через RabbitMQ или HTTP.
    """
    # Всегда имеет тип TASK (переопределяем базовое поле)
    message_type: MessageType = MessageType.TASK
    
    # Данные задачи для выполнения
    data: TaskData


class ResultData(BaseModel):
    """
    ДАННЫЕ РЕЗУЛЬТАТА
    
    Содержит результат выполнения задачи или информацию об ошибке.
    """
    # Флаг успешности выполнения
    success: bool
    
    # Результат работы (если успешно)
    result: Optional[Dict[str, Any]] = None  # Пример: {"processed_url": "http://...", "word_count": 42}
    
    # Сообщение об ошибке (если success = False)
    error_message: Optional[str] = None
    
    # Метаданные выполнения (время работы, используемые ресурсы, версия модели и т.д.)
    execution_metadata: Dict[str, Any] = Field(default_factory=dict)  # Пример: {"processing_time_ms": 150, "memory_used_mb": 128}


class ResultMessage(BaseMessage):
    """
    СООБЩЕНИЕ-РЕЗУЛЬТАТ
    
    Отправляется в ответ на TaskMessage после выполнения работы.
    Содержит результат обработки или информацию об ошибке.
    """
    # Всегда имеет тип RESULT (переопределяем базовое поле)
    message_type: MessageType = MessageType.RESULT
    
    # ID оригинального сообщения-задачи, на которое это ответ
    original_message_id: UUID
    
    # Данные результата выполнения
    data: ResultData


class VectorOperationType(str, Enum):
    STORE_VECTORS = "store_vectors"
    SEARCH_VECTORS = "search_vectors" 
    CREATE_COLLECTION = "create_collection"
    GET_COLLECTION_INFO = "get_collection_info"


class VectorData(BaseModel):
    """Данные для векторных операций"""
    operation_type: VectorOperationType
    collection_name: str
    vectors: Optional[List[List[float]]] = None
    payloads: Optional[List[Dict[str, Any]]] = None
    vector_ids: Optional[List[str]] = None
    query_vector: Optional[List[float]] = None
    search_limit: int = 10
    score_threshold: Optional[float] = None
    vector_size: Optional[int] = None
    filter_conditions: Optional[Dict[str, Any]] = None


class VectorMessage(BaseMessage):
    """Сообщение для векторных операций"""
    message_type: MessageType = MessageType.VECTOR_OPERATION
    data: VectorData

"""
ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ:

1. СОЗДАНИЕ ЗАДАЧИ:
task = TaskMessage(
    source_service="web-api",
    target_service="image-processor",
    data=TaskData(
        task_type="process_image",
        input_data={"image_url": "https://example.com/photo.jpg"},
        parameters={"width": 800, "height": 600}
    )
)

2. СОЗДАНИЕ РЕЗУЛЬТАТА:
result = ResultMessage(
    source_service="image-processor",
    target_service="web-api",
    original_message_id=task.message_id,
    data=ResultData(
        success=True,
        result={"processed_url": "https://storage.com/processed.jpg"},
        execution_metadata={"processing_time_ms": 250}
    )
)

3. СЕРИАЛИЗАЦИЯ В JSON:
json_string = task.model_dump_json()

4. ДЕСЕРИАЛИЗАЦИЯ ИЗ JSON:
restored_task = TaskMessage.model_validate_json(json_string)

5. ПРИМЕР ТОГО ЧТО МОЖНО ОТПРАВИТЬ В РАББИТ И ВОРКЕР ЭТО НОРМАЛЬНО ОБРАБОТАЕТ
{
  "message_id": "123e4567-e89b-12d3-a456-426614174001",
  "message_type": "task", 
  "source_service": "test-client",
  "target_service": "llm-service",
  "timestamp": "2024-01-15T10:35:00.000Z",
  "version": "1.0",
  "data": {
    "task_type": "analyze_text",
    "input_data": {
      "text": "Тест автоопределения сервиса по типу задачи"
    },
    "parameters": {
      "simple_mode": true
    }
  }
}

ИЛИ

{
  "message_id": "123e4567-e89b-12d3-a456-426614174002",
  "message_type": "task",
  "source_service": "test-client", 
  "target_service": "image-service",
  "timestamp": "2024-01-15T10:40:00.000Z",
  "version": "1.0",
  "data": {
    "task_type": "process_image",
    "input_data": {
      "image_url": "https://example.com/test.jpg",
      "format": "jpeg"
    },
    "parameters": {
      "width": 800,
      "height": 600,
      "quality": 85
    }
  }
}

Gigachat:

{
  "message_id": "123e4567-e89b-12d3-a456-426614174002",
  "message_type": "task",
  "source_service": "test-client", 
  "timestamp": "2024-01-15T10:40:00.000Z",
  "version": "1.0",
    "data": {
      "task_type": "generate_response",
      "input_data": {
        "prompt": "привет."
      }
    }
}

"""