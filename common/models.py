# common/models.py
from pydantic import Field, model_validator, BaseModel
from pydantic.generics import GenericModel
from typing import TypeVar, Generic, Optional, List, Dict, Any, Type
from uuid import UUID, uuid4
from datetime import datetime, timezone
from enum import Enum
import json

"""
МОДЕЛИ ДАННЫХ ДЛЯ МИКРОСЕРВИСНОЙ АРХИТЕКТУРЫ

Этот файл содержит общие модели данных (Pydantic-модели) для всех сервисов в системе.
Все контейнеры используют эти модели для обеспечения типобезопасности и согласованности данных.

Основные принципы:
- Все сообщения между сервисами должны наследоваться от BaseMessage
- Каждое сообщение имеет уникальный ID для сквозной трассировки
- Система использует JSON-сериализацию для обмена сообщениями
"""


class PayloadType(str, Enum):
    """
    Типы входных/выходных данных для контейнеров.
    Только самые основные.
    """
    TEXT = "text"
    URL = "url"
    AUDIO = "audio"
    VIDEO = "video"
    FILE = "file"
    ERROR = "error" # Ошибка и ее текст

class MessageType(str, Enum):
    """
    Типы сообщений в системе.
    
    Используется для маршрутизации и определения обработчиков сообщений.
    """
    TASK = "task"          # Задача для обработки воркером
    RESULT = "result"      # Результат выполнения задачи
    ERROR = "error"        # Сообщение об ошибке
    STATUS = "status"      # Статус сервиса (health check, мониторинг)


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
    
    # Сервисы-получатели (опционально, для прямой маршрутизации)
    target_services: List[str] = Field(default_factory=list)

    # Время создания сообщения (генерируется автоматически)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Версия формата сообщения (для обратной совместимости)
    version: str = "2.0"


class Data(BaseModel):
    """
    ДАННЫЕ ЕДИНЫЕ И ДЛЯ ЗАДАЧИ И ДЛЯ ОТВЕТА НА НЕЕ
    
    """
    # Тип задачи (определяет, какой воркер должен её обработать)
    task_type: Optional[str] = None  # Примеры: "process_image", "analyze_text", "generate_report"
    
    # Что ожидать от payload (можно проверять в контейнере то ли пришло вообще)
    payload_type: PayloadType = PayloadType.TEXT #пока что чтобы ошибки отлавливать 

    # Входные данные для задачи (произвольный словарь)
    payload: Dict[str, Any] = Field(default_factory=dict)  # Пример: {"image_url": "http://...", "text": "Hello"}

    # Чтобы вернуть конечный результат на вебхук wrapper
    wrapper_callback_url: Optional[str] = None

    # Вебхук на воркер чтобы сообщить ему что задача выполнена и он мог ее убрать из rabbit
    callback_url: Optional[str] = None

    # ID оригинального сообщения-задачи (возможно пригодится для защиты вебхуков)
    original_message_id: Optional[UUID] = None
    
    # Параметры выполнения задачи (настройки обработки) необходимо для следующих сервисов и для логирования
    parameters: Dict[str, Any] = Field(default_factory=dict)  # Пример: {"quality": "high", "timeout": 30}

    # Метаданные выполнения (время работы, используемые ресурсы, версия модели и т.д.) для ответа пользователю
    execution_metadata: Dict[str, Any] = Field(default_factory=dict)  # Пример: {"processing_time_ms": 150, "memory_used_mb": 128}


class TaskMessage(BaseMessage):
    """
    СООБЩЕНИЕ-ЗАДАЧА
    
    Отправляется, когда нужно выполнить какую-либо работу.
    Передается между сервисами через RabbitMQ или HTTP.
    """
    # Всегда имеет тип TASK (переопределяем базовое поле)
    message_type: MessageType = MessageType.TASK
    
    # Данные задачи для выполнения
    data: Data


class ResultMessage(BaseMessage):
    """
    СООБЩЕНИЕ-РЕЗУЛЬТАТ
    
    Отправляется в ответ на TaskMessage после выполнения работы.
    Содержит результат обработки или информацию об ошибке.
    always message_type == MessageType.RESULT
    """
    # Всегда имеет тип RESULT (переопределяем базовое поле)
    message_type: MessageType = MessageType.RESULT
    
    # Данные результата выполнения
    data: Optional[Data] = None

    # Флаг успешности выполнения
    success: bool = True # Если все норм то пускай так будет дефолт

    # Сообщение об ошибке (если success = False)
    error_message: Optional[str] = None


"""
ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ:

1. СОЗДАНИЕ ЗАДАЧИ:
task = TaskMessage(
    source_service= "web-api",
    target_services=["image-processor"],
    data=TaskData(
        task_type="process_image",
        input_data={"image_url": "https://example.com/photo.jpg"},
        parameters={"width": 800, "height": 600}
    )
)

2. СОЗДАНИЕ РЕЗУЛЬТАТА:
result = ResultMessage(
    source_service="image-processor",
    target_services=["web-api"],
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
  "target_services": ["llm-service"],
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
  "target_services": ["image-service"],
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
  "target_services": ["gigachat-service"],
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