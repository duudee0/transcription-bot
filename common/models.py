from pydantic import BaseModel, Field
from uuid import uuid4, UUID
from datetime import datetime
from typing import Any, Dict, Optional
from enum import Enum

# Модели запросов для обмена данными между контейнерами

class MessageType(str, Enum):
    TASK = "task"
    RESULT = "result"
    ERROR = "error"
    STATUS = "status"

class BaseMessage(BaseModel):
    message_id: UUID = Field(default_factory=uuid4)
    message_type: MessageType
    source_service: str
    target_service: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)
    version: str = "1.0"

class TaskData(BaseModel):
    task_type: str
    input_data: Dict[str, Any]
    parameters: Dict[str, Any] = Field(default_factory=dict)

class TaskMessage(BaseMessage):
    message_type: MessageType = MessageType.TASK
    data: TaskData

class ResultData(BaseModel):
    success: bool
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    execution_metadata: Dict[str, Any] = Field(default_factory=dict)

class ResultMessage(BaseMessage):
    message_type: MessageType = MessageType.RESULT
    original_message_id: UUID
    data: ResultData