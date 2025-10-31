from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
from aiogram.fsm.state import State, StatesGroup


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass
class UserTask:
    """Модель задачи пользователя."""
    task_id: str
    user_id: int
    chat_id: int
    task_type: str
    status: TaskStatus
    service_chain: List[str]
    input_data: Dict[str, Any]
    parameters: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)


class TaskCreationState(StatesGroup):
    """Состояния создания задачи."""
    waiting_for_service = State()
    waiting_for_input = State()
    waiting_for_parameters = State()
    waiting_for_chain = State()


class ServiceSelectionState(StatesGroup):
    """Состояния выбора сервисов."""
    selecting_llm = State()
    selecting_audio = State()
    building_chain = State()