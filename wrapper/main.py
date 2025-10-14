"""
ПОЛНОЦЕННЫЙ FASTAPI СЕРВЕР ДЛЯ УПРАВЛЕНИЯ ЗАДАЧАМИ

Предоставляет REST API для:
- Создания и отслеживания задач
- Получения результатов
- Мониторинга системы
- Интеграции с внешними клиентами
"""

import os
import asyncio
import logging
import json
import time
import uuid
from typing import Any, Dict, Optional, List, Callable
from uuid import UUID, uuid4
from datetime import datetime, timedelta
from enum import Enum
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from common.models import TaskMessage, ResultMessage, ResultData, MessageType, TaskData
from common.publisher import Publisher

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("task-api")

# Конфигурация
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
API_HOST = "0.0.0.0"
API_PORT = int(os.getenv("WORKER_PORT", "8777"))

# Модели Pydantic для API
class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    TIMEOUT = "timeout"

class CreateTaskRequest(BaseModel):
    task_type: str = Field(..., description="Тип задачи: analyze_text, process_image, etc")
    input_data: Dict[str, Any] = Field(..., description="Входные данные задачи")
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Параметры выполнения")
    target_service: Optional[str] = Field(None, description="Конкретный сервис для обработки")
    callback_url: Optional[str] = Field(None, description="URL для callback уведомления")

class TaskResponse(BaseModel):
    task_id: str
    status: str
    created_at: str
    task_type: str
    source_service: str
    target_service: Optional[str] = None

class TaskDetailResponse(TaskResponse):
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    execution_metadata: Optional[Dict[str, Any]] = None

class TasksListResponse(BaseModel):
    tasks: List[TaskResponse]
    total: int
    pending: int
    processing: int
    completed: int
    failed: int

class HealthResponse(BaseModel):
    status: str
    rabbitmq_connected: bool
    active_tasks: int
    uptime: float

class TaskInfo(BaseModel):
    """Информация о задаче для отслеживания"""
    task_id: UUID
    task_type: str
    status: TaskStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    source_service: str
    target_service: Optional[str] = None

class TaskManager:
    """
    Управление задачами - отправка, отслеживание, получение результатов
    """
    
    def __init__(
        self, 
        rabbit_url: str,
        queue_name: str = "tasks",
        result_queue: str = "results",
        app_name: str = "task-api",
        max_retries: int = 3,
        task_timeout: int = 300
    ):
        self.rabbit_url = rabbit_url
        self.queue_name = queue_name
        self.result_queue = result_queue
        self.app_name = app_name
        self.max_retries = max_retries
        self.task_timeout = task_timeout
        
        # Трекинг задач
        self._active_tasks: Dict[UUID, TaskInfo] = {}
        self._task_callbacks: Dict[UUID, List[Callable]] = {}
        
        # Клиенты RabbitMQ
        self._publisher: Optional[Publisher] = None
        self._connection: Optional[aio_pika.Connection] = None
        self._result_channel: Optional[aio_pika.Channel] = None
        
        # Фоновые задачи
        self._cleanup_task: Optional[asyncio.Task] = None
        self._is_running = False

    async def connect(self):
        """Подключение к RabbitMQ"""
        logger.info(f"🔗 Connecting to RabbitMQ: {self.rabbit_url}")
        
        self._connection = await aio_pika.connect_robust(self.rabbit_url)
        
        # Инициализация publisher
        self._publisher = Publisher(
            connection=self._connection,
            prefetch=10
        )
        await self._publisher.__aenter__()
        
        # Канал для получения результатов
        self._result_channel = await self._connection.channel()
        await self._result_channel.set_qos(prefetch_count=50)
        
        # Запуск фоновых задач
        self._is_running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_old_tasks())
        
        logger.info("✅ TaskManager connected successfully")

    async def disconnect(self):
        """Отключение от RabbitMQ"""
        self._is_running = False
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        if self._publisher:
            await self._publisher.close()
        
        if self._result_channel:
            await self._result_channel.close()
            
        if self._connection:
            await self._connection.close()
            
        logger.info("🔌 TaskManager disconnected")

    async def submit_task(
        self,
        task_type: str,
        input_data: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = None,
        target_service: Optional[str] = None,
        callback: Optional[Callable] = None
    ) -> UUID:
        """Отправка задачи на выполнение"""
        if not self._publisher:
            raise RuntimeError("TaskManager not connected")
            
        # Создаем сообщение-задачу
        task_message = TaskMessage(
            source_service=self.app_name,
            target_service=target_service,
            data=TaskData(
                task_type=task_type,
                input_data=input_data,
                parameters=parameters or {}
            )
        )
        
        # Сохраняем информацию о задаче
        task_info = TaskInfo(
            task_id=task_message.message_id,
            task_type=task_type,
            status=TaskStatus.PENDING,
            created_at=datetime.now(),
            source_service=self.app_name,
            target_service=target_service
        )
        
        self._active_tasks[task_message.message_id] = task_info
        
        # Сохраняем callback если указан
        if callback:
            if task_message.message_id not in self._task_callbacks:
                self._task_callbacks[task_message.message_id] = []
            self._task_callbacks[task_message.message_id].append(callback)
        
        # Отправляем задачу
        try:
            await self._publisher.publish_task(task_message, self.queue_name)
            logger.info(f"📤 Task submitted: {task_message.message_id} ({task_type})")
        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.error_message = f"Failed to submit task: {str(e)}"
            logger.error(f"❌ Failed to submit task {task_message.message_id}: {e}")
            raise
        
        return task_message.message_id

    async def get_task_status(self, task_id: UUID) -> Optional[TaskInfo]:
        """Получение статуса задачи"""
        return self._active_tasks.get(task_id)

    async def wait_for_result(self, task_id: UUID, timeout: Optional[int] = None) -> Optional[TaskInfo]:
        """Ожидание результата выполнения задачи"""
        if task_id not in self._active_tasks:
            raise ValueError(f"Task {task_id} not found")
            
        start_time = time.time()
        timeout = timeout or self.task_timeout
        
        while time.time() - start_time < timeout:
            task_info = self._active_tasks[task_id]
            
            if task_info.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMEOUT]:
                return task_info
                
            await asyncio.sleep(0.1)
            
        # Таймаут
        task_info = self._active_tasks[task_id]
        if task_info.status == TaskStatus.PENDING:
            task_info.status = TaskStatus.TIMEOUT
            task_info.error_message = f"Task timeout after {timeout} seconds"
            
        return task_info

    async def listen_for_results(self, result_handler: Optional[Callable] = None):
        """Прослушивание очереди результатов"""
        if not self._result_channel:
            raise RuntimeError("Result channel not initialized")
            
        logger.info(f"👂 Listening for results on '{self.result_queue}'")
        
        # Объявляем очередь результатов
        queue = await self._result_channel.declare_queue(self.result_queue, durable=True)
        
        async def default_result_handler(message: aio_pika.IncomingMessage):
            """Обработчик результатов по умолчанию"""
            async with message.process():
                try:
                    body = message.body.decode()
                    result_message = ResultMessage.model_validate_json(body)
                    await self._handle_task_result(result_message)
                except Exception as e:
                    logger.error(f"❌ Error processing result: {e}")

        handler = result_handler or default_result_handler
        await queue.consume(handler)

    async def _handle_task_result(self, result_message: ResultMessage):
        """Обработка результата выполнения задачи"""
        task_id = result_message.original_message_id
        
        if task_id not in self._active_tasks:
            logger.warning(f"⚠️ Received result for unknown task: {task_id}")
            return
            
        task_info = self._active_tasks[task_id]
        task_info.completed_at = datetime.now()
        
        if result_message.data.success:
            task_info.status = TaskStatus.COMPLETED
            task_info.result = result_message.data.result
            logger.info(f"✅ Task completed: {task_id}")
        else:
            task_info.status = TaskStatus.FAILED
            task_info.error_message = result_message.data.error_message
            logger.error(f"❌ Task failed: {task_id} - {result_message.data.error_message}")
        
        # Вызываем callbacks
        if task_id in self._task_callbacks:
            for callback in self._task_callbacks[task_id]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(task_info)
                    else:
                        callback(task_info)
                except Exception as e:
                    logger.error(f"❌ Callback error for task {task_id}: {e}")
            
            del self._task_callbacks[task_id]

    async def retry_task(self, task_id: UUID) -> bool:
        """Повторная отправка задачи"""
        if task_id not in self._active_tasks:
            return False
            
        task_info = self._active_tasks[task_id]
        
        if task_info.retry_count >= self.max_retries:
            logger.warning(f"🚫 Max retries exceeded for task {task_id}")
            return False
            
        task_info.retry_count += 1
        task_info.status = TaskStatus.RETRYING
        task_info.error_message = None
        
        # Создаем новую задачу с тем же ID
        task_message = TaskMessage(
            message_id=task_id,
            source_service=self.app_name,
            target_service=task_info.target_service,
            data=TaskData(
                task_type=task_info.task_type,
                input_data=task_info.result or {},
                parameters={"retry_count": task_info.retry_count}
            )
        )
        
        try:
            await self._publisher.publish_task(task_message, self.queue_name)
            task_info.status = TaskStatus.PENDING
            logger.info(f"🔄 Task retried: {task_id} (attempt {task_info.retry_count})")
            return True
        except Exception as e:
            task_info.status = TaskStatus.FAILED
            task_info.error_message = f"Retry failed: {str(e)}"
            logger.error(f"❌ Task retry failed: {task_id} - {e}")
            return False

    async def _cleanup_old_tasks(self):
        """Очистка старых завершенных задач"""
        while self._is_running:
            try:
                now = datetime.now()
                tasks_to_remove = []
                
                for task_id, task_info in self._active_tasks.items():
                    if (task_info.completed_at and 
                        now - task_info.completed_at > timedelta(hours=1)):
                        tasks_to_remove.append(task_id)
                
                for task_id in tasks_to_remove:
                    del self._active_tasks[task_id]
                    if task_id in self._task_callbacks:
                        del self._task_callbacks[task_id]
                    
                if tasks_to_remove:
                    logger.debug(f"🧹 Cleaned up {len(tasks_to_remove)} old tasks")
                    
            except Exception as e:
                logger.error(f"❌ Cleanup error: {e}")
                
            await asyncio.sleep(300)

    def get_active_tasks(self) -> List[TaskInfo]:
        """Получение списка активных задач"""
        return list(self._active_tasks.values())

    def get_task_count_by_status(self) -> Dict[TaskStatus, int]:
        """Статистика задач по статусам"""
        counts = {status: 0 for status in TaskStatus}
        for task in self._active_tasks.values():
            counts[task.status] += 1
        return counts

# Глобальный экземпляр TaskManager
task_manager: Optional[TaskManager] = None
startup_time: Optional[float] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup
    global task_manager, startup_time
    startup_time = time.time()
    
    # Инициализация TaskManager
    task_manager = TaskManager(
        rabbit_url=RABBIT_URL,
        app_name="task-api"
    )
    await task_manager.connect()
    
    # Запуск прослушивания результатов в фоне
    asyncio.create_task(task_manager.listen_for_results())
    
    yield
    
    # Shutdown
    if task_manager:
        await task_manager.disconnect()

# Создание FastAPI приложения
app = FastAPI(
    title="Task Management API",
    description="Микросервис для управления асинхронными задачами через RabbitMQ",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Вспомогательные функции
async def get_task_manager() -> TaskManager:
    if not task_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service unavailable"
        )
    return task_manager

# API Endpoints
@app.post("/tasks", response_model=TaskResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_task(request: CreateTaskRequest):
    """Создание новой задачи"""
    manager = await get_task_manager()
    
    try:
        # Создаем callback если указан URL
        callback = None
        if request.callback_url:
            # Здесь можно добавить логику отправки webhook
            pass
            
        # Отправляем задачу
        task_id = await manager.submit_task(
            task_type=request.task_type,
            input_data=request.input_data,
            parameters=request.parameters,
            target_service=request.target_service,
            callback=callback
        )
        
        # Получаем информацию о задаче
        task_info = await manager.get_task_status(task_id)
        if not task_info:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create task"
            )
        
        return TaskResponse(
            task_id=str(task_id),
            status=task_info.status.value,
            created_at=task_info.created_at.isoformat(),
            task_type=task_info.task_type,
            source_service=task_info.source_service,
            target_service=task_info.target_service
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@app.get("/tasks/{task_id}", response_model=TaskDetailResponse)
async def get_task(task_id: str):
    """Получение детальной информации о задаче"""
    manager = await get_task_manager()
    
    try:
        task_uuid = UUID(task_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid task ID format"
        )
    
    task_info = await manager.get_task_status(task_uuid)
    if not task_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    
    return TaskDetailResponse(
        task_id=str(task_info.task_id),
        status=task_info.status.value,
        created_at=task_info.created_at.isoformat(),
        started_at=task_info.started_at.isoformat() if task_info.started_at else None,
        completed_at=task_info.completed_at.isoformat() if task_info.completed_at else None,
        task_type=task_info.task_type,
        source_service=task_info.source_service,
        target_service=task_info.target_service,
        result=task_info.result,
        error_message=task_info.error_message,
        retry_count=task_info.retry_count
    )

@app.get("/tasks/{task_id}/wait", response_model=TaskDetailResponse)
async def wait_for_task(
    task_id: str,
    timeout: int = Query(30, ge=1, le=300, description="Таймаут ожидания в секундах")
):
    """Ожидание завершения задачи"""
    manager = await get_task_manager()
    
    try:
        task_uuid = UUID(task_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid task ID format"
        )
    
    task_info = await manager.wait_for_result(task_uuid, timeout)
    if not task_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found or timeout exceeded"
        )
    
    return TaskDetailResponse(
        task_id=str(task_info.task_id),
        status=task_info.status.value,
        created_at=task_info.created_at.isoformat(),
        started_at=task_info.started_at.isoformat() if task_info.started_at else None,
        completed_at=task_info.completed_at.isoformat() if task_info.completed_at else None,
        task_type=task_info.task_type,
        source_service=task_info.source_service,
        target_service=task_info.target_service,
        result=task_info.result,
        error_message=task_info.error_message,
        retry_count=task_info.retry_count
    )

@app.post("/tasks/{task_id}/retry", response_model=TaskResponse)
async def retry_task(task_id: str):
    """Повторная отправка задачи"""
    manager = await get_task_manager()
    
    try:
        task_uuid = UUID(task_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid task ID format"
        )
    
    success = await manager.retry_task(task_uuid)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot retry task - max retries exceeded or task not found"
        )
    
    task_info = await manager.get_task_status(task_uuid)
    if not task_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    
    return TaskResponse(
        task_id=str(task_info.task_id),
        status=task_info.status.value,
        created_at=task_info.created_at.isoformat(),
        task_type=task_info.task_type,
        source_service=task_info.source_service,
        target_service=task_info.target_service
    )

@app.get("/tasks", response_model=TasksListResponse)
async def list_tasks(
    status: Optional[TaskStatus] = Query(None, description="Фильтр по статусу"),
    task_type: Optional[str] = Query(None, description="Фильтр по типу задачи"),
    limit: int = Query(100, ge=1, le=1000, description="Лимит задач"),
    offset: int = Query(0, ge=0, description="Смещение")
):
    """Получение списка задач с фильтрацией"""
    manager = await get_task_manager()
    
    all_tasks = manager.get_active_tasks()
    
    # Применяем фильтры
    filtered_tasks = all_tasks
    if status:
        filtered_tasks = [t for t in filtered_tasks if t.status == status]
    if task_type:
        filtered_tasks = [t for t in filtered_tasks if t.task_type == task_type]
    
    # Пагинация
    paginated_tasks = filtered_tasks[offset:offset + limit]
    
    # Статистика
    status_counts = manager.get_task_count_by_status()
    
    return TasksListResponse(
        tasks=[
            TaskResponse(
                task_id=str(task.task_id),
                status=task.status.value,
                created_at=task.created_at.isoformat(),
                task_type=task.task_type,
                source_service=task.source_service,
                target_service=task.target_service
            )
            for task in paginated_tasks
        ],
        total=len(all_tasks),
        pending=status_counts[TaskStatus.PENDING],
        processing=status_counts[TaskStatus.PROCESSING],
        completed=status_counts[TaskStatus.COMPLETED],
        failed=status_counts.get(TaskStatus.FAILED, 0) + status_counts.get(TaskStatus.TIMEOUT, 0)
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Проверка здоровья сервиса"""
    manager = await get_task_manager()
    
    rabbitmq_connected = manager._connection and not manager._connection.is_closed
    active_tasks = len(manager.get_active_tasks())
    uptime = time.time() - startup_time if startup_time else 0
    
    status = "healthy" if rabbitmq_connected else "unhealthy"
    
    return HealthResponse(
        status=status,
        rabbitmq_connected=rabbitmq_connected,
        active_tasks=active_tasks,
        uptime=uptime
    )

@app.get("/")
async def root():
    """Корневой эндпоинт с информацией об API"""
    return {
        "message": "Task Management API",
        "version": "1.0.0",
        "endpoints": {
            "create_task": "POST /tasks",
            "get_task": "GET /tasks/{task_id}",
            "wait_for_task": "GET /tasks/{task_id}/wait",
            "list_tasks": "GET /tasks",
            "retry_task": "POST /tasks/{task_id}/retry",
            "health": "GET /health"
        },
        "documentation": "/docs"
    }

# Обработчики ошибок
@app.exception_handler(500)
async def internal_server_error_handler(request, exc):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "task_api:app",
        host=API_HOST,
        port=API_PORT,
        reload=True
    )

'''
# 1. Создание задачи
curl -X POST "http://localhost:8000/tasks" \\
  -H "Content-Type: application/json" \\
  -d '{
    "task_type": "analyze_text",
    "input_data": {
      "text": "Привет, мир!",
      "language": "ru"
    },
    "parameters": {
      "model": "gpt-3.5-turbo"
    }
  }'

# 2. Проверка статуса задачи
curl "http://localhost:8000/tasks/ВАШ_TASK_ID"

# 3. Ожидание завершения задачи
curl "http://localhost:8000/tasks/ВАШ_TASK_ID/wait?timeout=30"

# 4. Список всех задач
curl "http://localhost:8000/tasks"

# 5. Проверка здоровья
curl "http://localhost:8000/health"
'''