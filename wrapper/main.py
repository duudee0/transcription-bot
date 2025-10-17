import os
import uvicorn
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import uuid
import time
import asyncio
from aio_pika import connect_robust
import httpx
from datetime import datetime
import logging
from contextlib import asynccontextmanager

from common.models import TaskMessage, ResultMessage, ResultData, TaskData, MessageType
from common.publisher import Publisher

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
WRAPPER_HOST = os.getenv("WRAPPER_HOST", "0.0.0.0")
WRAPPER_PORT = int(os.getenv("WRAPPER_PORT", "8003"))

# Глобальный publisher
publisher = None

# Задачи, которые должны проходить через несколько сервисов (последовательно)
MULTI_SERVICE_CHAINS = {
    "comprehensive_analysis": ["llm-service", "gigachat-service"],
    "text_to_speech": ["llm-service", "voice-service"], 
    "content_creation": ["gigachat-service", "image-service"],
    "full_processing": ["llm-service", "gigachat-service", "image-service"]
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global publisher
    logger.info("🚀 Starting Task API Wrapper...")
    
    try:
        # Инициализируем publisher
        logger.info("🔄 Initializing RabbitMQ connection for wrapper...")
        connection = await connect_robust(RABBIT_URL)
        publisher = Publisher(connection)
        logger.info("✅ RabbitMQ connected for wrapper")
        
        yield  # Приложение запущено и работает
        
    finally:
        # Shutdown
        if publisher:
            await publisher.close()
            logger.info("✅ Publisher closed")
        logger.info("🛑 Task API Wrapper stopped")

app = FastAPI(
    title="Task API Wrapper", 
    version="1.0",
    lifespan=lifespan  # Используем современный lifespan
)

logger = logging.getLogger("wrapper")
logging.basicConfig(level=logging.INFO)

# In-memory хранилище (в продакшене заменить на Redis)
task_store = {}

# Конфигурация сервисов (аналогично воркеру)
SERVICE_CONFIGS = {
    "generate_response": {"service_name": "gigachat-service"},
    "analyze_text": {"service_name": "llm-service"},
    "process_image": {"service_name": "image-service"},
}

class TaskRequest(BaseModel):
    """Упрощенный запрос от клиента"""
    task_type: str
    input_data: Dict[str, Any]
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    callback_url: Optional[str] = None  # для вебхука КЛИЕНТУ
    timeout: Optional[int] = 30  # таймаут в секундах для синхронного ответа
    service_chain: Optional[List[str]] = None  # новая фича: цепочка сервисов

class TaskResponse(BaseModel):
    """Ответ клиенту"""
    task_id: str
    status: str  # "accepted", "processing", "completed", "error"
    message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    estimated_time: Optional[float] = None
    created_at: datetime

class StatusResponse(BaseModel):
    """Статус задачи"""
    task_id: str
    status: str
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime

async def get_publisher():
    """Возвращает глобальный publisher"""
    global publisher
    if publisher is None:
        raise RuntimeError("Publisher not initialized")
    return publisher

@app.post("/api/v1/tasks", response_model=TaskResponse)
async def create_task(
    task_request: TaskRequest,
    background_tasks: BackgroundTasks
):
    """Создание новой задачи"""
    task_id = str(uuid.uuid4()) 
    
    # Определяем целевые сервисы
    target_services = task_request.service_chain
    
    if not target_services:
        # ПРОВЕРЯЕМ МНОГОСЕРВИСНЫЕ ЦЕПОЧКИ
        if task_request.task_type in MULTI_SERVICE_CHAINS:
            target_services = MULTI_SERVICE_CHAINS[task_request.task_type]
            logger.info(f"🔗 Multi-service chain: {task_request.task_type} -> {target_services}")
        else:
            # Старая логика для одиночных задач
            service_config = SERVICE_CONFIGS.get(task_request.task_type)
            if service_config:
                target_services = [service_config["service_name"]]
    
    # TODO: ПРОДАКШЕН НУЖНО ФИКСИТЬ
    # Подготавливаем input_data с callback_url для wrapper'а
    wrapper_container_name = "wrapper"  # Имя контейнера в Docker сети
    wrapper_callback_url = f"http://{wrapper_container_name}:{WRAPPER_PORT}/internal/webhook/{task_id}"
    enhanced_input_data = {
        **task_request.input_data,
        "wrapper_callback_url": wrapper_callback_url,  # для сервисов
        "client_callback_url": task_request.callback_url  # для wrapper'а
    }
    
    # Создаем TaskMessage
    task_message = TaskMessage(
        message_id=task_id,
        source_service="api-wrapper",
        target_services=target_services,
        data=TaskData(
            task_type=task_request.task_type,
            input_data=enhanced_input_data,
            parameters=task_request.parameters or {}
        )
    )
    
    # Сохраняем в хранилище
    task_store[task_id] = {
        "status": "accepted",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "client_callback_url": task_request.callback_url,  # сохраняем отдельно
        "task_message": task_message.model_dump(),
        "result": None
    }
    
    # Получаем publisher и отправляем в очередь
    pub = await get_publisher()
    await pub.publish_task(task_message)
    
    logger.info(f"📨 Task created: {task_id}, type: {task_request.task_type}")
    
    # Если указан callback_url, используем асинхронный режим
    if task_request.callback_url:
        return TaskResponse(
            task_id=str(task_id),
            status="accepted",
            message="Task queued for processing, result will be sent via webhook",
            created_at=datetime.now()
        )
    
    # Иначе ждем результат синхронно (с таймаутом)
    background_tasks.add_task(
        wait_for_task_completion,
        str(task_id),
        task_request.timeout
    )
    
    return TaskResponse(
        task_id=str(task_id),
        status="processing", 
        message="Task is being processed",
        estimated_time=30.0,
        created_at=datetime.now()
    )

@app.get("/api/v1/tasks/{task_id}", response_model=StatusResponse)
async def get_task_status(task_id: str):
    """Получение статуса задачи"""
    if task_id not in task_store:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = task_store[task_id]
    return StatusResponse(
        task_id=task_id,
        status=task["status"],
        result=task.get("result"),
        error=task.get("error"),
        created_at=task["created_at"],
        updated_at=task["updated_at"]
    )

@app.post("/internal/webhook/{task_id}")
async def handle_webhook(task_id: str, request: Request):
    """Внутренний вебхук для получения ФИНАЛЬНЫХ результатов от сервисов"""
    try:
        # Получаем JSON из запроса
        payload = await request.json()
        logger.info(f"📬 Final webhook received for task: {task_id}")
        
        # Парсим в ResultMessage
        result_message = ResultMessage.model_validate(payload)
        
        if task_id not in task_store:
            logger.warning(f"Webhook for unknown task: {task_id}")
            return {"status": "ignored"}
        
        task = task_store[task_id]
        task["status"] = "completed" if result_message.data.success else "error"
        task["result"] = result_message.data.result
        task["error"] = result_message.data.error_message
        task["updated_at"] = datetime.now()
        
        logger.info(f"✅ Task {task_id} completed with status: {task['status']}")
        
        # Если у клиента есть callback_url, отправляем результат КЛИЕНТУ
        client_callback_url = task.get("client_callback_url")
        if client_callback_url:
            await send_webhook_to_client(client_callback_url, {
                "task_id": task_id,
                "status": task["status"],
                "result": task["result"],
                "error": task.get("error")
            })
            logger.info(f"📤 Callback sent to CLIENT for task: {task_id}")
        
        return {"status": "processed"}
        
    except Exception as e:
        logger.error(f"❌ Webhook processing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tasks")
async def list_tasks():
    """Список всех задач"""
    return {
        "total_tasks": len(task_store),
        "tasks": {
            task_id: {
                "status": task["status"],
                "created_at": task["created_at"],
                "updated_at": task["updated_at"]
            }
            for task_id, task in task_store.items()
        }
    }

async def wait_for_task_completion(task_id: str, timeout: int):
    """Фоновая задача для ожидания завершения"""
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        if task_id in task_store:
            task = task_store[task_id]
            if task["status"] in ["completed", "error"]:
                logger.info(f"⏹️ Task {task_id} completed within timeout")
                return
        
        await asyncio.sleep(0.5)
    
    # Таймаут
    if task_id in task_store:
        task_store[task_id]["status"] = "timeout"
        logger.warning(f"⏰ Task {task_id} timed out after {timeout} seconds")

async def send_webhook_to_client(url: str, data: dict):
    """Отправка вебхука клиенту"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json=data)
            if response.status_code == 200:
                logger.info(f"✅ Client webhook delivered: {url}")
            else:
                logger.warning(f"⚠️ Client webhook failed: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ Failed to send webhook to client: {e}")

@app.get("/health")
async def health():
    """Health check"""
    global publisher
    rabbit_status = "connected" if publisher else "disconnected"
    
    return {
        "status": "healthy", 
        "service": "api-wrapper",
        "timestamp": datetime.now(),
        "active_tasks": len(task_store),
        "rabbitmq": rabbit_status
    }

@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": "Task API Wrapper",
        "version": "1.0",
        "endpoints": {
            "create_task": "POST /api/v1/tasks",
            "get_status": "GET /api/v1/tasks/{task_id}",
            "list_tasks": "GET /tasks",
            "health": "GET /health"
        }
    }

if __name__ == "__main__":
    """Запуск сервера"""
    logger.info(f"🚀 Starting Task API Wrapper on {WRAPPER_HOST}:{WRAPPER_PORT}")
    uvicorn.run(
        app, 
        host=WRAPPER_HOST, 
        port=WRAPPER_PORT,
        log_level="info"
    )

# curl -X POST "http://localhost:8003/api/v1/tasks" \
#   -H "Content-Type: application/json" \
#   -d '{
#     "task_type": "analyze_text",
#     "input_data": {
#       "text": "Привет, как дела? Это тестовый текст для анализа."
#     },
#     "parameters": {
#       "language": "ru",
#       "detailed_analysis": true
#     },
#     "timeout": 30
#   }'