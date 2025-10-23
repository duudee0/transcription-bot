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
import secrets

from common.models import PayloadType, Data, TaskMessage, ResultMessage, MessageType
from common.models import TaskMessage, ResultMessage, Data ,MessageType
from common.publisher import Publisher
from common.service_config import get_chain_for_task, get_service_url

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
WRAPPER_HOST = os.getenv("WRAPPER_HOST", "0.0.0.0")
WRAPPER_PORT = int(os.getenv("WRAPPER_PORT", "8003"))
WRAPPER_HOST_DOCKER = "wrapper"  # Имя контейнера в Docker сети

# Глобальный publisher
publisher = None

logger = logging.getLogger("wrapper")
logging.basicConfig(level=logging.INFO)

# In-memory хранилище (в продакшене заменить на Redis)
task_store = {}


"""
! ДОБАВИТЬ ОБРАБОТКУ ОШИБОК ДЛЯ ОТПРАВКИ КЛИЕНТУ ОШИБКИ
! НЕ ВЫДАВАТЬ ПОЛНЫЕ ОШИБКИ ТОЛЬКО КРАТКОЕ ИХ ОПИСАНИЕ 
! ПЕРЕРАБОТАТЬ ПОД ЭТО ОЧЕРЕДЬ RESULT НА ДРУГУЮ
! ОТПРАВЛЯТЬ ВОРКЕРУ ЧТО ТАЙМАУТ ТОЖЕ ЛИБО ИЗМЕНИТЬ ЛОГИКУ

"""


def infer_payload_type(payload: Dict[str, Any]) -> PayloadType:
    """Простая эвристика для определения payload_type."""
    if not isinstance(payload, dict):
        return PayloadType.TEXT
    if "text" in payload and isinstance(payload["text"], str):
        return PayloadType.TEXT
    if "url" in payload and isinstance(payload["url"], str):
        return PayloadType.URL
    # common audio/video keys
    if any(k in payload for k in ("audio_url", "audio")):
        return PayloadType.AUDIO
    if any(k in payload for k in ("video_url", "video")):
        return PayloadType.VIDEO
    if any(k in payload for k in ("file_url", "file")):
        return PayloadType.FILE
    # fallback
    return PayloadType.TEXT

def make_wrapper_callback(task_id: str, secret: str) -> str:
    """
    Возвращает internal wrapper callback, который сервисы будут
    вызывать чтобы доставить final result.
    Мы используем имя контейнера (WRAPPER_HOST_DOCKER) для докер-сети.
    """
    return f"http://{WRAPPER_HOST_DOCKER}:{WRAPPER_PORT}/internal/webhook/{task_id}/{secret}"



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

def get_publisher():
    """Возвращает глобальный publisher"""
    global publisher
    if publisher is None:
        raise RuntimeError("Publisher not initialized")
    return publisher

@app.post("/api/v1/tasks", response_model=TaskResponse)
async def create_task(task_request: TaskRequest, background_tasks: BackgroundTasks):
    """Создание новой задачи — формируем TaskMessage и публикуем в очередь."""
    task_id = str(uuid.uuid4())
    # determine target chain
    target_services = task_request.service_chain
    if not target_services:
        # Проверяем, есть ли цепочка для данного типа задачи
        chain = get_chain_for_task(task_request.task_type)
        if chain:
            target_services = chain
            logger.info(f"🔗 Multi-service chain: {task_request.task_type} -> {target_services}")
        else:
            # Одиночный сервис: используем сопоставление task_type -> service_name
            service_name = task_request.task_type
            if not service_name:
                raise HTTPException(status_code=400, detail="Unknown task_type and no service_chain provided")
            target_services = [service_name]

    # security: generate per-task secret for internal webhook
    webhook_secret = secrets.token_urlsafe(16)

    # internal wrapper callback that services will call with final result
    wrapper_callback_url = make_wrapper_callback(task_id, webhook_secret)

    # build Data object for TaskMessage
    inferred_type = infer_payload_type(task_request.input_data or {})
    data_obj = Data(
        task_type=task_request.task_type,
        payload_type=inferred_type,
        payload=task_request.input_data or {},
        wrapper_callback_url=wrapper_callback_url,
        callback_url=None,
        original_message_id=task_id,
        parameters=task_request.parameters or {},
        execution_metadata={}
    )

    # Build TaskMessage (message_id may be string UUID; pydantic accepts)
    task_message = TaskMessage(
        message_id=task_id,
        source_service="api-wrapper",
        target_services=target_services,
        data=data_obj
    )

    # Save in memory store — **do not include client_callback_url in the outgoing message**
    task_store[task_id] = {
        "status": "accepted",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "client_callback_url": task_request.callback_url,   # ONLY stored locally
        "webhook_secret": webhook_secret,
        "task_message": task_message.model_dump(),  # for debugging / re-publish
        "result": None,
        "error": None
    }

    # Publish to queue
    pub = get_publisher()
    await pub.publish_task(task_message)

    logger.info(f"📨 Task created: {task_id}, type: {task_request.task_type}")

    if task_request.callback_url:
        # client requested async callback
        return TaskResponse(
            task_id=task_id,
            status="accepted",
            message="Task queued for processing, result will be sent via webhook",
            created_at=datetime.now()
        )

    # sync path: schedule background waiter and return processing response
    background_tasks.add_task(wait_for_task_completion, task_id, task_request.timeout)
    return TaskResponse(
        task_id=task_id,
        status="processing",
        message="Task queued and being processed",
        estimated_time=float(task_request.timeout),
        created_at=datetime.now()
    )


@app.get("/api/v1/tasks/{task_id}", response_model=StatusResponse)
async def get_task_status(task_id: str):
    """Получение статуса задачи"""
    if task_id not in task_store:
        raise HTTPException(status_code=404, detail="Task not found")
    
    #TODO РЕАЛИЗОВАТЬ КЭШ ПАМЯТЬ С ОПРОСОМ RABBITMQ (?WORKER)
    task = task_store[task_id]
    return StatusResponse(
        task_id=task_id,
        status=task["status"],
        result=task.get("result"),
        error=task.get("error"),
        created_at=task["created_at"],
        updated_at=task["updated_at"]
    )

@app.post("/internal/webhook/{task_id}/{secret}")
async def handle_webhook(task_id: str, secret: str, request: Request):
    """Internal webhook for receiving FINAL results from services.
       URL contains secret token that wrapper issued when task created."""
    try:
        if task_id not in task_store:
            logger.warning(f"⚠️ Webhook for unknown task: {task_id}")
            raise HTTPException(status_code=404, detail="Task not found")

        stored = task_store[task_id]
        expected = stored.get("webhook_secret")
        if not expected or secret != expected:
            logger.warning(f"⛔ Invalid webhook secret for task {task_id}")
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

        payload = await request.json()
        # validate as ResultMessage — tolerant to shapes
        try:
            result_message = ResultMessage.model_validate(payload)
        except Exception:
            # try nested 'data' dict presence — allow service to send just data
            # If payload contains 'data', wrap into a ResultMessage skeleton
            if isinstance(payload, dict) and "data" in payload:
                result_message = ResultMessage(
                    message_id=task_id,
                    message_type=MessageType.RESULT,
                    source_service=payload.get("source_service", "unknown"),
                    target_services=None,
                    original_message_id=task_id,
                    data=Data.model_validate(payload["data"]),
                    success=payload.get("success", True),
                    error_message=payload.get("error_message")
                )
            else:
                raise

        # Update store
        task = task_store[task_id]
        # prefer data.success (bool) inside ResultMessage.data if present else top-level success
        success_flag = getattr(result_message, "success", None)
        if success_flag is None and result_message.data:
            success_flag = getattr(result_message.data, "success", None)
        task["status"] = "completed" if success_flag else "error"
        # Normalize result & error fields
        task["result"] = result_message.data.payload if result_message.data else None
        task["error"] = result_message.error_message
        task["updated_at"] = datetime.now()

        logger.info(f"✅ Task {task_id} completed status={task['status']}")

        # send client callback if provided (server-to-client)
        client_callback_url = task.get("client_callback_url")
        if client_callback_url:
            # send concise payload to client
            send_payload = {
                "task_id": task_id,
                "status": task["status"],
                "result": task["result"],
                "error": task["error"]
            }
            # fire-and-forget (don't block wrapper)
            res = asyncio.create_task(send_webhook_to_client(client_callback_url, send_payload))
            logger.info(f"📤 Client callback enqueued for task {task_id}, {res}")

        # Optionally: delete webhook_secret to avoid replays (one-time)
        task_store[task_id]["webhook_secret"] = None

        return {"status": "processed"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ Webhook processing error for {task_id}: {e}")
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
    
    """
    TODO
    НАСТРОИТЬ ОБЩЕНИЕ С ВОРКЕРОМ СЛУЧАЕ ТАЙМАУТА
    """

    #! ТАЙМАУТ
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
        client_callback_url = task.get("client_callback_url")

        # Отправим отчет о том что время кончилось
        if client_callback_url:
            # send concise payload to client
            send_payload = {
                "task_id": task_id,
                "status": "delete task",
                "result": None,
                "error": f"timeout: {timeout}"
            }
            # fire-and-forget (don't block wrapper)
            res = asyncio.create_task(send_webhook_to_client(client_callback_url, send_payload))
            logger.info(f"📤 Client callback enqueued for task {task_id}, {res}")

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
        logger.error(f"❌ Failed to send webhook to client: {e} \n url: {url}")

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