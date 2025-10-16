# wrapper/main.py
import os
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import uuid
import time
import asyncio
from aio_pika import connect_robust
import httpx
from datetime import datetime
import logging

from common.models import TaskMessage, ResultMessage, ResultData, TaskData, MessageType
from common.publisher import Publisher


RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")

app = FastAPI(title="Task API Wrapper", version="1.0")

logger = logging.getLogger("wrapper")

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
    callback_url: Optional[str] = None  # для вебхука
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

# Dependency для publisher
async def get_publisher():
    # Инициализация publisher (аналогично воркеру)
    connection = await connect_robust(RABBIT_URL)
    publisher = Publisher(connection)
    try:
        yield publisher
    finally:
        await publisher.close()

@app.post("/api/v1/tasks", response_model=TaskResponse)
async def create_task(
    task_request: TaskRequest,
    background_tasks: BackgroundTasks,
    publisher: Publisher = Depends(get_publisher)
):
    """Создание новой задачи"""
    task_id = str(uuid.uuid4())
    
    # Определяем целевые сервисы
    target_services = task_request.service_chain
    if not target_services:
        # Автоматическое определение по task_type
        service_config = SERVICE_CONFIGS.get(task_request.task_type)
        if service_config:
            target_services = [service_config["service_name"]]
    
    # Создаем TaskMessage
    task_message = TaskMessage(
        message_id=task_id,
        source_service="api-wrapper",
        target_services=target_services,
        data=TaskData(
            task_type=task_request.task_type,
            input_data=task_request.input_data,
            parameters=task_request.parameters or {}
        )
    )
    
    # Сохраняем в хранилище
    task_store[task_id] = {
        "status": "accepted",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "callback_url": task_request.callback_url,
        "task_message": task_message.model_dump(),
        "result": None
    }
    
    # Отправляем в очередь
    await publisher.publish_task(task_message)
    
    # Если указан callback_url, используем асинхронный режим
    if task_request.callback_url:
        return TaskResponse(
            task_id=task_id,
            status="accepted",
            message="Task queued for processing, result will be sent via webhook",
            created_at=datetime.now()
        )
    
    # Иначе ждем результат синхронно (с таймаутом)
    background_tasks.add_task(
        wait_for_task_completion,
        task_id,
        task_request.timeout
    )
    
    return TaskResponse(
        task_id=task_id,
        status="processing", 
        message="Task is being processed",
        estimated_time=30.0,  # можно вычислять на основе task_type
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
async def handle_webhook(task_id: str, result: ResultMessage):
    """Внутренний вебхук для получения результатов от воркеров"""
    if task_id not in task_store:
        logger.warning(f"Webhook for unknown task: {task_id}")
        return {"status": "ignored"}
    
    task = task_store[task_id]
    task["status"] = "completed" if result.data.success else "error"
    task["result"] = result.data.result
    task["error"] = result.data.error_message
    task["updated_at"] = datetime.now()
    
    # Если у клиента есть callback_url, отправляем результат
    callback_url = task.get("callback_url")
    if callback_url:
        await send_webhook_to_client(callback_url, {
            "task_id": task_id,
            "status": task["status"],
            "result": task["result"],
            "error": task.get("error")
        })
    
    return {"status": "processed"}

async def wait_for_task_completion(task_id: str, timeout: int):
    """Фоновая задача для ожидания завершения"""
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        if task_id in task_store:
            task = task_store[task_id]
            if task["status"] in ["completed", "error"]:
                return
        
        await asyncio.sleep(0.5)
    
    # Таймаут
    if task_id in task_store:
        task_store[task_id]["status"] = "timeout"

async def send_webhook_to_client(url: str, data: dict):
    """Отправка вебхука клиенту"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(url, json=data)
    except Exception as e:
        logger.error(f"Failed to send webhook to client: {e}")

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "api-wrapper"}