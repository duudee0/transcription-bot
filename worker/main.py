#!/usr/bin/env python3
"""
Типизированный async воркер с Pydantic-моделями + умный мониторинг задач
"""
import logging
import asyncio
import os
import json
import sys
import time
from typing import Optional, Dict, Any, Set
from dataclasses import dataclass
from aio_pika import connect_robust, Message, IncomingMessage
from uuid import uuid4
import httpx

# Импортируем наши модели
from common.models import TaskMessage, ResultMessage, ResultData, MessageType

# Конфиг через env
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")
RESULT_QUEUE = os.getenv("RESULT_QUEUE", "results")
SEND_METHOD = os.getenv("SEND_METHOD", "http")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "5.0"))
WORKER_NAME = os.getenv("WORKER_NAME", "generic-worker")
WORKER_HOST = os.getenv("WORKER_HOST", "worker")
WORKER_PORT = int(os.getenv("WORKER_PORT", "8080"))

# Конфигурация сервисов
SERVICE_CONFIGS = {
    "generate_response": {
        "base_url": "http://gigachat-service:8000",
        "service_name": "gigachat-service"
    },
    "analyze_text": {
        "base_url": "http://llm-service:8000",
        "service_name": "llm-service"
    },
    "process_image": {
        "base_url": "http://image-service:8000", 
        "service_name": "image-service"
    },
}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler('/var/log/worker.log')
    ]
)
logger = logging.getLogger("typed-worker")

print(f"🚀 Typed worker '{WORKER_NAME}' starting...", file=sys.stderr)

# =============================================================================
# НОВЫЙ КОД: Менеджер асинхронных задач
# =============================================================================

@dataclass
class AsyncTaskState:
    """Состояние асинхронной задачи"""
    task: TaskMessage
    service_config: Dict[str, Any]
    start_time: float
    last_check: float
    status: str  # "waiting", "processing", "completed", "failed"
    attempts: int = 0
    callback_received: bool = False

class AsyncTaskManager:
    """Управляет асинхронными задачами с вебхуками и поллингом"""
    
    def __init__(self):
        self.active_tasks: Dict[str, AsyncTaskState] = {}
        self.max_wait_time = 3600  # 1 час максимум
        self.check_interval = 5   # проверка каждые 30 секунд
        self.max_attempts = 3
        
    async def start_monitoring(self):
        """Запускает фоновый мониторинг задач"""
        logger.info("🔍 Starting async task monitor...")
        asyncio.create_task(self._monitor_loop())
    
    async def _monitor_loop(self):
        """Основной цикл мониторинга"""
        while True:
            try:
                await self._check_active_tasks()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"❌ Monitor loop error: {e}")
                await asyncio.sleep(10)  # пауза при ошибке
    
    async def _check_active_tasks(self):
        """Проверяет все активные задачи"""
        current_time = time.time()
        completed_tasks = []
        
        for task_id, task_state in self.active_tasks.items():
            try:
                # Пропускаем недавно созданные задачи
                if current_time - task_state.start_time < 10:
                    continue
                    
                # Проверяем таймаут
                if current_time - task_state.start_time > self.max_wait_time:
                    logger.warning(f"⏰ Task {task_id} timeout")
                    await self._handle_task_timeout(task_id)
                    completed_tasks.append(task_id)
                    continue
                
                # Проверяем статус сервиса
                if not await self._is_service_alive(task_state.service_config):
                    task_state.attempts += 1
                    logger.warning(f"🚨 Service {task_state.service_config['service_name']} down for task {task_id}")
                    
                    if task_state.attempts >= self.max_attempts:
                        await self._handle_service_down(task_id)
                        completed_tasks.append(task_id)
                    continue
                
                # Если вебхук не пришел, проверяем статус задачи
                if not task_state.callback_received:
                    await self._check_task_status(task_id, task_state)
                else: # Вебхук пришел удаляем
                    completed_tasks.append(task_id)
                    
            except Exception as e:
                logger.error(f"❌ Error monitoring task {task_id}: {e}")
                task_state.attempts += 1
                
                if task_state.attempts >= self.max_attempts:
                    completed_tasks.append(task_id)
        
        # Удаляем завершенные задачи
        for task_id in completed_tasks:
            if task_id in self.active_tasks:
                del self.active_tasks[task_id]
    
    async def _is_service_alive(self, service_config: Dict) -> bool:
        """Проверяет жив ли сервис"""
        try:
            health_url = f"{service_config['base_url']}/health"
            logger.info(f"❔ Check health: url - {health_url}")
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(health_url)
                return response.status_code == 200
        except:
            return False
    
    async def _check_task_status(self, task_id: str, task_state: AsyncTaskState):
        """Проверяет статус задачи в сервисе"""
        try:
            # Используем endpoint статуса сервиса
            status_url = f"{task_state.service_config['base_url']}/status"
            logger.info(f"❓ Check status task: url - {status_url}")
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(status_url)
                
                if response.status_code == 200:
                    status_data = response.json()
                    # Если сервис свободен, наша задача должна быть завершена
                    if not status_data.get("is_busy", False):
                        logger.info(f"✅ Service free, checking task {task_id} completion")
                        await self._verify_task_completion(task_id, task_state)
                    
        except Exception as e:
            logger.warning(f"⚠️ Status check failed for {task_id}: {e}")
    
    async def _verify_task_completion(self, task_id: str, task_state: AsyncTaskState):
        """Проверяет завершение задачи через историю"""
        try:
            history_url = f"{task_state.service_config['base_url']}/requests/{task_id}"
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(history_url)
                
                if response.status_code == 200:
                    history_data = response.json()
                    if history_data.get("status") == "completed":
                        logger.info(f"🎉 Task {task_id} completed (via history)")
                        await self._handle_task_completed(task_id, history_data.get("result", {}))
                    elif history_data.get("status") == "failed":
                        logger.error(f"❌ Task {task_id} failed (via history)")
                        await self._handle_task_failed(task_id, history_data.get("error_message", "Unknown error"))
        except Exception as e:
            logger.warning(f"⚠️ History check failed for {task_id}: {e}")
    
    async def _handle_task_completed(self, task_id: str, result_data: Dict):
        """Обработка завершенной задачи"""
        task_state = self.active_tasks.get(task_id)
        if task_state:
            result_message = ResultMessage(
                source_service=WORKER_NAME,
                target_service=task_state.task.source_service,
                original_message_id=task_state.task.message_id,
                data=ResultData(
                    success=True,
                    result=result_data,
                    execution_metadata={
                        "worker": WORKER_NAME,
                        "service": task_state.service_config["service_name"],
                        "processed_via": "async_polling"
                    }
                )
            )
            # Удаляем из активных задач
            self.active_tasks.pop(task_id)

            await send_to_result_queue(result_message)
    
    async def _handle_task_failed(self, task_id: str, error: str):
        """Обработка неудачной задачи"""
        task_state = self.active_tasks.get(task_id)
        if task_state:
            result_message = ResultMessage(
                source_service=WORKER_NAME,
                target_service=task_state.task.source_service,
                original_message_id=task_state.task.message_id,
                data=ResultData(
                    success=False,
                    error_message=error,
                    execution_metadata={
                        "worker": WORKER_NAME,
                        "service": task_state.service_config["service_name"],
                        "error": True
                    }
                )
            )
            await send_to_result_queue(result_message)
    
    async def _handle_task_timeout(self, task_id: str):
        """Обработка таймаута задачи"""
        task_state = self.active_tasks.get(task_id)
        if task_state:
            logger.error(f"⏰ Task {task_id} timeout after {self.max_wait_time}s")
            result_message = ResultMessage(
                source_service=WORKER_NAME,
                target_service=task_state.task.source_service,
                original_message_id=task_state.task.message_id,
                data=ResultData(
                    success=False,
                    error_message=f"Task timeout after {self.max_wait_time}s",
                    execution_metadata={
                        "worker": WORKER_NAME,
                        "service": task_state.service_config["service_name"],
                        "timeout": True
                    }
                )
            )
            await send_to_result_queue(result_message)
    
    async def _handle_service_down(self, task_id: str):
        """Обработка недоступности сервиса"""
        task_state = self.active_tasks.get(task_id)
        if task_state:
            logger.error(f"🚨 Service down for task {task_id}")
            result_message = ResultMessage(
                source_service=WORKER_NAME,
                target_service=task_state.task.source_service,
                original_message_id=task_state.task.message_id,
                data=ResultData(
                    success=False,
                    error_message=f"Service {task_state.service_config['service_name']} unavailable",
                    execution_metadata={
                        "worker": WORKER_NAME,
                        "service": task_state.service_config["service_name"],
                        "service_down": True
                    }
                )
            )
            await send_to_result_queue(result_message)
    
    def register_async_task(self, task: TaskMessage, service_config: Dict):
        """Регистрирует задачу для асинхронного отслеживания"""
        task_id = str(task.message_id)
        self.active_tasks[task_id] = AsyncTaskState(
            task=task,
            service_config=service_config,
            start_time=time.time(),
            last_check=time.time(),
            status="waiting"
        )
        logger.info(f"📝 Registered async task: {task_id}")
    
    async def handle_webhook(self, message_id: str, payload: dict) -> bool:
        """Обрабатывает вебхук уведомление"""
        task_state = self.active_tasks.get(message_id)
        
        if not task_state:
            logger.warning(f"🤔 Webhook for unknown task: {message_id}")
            return False
        
        task_state.callback_received = True
        task_state.last_check = time.time()
        
        if payload.get("success") != True:
            logger.info(f"✅ Webhook: task {message_id} completed")
            await self._handle_task_completed(message_id, payload.get("result", {}))
            return True
        else:
            logger.error(f"❌ Webhook: task {message_id} failed")
            await self._handle_task_failed(message_id, payload.get("error_message", "Unknown error"))
            return True
        
        return False

# Глобальный менеджер задач
task_manager = AsyncTaskManager()


async def check_service_ready(service_config: dict) -> bool:
    """Проверяет что сервис готов к работе (health + status) с детальным логированием"""

    base_url = service_config["base_url"]
    service_name = service_config["service_name"]
    
    logger.info(f"🏥 Health checking {service_name} at {base_url}")
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Проверяем здоровье
            health_url = f"{base_url}/health"
            logger.debug(f"   Checking health: {health_url}")
            health_response = await client.get(health_url)
            
            if health_response.status_code != 200:
                logger.warning(f"   ❌ Health check failed: {health_response.status_code}")
                return False
            
            # Проверяем занятость
            status_url = f"{base_url}/status"
            logger.debug(f"   Checking status: {status_url}")
            status_response = await client.get(status_url)
            
            if status_response.status_code == 200:
                status_data = status_response.json()
                is_busy = status_data.get("is_busy", False)
                if is_busy:
                    logger.info(f"   ⏸️ Service {service_name} is busy")
                else:
                    logger.info(f"   ✅ Service {service_name} is ready")
                return not is_busy
            
            logger.warning(f"   ❌ Status check failed: {status_response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"   💥 Service {service_name} unreachable: {e}")
        return False

def get_service_config(task_type: str, target_service: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Определяет какой сервис должен обрабатывать задачу с детальным логированием"""

    logger.info(f"🔍 Looking up service config for task_type='{task_type}', target_service='{target_service}'")
    
    # Если явно указан целевой сервис
    if target_service:
        logger.info(f"🎯 Explicit target_service specified: {target_service}")
        for task_key, config in SERVICE_CONFIGS.items():
            if config["service_name"] == target_service:
                logger.info(f"✅ Found service config: {config['service_name']} for task type: {task_key}")
                return config
        
        logger.warning(f"❌ Target service '{target_service}' not found in SERVICE_CONFIGS")
        return None
    
    # Определяем по типу задачи
    if task_type in SERVICE_CONFIGS:
        config = SERVICE_CONFIGS[task_type]
        logger.info(f"✅ Found direct mapping: task_type='{task_type}' -> service='{config['service_name']}'")
        return config
    
    # Ищем подходящий сервис по паттерну (например, "process_*" -> "image-service")
    for task_pattern, config in SERVICE_CONFIGS.items():
        if task_type.startswith(task_pattern.split('_')[0] + '_'):  # простой паттерн
            logger.info(f"🔀 Pattern match: task_type='{task_type}' matches pattern '{task_pattern}' -> service='{config['service_name']}'")
            return config
    
    logger.error(f"🚨 No service config found for task_type='{task_type}' and no fallback available")
    logger.info(f"📋 Available task types: {list(SERVICE_CONFIGS.keys())}")
    return None

async def send_via_http(url: str, payload: dict) -> dict:
    """Универсальная функция отправки HTTP запроса с улучшенным логированием"""

    logger.info(f"🌐 HTTP Request: POST {url}")
    
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        try:
            # Сериализуем данные в JSON строку
            if hasattr(payload, 'model_dump_json'):
                json_data = payload.model_dump_json()
            else:
                json_data = json.dumps(payload, ensure_ascii=False, default=str)
            
            start_time = time.time()
            resp = await client.post(
                url,
                content=json_data,
                headers={"Content-Type": "application/json"}
            )
            response_time = time.time() - start_time
            
            logger.info(f"📡 HTTP Response: {resp.status_code} in {response_time:.2f}s")
            
            resp.raise_for_status()
            
            try:
                result = resp.json()
                logger.debug(f"✅ JSON response received from {url}")
                return result
            except Exception as e:
                raw_text = (await resp.aread()).decode(errors="ignore")
                logger.warning(f"⚠️ Non-JSON response from {url}: {e}")
                logger.debug(f"   Raw response: {raw_text[:200]}...")
                return {"status": "ok", "raw_text": raw_text}
                
        except httpx.TimeoutException:
            error_msg = f"Timeout after {HTTP_TIMEOUT}s connecting to {url}"
            logger.error(f"⏰ {error_msg}")
            return {"error": error_msg}
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error {e.response.status_code} from {url}: {str(e)}"
            logger.error(f"🚨 {error_msg}")
            return {"error": error_msg}
        except Exception as e:
            error_msg = f"HTTP request failed to {url}: {str(e)}"
            logger.error(f"💥 {error_msg}")
            return {"error": error_msg}

# TODO: ОПРЕДЕЛИТЬСЯ КАК МЫ БУДЕМ РАСПРЕДЕЛЯТЬ ГОТОВЫЕ ОТВЕТЫ
async def send_to_result_queue(result_message: ResultMessage):
    """Отправляет результат в очередь результатов"""
    # Эта функция будет реализована позже
    logger.info(f"📤 Would send result to queue: {result_message.original_message_id}")
    # Пока просто логируем
    if result_message.data.success:
        logger.info(f"✅ Task {result_message.original_message_id} completed successfully")
    else:
        logger.error(f"❌ Task {result_message.original_message_id} failed: {result_message.data.error_message}")

async def process_task(task: TaskMessage) -> Optional[ResultMessage]:
    """Логика обработки задачи с гарантированным возвратом"""
    logger.info(f"🔄 Starting task processing: {task.message_id}")
    
    try:
        # Определяем конфиг сервиса
        service_config = get_service_config(
            task.data.task_type,
            task.target_service
        )
        
        if not service_config:
            error_msg = f"No service configuration found for task type '{task.data.task_type}'"
            logger.error(f"❌ {error_msg}")
            return ResultMessage(
                source_service=WORKER_NAME,
                target_service=task.source_service,
                original_message_id=task.message_id,
                data=ResultData(
                    success=False,
                    error_message=error_msg,
                    execution_metadata={"worker": WORKER_NAME, "error": True}
                )
            )
        
        service_name = service_config["service_name"]
        base_url = service_config["base_url"]
        endpoint = service_config.get("endpoint", "/api/v1/process")
        target_url = f"{base_url}{endpoint}"
        
        logger.info(f"🎯 Target: {service_name} at {target_url}")
        
        # Пытаемся использовать вебхук для долгих задач
        # Добавляем callback_url для вебхука
        callback_url = f"http://{WORKER_HOST}:{WORKER_PORT}/webhook/{task.message_id}"
        
        # Создаем копию задачи с callback_url
        enhanced_input_data = {
            **task.data.input_data,
            "callback_url": callback_url,
            "webhook_supported": True
        }
        
        enhanced_task = TaskMessage(
            **{
                **task.model_dump(),
                "data": {
                    **task.data.model_dump(),
                    "input_data": enhanced_input_data
                }
            }
        )
        
        logger.info(f"🔔 Using webhook for task {task.message_id}")
        task_manager.register_async_task(task, service_config)
        
        # Отправляем задачу с вебхуком
        service_result = await send_via_http(target_url, enhanced_task.model_dump())

        
        # ОБРАБОТКА РЕЗУЛЬТАТА
        if "error" in service_result:
            logger.error(f"❌ HTTP request failed to {service_name}: {service_result['error']}")
            return ResultMessage(
                source_service=WORKER_NAME,
                target_service=task.source_service,
                original_message_id=task.message_id,
                data=ResultData(
                    success=False,
                    error_message=service_result["error"],
                    execution_metadata={"worker": WORKER_NAME, "service": service_name}
                )
            )
        
        # Если задача была отправлена асинхронно, возвращаем None - результат придет через вебхук
        logger.info(f"⏳ Task {task.message_id} processing asynchronously")
        return None

        
    except Exception as e:
        logger.error(f"💥 Unexpected error in process_task: {e}", exc_info=True)
        return ResultMessage(
            source_service=WORKER_NAME,
            target_service=task.source_service if task else "unknown",
            original_message_id=task.message_id if task else uuid4(),
            data=ResultData(
                success=False,
                error_message=f"Unexpected processing error: {str(e)}",
                execution_metadata={"worker": WORKER_NAME, "error": True}
            )
        )

async def handle_message(msg: IncomingMessage):
    """Обработка с проверкой состояния сервисов"""
    try:
        body = msg.body.decode("utf-8")
        task_message = TaskMessage.model_validate_json(body)
        
        logger.info(f"📨 Received typed message: {task_message.message_id}")
        logger.info(f"   Task: {task_message.data.task_type}")
        logger.info(f"   From: {task_message.source_service}")
        
        # Определяем целевой сервис
        service_config = get_service_config(
            task_message.data.task_type,
            task_message.target_service
        )
        
        if not service_config:
            logger.error(f"❌ No service config found for task {task_message.message_id}")
            await msg.ack()
            return
            
        service_name = service_config["service_name"]
        logger.info(f"🎯 Target service: {service_name}")
        
        # ПРОВЕРЯЕМ ГОТОВНОСТЬ СЕРВИСА
        if not await check_service_ready(service_config):
            logger.warning(f"⏸️ Service {service_name} not ready, requeuing...")
            await asyncio.sleep(5)
            await msg.nack(requeue=True)
            return
        
        # Обрабатываем задачу
        result_message = await process_task(task_message)
        
        # Подтверждаем сообщение
        await msg.ack()

        # Если есть синхронный результат - отправляем его
        if result_message:
            await send_to_result_queue(result_message)
            if result_message.data.error_message:
                logger.error(f"❌ Error: {result_message.data.error_message}")
                
    except Exception as e:
        logger.error(f"❌ Message processing failed: {e}")
        await asyncio.sleep(1)
        await msg.nack(requeue=False)


# FastAPI для вебхуков 
from fastapi import FastAPI, Request, HTTPException
import uvicorn

webhook_app = FastAPI(title=f"{WORKER_NAME}-webhook", version="1.0")

@webhook_app.post("/webhook/{message_id}")
async def webhook_handler(message_id: str, request: Request):
    """Обрабатывает вебхук уведомления от сервисов"""
    try:
        payload = await request.json()
        logger.info(f"📬 Webhook received for {message_id}: {payload.get('success', 'unknown')}")
        
        # Передаем в менеджер задач
        processed = await task_manager.handle_webhook(message_id, payload)
        
        if processed:
            return {"status": "processed"}
        else:
            logger.error(f"☢️ Webhook ignoring request: {str(payload)}")
            return {"status": "ignored"}
            
    except Exception as e:
        logger.error(f"❌ Webhook processing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@webhook_app.get("/health")
async def health():
    return {"status": "ok", "service": f"{WORKER_NAME}-webhook"}

@webhook_app.get("/tasks")
async def list_tasks():
    """Показывает активные задачи"""
    return {
        "active_tasks": len(task_manager.active_tasks),
        "tasks": list(task_manager.active_tasks.keys())
    }

async def run_webhook_server_async():
    """
    Запускает Uvicorn ASGI сервер асинхронно в текущем event loop.
    Важно: Server.serve() — корутина, поэтому её можно запустить через create_task.
    """
    config = uvicorn.Config(
        app=webhook_app,
        host="0.0.0.0",
        port=WORKER_PORT,
        log_level="info",
        # lifespan="on"  # можно включить если нужен lifespan events
    )
    server = uvicorn.Server(config)
    # server.serve() блокирует до завершения сервера — это корутина, которую мы запустим как таск
    await server.serve()



# =============================================================================
# Основная функция
# =============================================================================

async def main():
    """Основная функция инициализации"""
    try:
        # Запускаем вебхук сервер в том же event loop как background task
        asyncio.create_task(run_webhook_server_async())
        logger.info(f"🌐 Webhook server (async) start requested on port {WORKER_PORT}")
        
        # Запускаем мониторинг задач
        await task_manager.start_monitoring()
        logger.info("🔍 Async task monitor started")
        
        # Подключаемся к RabbitMQ
        connection = await connect_robust(RABBIT_URL)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)  # Обрабатываем по одной задаче

            # Убедимся, что очереди существуют
            await channel.declare_queue(QUEUE_NAME, durable=True)
            await channel.declare_queue(RESULT_QUEUE, durable=True)

            queue = await channel.get_queue(QUEUE_NAME)
            logger.info(f"🎯 Waiting for typed messages on '{QUEUE_NAME}'...")
            await queue.consume(handle_message)

            # Держим программу живой
            await asyncio.Future()
            
    except Exception as e:
        logger.error(f"❌ RabbitMQ connection failed: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Stopped by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)