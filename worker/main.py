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
import functools
import httpx
from typing import Optional, Dict, Any, Set, List
from aio_pika import connect_robust, Message, IncomingMessage, DeliveryMode
from uuid import uuid4
# FastAPI и uvicorn для вебхуков 
from fastapi import FastAPI, Request, HTTPException
import uvicorn

# Общие модули
from common.models import TaskMessage, ResultMessage, ResultData, MessageType
from common.publisher import Publisher
from common.service_config import get_service_url

from task_manager import AsyncTaskManager, send_to_result_queue


# Конфиг через env
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")
RESULT_QUEUE = os.getenv("RESULT_QUEUE", "results")

# Републикация поломанных заданий
RETRY_HEADER = "x-retries"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

SEND_METHOD = os.getenv("SEND_METHOD", "http")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "5.0"))
WORKER_NAME = os.getenv("WORKER_NAME", "generic-worker")
WORKER_HOST = os.getenv("WORKER_HOST", "worker")
WORKER_PORT = int(os.getenv("WORKER_PORT", "8080"))


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


# Глобальный менеджер задач
task_manager = AsyncTaskManager()

webhook_app = FastAPI(title=f"{WORKER_NAME}-webhook", version="1.0")


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
            
            try:
                health_status = health_response.json().status == 'ok'
            except:
                health_status = True

            logger.info(f" 🩷 Status: {health_status}")

            if health_response.status_code != 200 and not health_status:
                logger.warning(f"   ❌ Health check failed: {health_response.status_code}")
                return None
            
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
        return None

def get_service_config(target_services: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    """Определяет конфиг сервиса на основе target_services"""
    
    logger.info(f"🔍 Looking up service config for target_services='{target_services}'")
    
    if not target_services or len(target_services) == 0:
        logger.error("❌ No target_services provided")
        return None
    
    target_service = target_services[0]
    logger.info(f"🎯 Using first from target_services: {target_service}")
    
    base_url = get_service_url(target_service)
    if not base_url:
        logger.warning(f"❌ Target service '{target_service}' not found in SERVICE_REGISTRY")
        return None
    
    # Возвращаем простой конфиг
    return {
        "service_name": target_service,
        "base_url": base_url,
        "endpoint": "/api/v1/process"  # Стандартный endpoint
    }

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
            
            # Попробуем распарсить JSON; если не JSON — вернём текст
            try:
                body = resp.json()
            except Exception:
                body = (await resp.aread()).decode(errors="ignore")

            return {"status_code": resp.status_code, "body": body}
                
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



async def process_task(task: TaskMessage, msg: IncomingMessage, service_config: Dict[str, Any]) -> Optional[ResultMessage]:
    """Логика обработки задачи с гарантированным возвратом"""
    logger.info(f"🔄 Starting task processing: {task.message_id}")
    
    try:
        service_name = service_config["service_name"]
        base_url = service_config["base_url"]
        endpoint = service_config.get("endpoint", "/api/v1/process")
        target_url = f"{base_url}{endpoint}"
        
        logger.info(f"🎯 Target: {service_name} at {target_url}")
        
        # Подготавливаем данные для следующего сервиса в цепочке (НОВАЯ ЛОГИКА)
        remaining_services = task.target_services[1:] if task.target_services else []

        # Если есть следующие сервисы, готовим данные для цепочки
        enhanced_input_data = {
            **task.data.input_data,
            "callback_url": f"http://{WORKER_HOST}:{WORKER_PORT}/webhook/{task.message_id}",
        }
        
        # Если есть цепочка, передаем оставшиеся сервисы
        enhanced_task_data = {
            **task.data.model_dump(),
            "input_data": enhanced_input_data
        }
        
        # Создаем enhanced task с обновленными target_services если есть цепочка
        enhanced_task = TaskMessage(
            **{
                **task.model_dump(),
                "data": enhanced_task_data,
                "target_services": [service_name] + remaining_services if remaining_services else None
            }
        )
        
        service_result = await send_via_http(target_url, enhanced_task.model_dump())

        # ОБРАБОТКА РЕЗУЛЬТАТА
        if "error" in service_result:
            logger.error(f"❌ HTTP request failed to {service_name}: {service_result['error']}")
            return ResultMessage(
                source_service=WORKER_NAME,
                target_services=[task.source_service],
                original_message_id=task.message_id,
                data=ResultData(
                    success=False,
                    error_message=service_result["error"],
                    execution_metadata={"worker": WORKER_NAME, "service": service_name}
                )
            )
        
        status_code = int(service_result.get("status_code", 0))

        # Если сервер вернул ошибку — сразу возвращаем ошибку
        # (эта проверка уже была выше по "error" ключу, но на всякий случай)
        if status_code >= 400:
            return ResultMessage(
                source_service=WORKER_NAME,
                target_service=task.source_service,
                original_message_id=task.message_id,
                data=ResultData(
                    success=False,
                    error_message=f"Service returned status {status_code}",
                    execution_metadata={"worker": WORKER_NAME, "service": service_name}
                )
            )
        
        logger.info(f"⚙️ HTTP request to {service_name}: {str(status_code)}")

        # Решение: если сервис вернул 202 или явно отметил 'accepted'/'queued' -> регистрируем async
        if status_code in (200, 201, 202):
            # если тело содержит явный индикатор
            # Отправляем задачу с вебхуком
            logger.info(f"🔔 Using webhook for task {task.message_id}")
            await task_manager.register_async_task(task, service_config, msg)   
            # Если задача была отправлена асинхронно, возвращаем None - результат придет через вебхук
            logger.info(f"⏳ Task {task.message_id} processing asynchronously")
            return None
        
    except Exception as e:
        logger.error(f"💥 Unexpected error in process_task: {e}", exc_info=True)
        return ResultMessage(
            source_service=WORKER_NAME,
            target_services=[task.source_service] if task else ["unknown"],
            original_message_id=task.message_id if task else uuid4(),
            data=ResultData(
                success=False,
                error_message=f"Unexpected processing error: {str(e)}",
                execution_metadata={"worker": WORKER_NAME, "error": True}
            )
        )

async def handle_message(msg: IncomingMessage, publisher: Publisher):
    """Обработка с проверкой состояния сервисов"""
    try:
        body = msg.body.decode("utf-8")
        task_message = TaskMessage.model_validate_json(body)
        
        logger.info(f"📨 Received typed message: {task_message.message_id}")
        logger.info(f"   Task: {task_message.data.task_type}")
        logger.info(f"   From: {task_message.source_service}")
        
        # Определяем целевой сервис
        service_config = get_service_config(task_message.target_services)

        if not service_config:
            logger.error(f"❌ No service config found for task {task_message.message_id}")
            error_msg = f"No service configuration found for target services: {task_message.target_services}"
            logger.error(f"❌ {error_msg}")

            await msg.ack()
            return ResultMessage(
                source_service=WORKER_NAME,
                target_services=[task_message.source_service],
                original_message_id=task_message.message_id,
                data=ResultData(
                    success=False,
                    error_message=error_msg,
                    execution_metadata={"worker": WORKER_NAME, "error": True}
                )
            )

            
        service_name = service_config["service_name"]
        logger.info(f"🎯 Target service: {service_name}")
        
        # ПРОВЕРЯЕМ ГОТОВНОСТЬ СЕРВИСА
        ready = await check_service_ready(service_config)

        if not ready:
            # подготовка attempts и headers
            headers = dict(msg.headers) if msg.headers and isinstance(msg.headers, dict) else {}
            attempts = int(headers.get(RETRY_HEADER, 0)) + 1
            headers[RETRY_HEADER] = attempts            

        if ready == None: # Если контейнер просто мертв
            logger.warning(f"⏸️ Service {service_name} not ready — will requeue to tail")

            logger.warning(f" + exceeded for {task_message.message_id} (attempts={attempts}) — sending failure")

            if attempts >= MAX_RETRIES:
                logger.error(f"❌ Max retries exceeded for {task_message.message_id} (attempts={attempts}) — sending failure")

                result_message = ResultMessage(
                    source_service=WORKER_NAME,
                    target_services=[task_message.source_service],
                    original_message_id=task_message.message_id,
                    data=ResultData(
                        success=False,
                        error_message=f"Service {service_name} unavailable after {attempts} attempts",
                        execution_metadata={"worker": WORKER_NAME, "service": service_name, "retries": attempts}
                    )
                )
                # используем publisher чтобы отправить result
                # Попытаемся опубликовать результат, но ошибка публикации не должна
                # приводить к попытке nack на уже ack'нутом сообщении.
                try:
                    await publisher.publish_result(result_message)
                    # Убираем из очереди
                    try:
                        await msg.ack()
                    except Exception:
                        logger.exception(" ⛔⛔ Failed to ack message before publishing result")

                except Exception as e:
                    logger.exception("Failed to publish result for %s: %s", task_message.message_id, e)
                    # Возможные опции:
                    # - логируем и возвращаем (мы уже ack'нули исходное сообщение)
                    # - сохраняем результат в локальный файл/базу как запасной вариант
                return
        
        # Если просто занят 
        elif ready == False:
            #TODO: ЛОГИКА ЕСЛИ ЗАНЯТ ПРОСТО, НАДО ДОБАВИТЬ СУПЕР МАКСИМАЛЬНОЕ ВРЕМЯ ОТВЕТА
            logger.info(f" Service {service_name} is busy")
            #await msg.nack(requeue=True)
        
        if not ready:
            # Убираем из очереди и кладем на ожидание
            try:
                await publisher.publish_to_retry_single(body=msg.body, headers=headers)
                await msg.ack()
            except Exception:
                logger.exception(" ⛔⛔ Failed to ack message before publishing result")

            return
        
        # Обрабатываем задачу
        result_message = await process_task(task_message, msg, service_config)

        # Обработка ошибок
        if result_message:
            if result_message.data.error_message:
                #TODO СДЕЛАТЬ ОБРАБОТКУ ОШИБОК
                logger.error(f"❌ 'error' in result_message: {result_message.data.error_message}")
                await msg.ack() # В парашу его не рабочее

                
    except Exception as e:
        logger.error(f"❌ Message processing failed: {e}")
        await asyncio.sleep(1)
        await msg.nack(requeue=False)


@webhook_app.post("/webhook/{message_id}")
async def webhook_handler(message_id: str, request: Request):
    """Обрабатывает вебхук уведомления от сервисов"""
    try:
        payload = await request.json()
        logger.info(f"📬 Webhook received for {message_id}: {payload.get('data').get('success', 'None success')}")
        
        # Передаем в менеджер задач
        processed = await task_manager.handle_webhook(message_id, payload)
        
        if processed:
            return {"status": "processed"}
        else:
            logger.error(f"☢️ Webhook ignoring request: {str(payload)} \n + processed: {str(processed)}")
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
            declare = os.getenv("PUBLISHER_DECLARE", "true").lower() in ("1","true","yes")
            publisher = Publisher(
                connection,
                prefetch=int(os.getenv("PREFETCH_COUNT", "5")),
                declare_queues=declare,
                retry_queue_name=os.getenv("RETRY_QUEUE", f"{os.getenv('QUEUE_NAME','tasks')}_retry"),
                retry_ttl_ms=int(os.getenv("RETRY_TTL_MS", "5000")),
            )

            # Создаем заранее очередь для повтора тасков
            if declare:
                try:
                    await publisher.ensure_single_retry_queue()
                except Exception:
                    logger.exception("Failed to ensure retry queue on startup")

            #task_manager.publisher = publisher

            # start monitoring
            await task_manager.start_monitoring()

            # create consumer channel and handler as before, pass publisher into handle_message if needed
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=int(os.getenv("PREFETCH_COUNT", "5")))
            await channel.declare_queue(os.getenv("QUEUE_NAME", "tasks"), durable=True)
            await channel.declare_queue(os.getenv("RESULT_QUEUE", "results"), durable=True)

            queue = await channel.get_queue(os.getenv("QUEUE_NAME", "tasks"))
            handler = functools.partial(handle_message, publisher=publisher)  # если handle_message принимает publisher
            await queue.consume(handler)

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