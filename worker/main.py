"""
Типизированный асинхронный воркер(почти оркестр) с Pydantic-моделями + умный мониторинг задач
"""
from contextlib import asynccontextmanager
import logging
import asyncio
import os
import json
import sys
import time
import functools
import httpx
from typing import Optional, Dict, Any, Set, List, Tuple
from aio_pika import connect_robust, Message, IncomingMessage, DeliveryMode
from uuid import uuid4
# FastAPI и uvicorn для вебхуков 
from fastapi import FastAPI, Request, HTTPException
import uvicorn

# Общие модули
from common.models import MessageType, PayloadType, TaskMessage, ResultMessage, Data
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


async def check_service_ready(service_config: dict) -> bool | None:
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
                health_status = health_response.json()['status'] == 'ok'
            except Exception:
                health_status = True

            logger.info(f" 🩷 Status: {health_status}: {health_response.json()['status']}")

            if health_response.status_code != 200 or not health_status:
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


def build_enhanced_task_dict(task: TaskMessage, service_config: Dict[str, Any]) -> dict:
    """
    Собирает "enhanced" dict на основе входного TaskMessage и конфигурации сервиса.
    (отдельная функция чтобы снизить когнитивную сложность основной функции)
    """
    remaining_services = task.target_services[1:] if task.target_services else []
    payload = task.data.payload if (task.data and isinstance(task.data.payload, dict)) else {}

    worker_callback = f"http://{WORKER_HOST}:{WORKER_PORT}/webhook/{task.message_id}"

    enhanced_payload = {**payload}
    enhanced_data_dict = task.data.model_dump() if hasattr(task.data, "model_dump") else {}
    enhanced_data_dict["callback_url"] = worker_callback
    enhanced_data_dict["payload"] = enhanced_payload

    service_name = service_config["service_name"]
    new_target_services = [service_name] + remaining_services if remaining_services else [service_name]

    enhanced_task_dict = {
        **task.model_dump(),
        "data": enhanced_data_dict,
        "target_services": new_target_services
    }
    return enhanced_task_dict

async def handle_service_response(service_result: dict, task: TaskMessage, service_name: str, service_config: Dict[str, Any], msg: IncomingMessage) -> Optional[ResultMessage]:
    """
    Интерпретирует ответ от сервиса и либо:
      - возвращает ResultMessage (при ошибке),
      - либо регистрирует асинхронную задачу (если сервис принял задачу на асинхронную обработку)
      - либо возвращает None (успешное синхронное завершение, без доп. действий)
    """
    # Ошибка на уровне httpx/send_via_http
    if "error" in service_result:
        err = service_result["error"]
        logger.error(f"❌ HTTP request failed to {service_name}: {err}")
        return ResultMessage(
            message_id = uuid4(),
            message_type = MessageType.RESULT,
            source_service = WORKER_NAME,
            target_services = [task.source_service] if task.source_service else [],
            success = False,
            error_message = err
        )

    status_code = int(service_result.get("status_code", 0))

    if status_code >= 400:
        logger.error(f"❌ Service {service_name} returned error status {status_code}")
        return ResultMessage(
            source_service=WORKER_NAME,
            target_services=[task.source_service] if task.source_service else [],
            original_message_id=task.message_id,
            success = False,
            error_message = f"Service returned status {status_code}",
        )

    logger.info(f"⚙️ HTTP request to {service_name}: {status_code}")

    # Если сервис принял задачу на асинхронную обработку — регистрируем ожидание (webhook)
    if status_code in (200, 201, 202):
        logger.info(f"🔔 Service {service_name} accepted task {task.message_id} for async processing")
        # регистрируем задачу в task_manager — дальше результат придёт через вебхук
        await task_manager.register_async_task(task, service_config, msg)
        logger.info(f"⏳ Registered async wait for task {task.message_id}")
        return None

    # Для остальных кодов (например 204/2xx без явного асинхронного поведения) — ничего не делаем
    return None

async def process_task(task: TaskMessage, msg: IncomingMessage, service_config: Dict[str, Any]) -> Optional[ResultMessage]:
    """
    Обработка задачи: подготовка улучшенного payload, отправка в целевой сервис и обработка результата.
    Возвращает ResultMessage при ошибке/фейле, или None если задача обработана асинхронно (ожидаем webhook)
    или успешно синхронно (нет действий).
    """
    logger.info(f"🔄 Starting task processing: {task.message_id}")

    try:
        service_name = service_config["service_name"]
        base_url = service_config["base_url"]
        endpoint = service_config.get("endpoint", "/api/v1/process")
        target_url = f"{base_url}{endpoint}"

        logger.info(f"🎯 Target: {service_name} at {target_url}")

        # 1) Построение enhanced task (dict)
        enhanced_task_dict = build_enhanced_task_dict(task, service_config)

        # 2) Валидация (если возможна) — но не падаем при ошибке валидации
        try:
            enhanced_task =  TaskMessage.model_validate(enhanced_task_dict)
        except Exception:
            logger.debug("Enhanced task validation failed; falling back to raw dict", exc_info=False)
            enhanced_task =  enhanced_task_dict

        # 3) Отправка в сервис
        payload = enhanced_task.model_dump() if hasattr(enhanced_task, "model_dump") else enhanced_task
        service_result = await send_via_http(target_url, payload)

        # 4) Обработка ответа сервиса (включая регистрацию async ожидания)
        return await handle_service_response(service_result, task, service_name, service_config, msg)

    except Exception as e:
        logger.error(f"💥 Unexpected error in process_task: {e}", exc_info=True)
        return ResultMessage(
            source_service=WORKER_NAME,
            target_services=[task.source_service] if task else ["unknown"],
            original_message_id=task.message_id if task else uuid4(),
            success=False,
            error_message=f"Unexpected processing error: {str(e)}",
        )

def _prepare_retry_headers_and_attempts(msg: IncomingMessage) -> Tuple[dict, int]:
    """Собирает заголовки для retry и вычисляет attempts уже для этой доставки."""
    headers = dict(msg.headers) if msg.headers and isinstance(msg.headers, dict) else {}
    attempts = int(headers.get(RETRY_HEADER, 0)) + 1
    headers[RETRY_HEADER] = attempts
    return headers, attempts


async def _handle_no_service_config(task_message: TaskMessage, publisher: Publisher, msg: IncomingMessage):
    """Если сервис не найден — отправляем ResultMessage обратно и ack'аем входящее сообщение."""
    logger.error(f"❌ No service config found for task {task_message.message_id}")
    error_msg = f"No service configuration found for target services: {task_message.target_services}"
    result_message = ResultMessage(
        source_service=WORKER_NAME,
        target_services=[task_message.source_service],
        original_message_id=task_message.message_id,
        success=False,
        error_message=error_msg,
    )
    try:
        await publisher.publish_result(result_message)
    except Exception:
        logger.exception("Failed to publish result for missing service config")
    try:
        await msg.ack()
    except Exception:
        logger.exception("Failed to ack message after no service config")


async def _handle_service_unavailable_or_busy(task_message: TaskMessage,
                                              publisher: Publisher,
                                              msg: IncomingMessage,
                                              headers: dict,
                                              attempts: int,
                                              service_name: str,
                                              ready: Optional[bool]):
    """
    Логика, когда сервис либо недоступен (ready is None), либо занят (ready == False).
    При достижении MAX_RETRIES отправляем ResultMessage с ошибкой.
    Иначе — публикуем в retry-очередь и ack.
    """
    state = "unreachable" if ready is None else "busy"
    logger.warning(f"⏸️ Service {service_name} is {state} (attempts={attempts})")

    if attempts >= MAX_RETRIES and ready is None:
        logger.error(f"❌ Max retries exceeded for {task_message.message_id} (attempts={attempts})")
        result_message = ResultMessage(
            source_service=WORKER_NAME,
            target_services=[task_message.source_service],
            original_message_id=task_message.message_id,
            success=False,
            error_message=f"Service {service_name} {state} after {attempts} attempts",
        )
        try:
            await publisher.publish_result(result_message)
        except Exception:
            logger.exception("Failed to publish failure result after max retries")
        try:
            await msg.ack()
        except Exception:
            logger.exception("Failed to ack message after publishing failure result")
        return

    # Иначе — отправляем в retry queue и ack
    try:
        await publisher.publish_to_retry_single(body=msg.body, headers=headers)
    except Exception:
        logger.exception("Failed to publish to retry queue")
    finally:
        try:
            await msg.ack()
        except Exception:
            logger.exception("Failed to ack message after publish to retry")

async def handle_message(msg: IncomingMessage, publisher: Publisher):
    """
    Декомпозированная версия handle_message:
      1) парсит задачу
      2) находит конфиг сервиса
      3) проверяет готовность сервиса (health + busy)
      4) при недоступности/занятости — retry или fail
      5) при готовности — вызывает process_task и финализирует
    """
    try:
        body = msg.body.decode("utf-8")
        task_message = TaskMessage.model_validate_json(body)
        logger.info(f"📨 Received typed message: {task_message.message_id}")
        logger.info(f"   Task: {task_message.data.task_type}")
        logger.info(f"   From: {task_message.source_service}")

        # Подготовка retry headers/attempts заранее (будем использовать в любом случае)
        headers, attempts = _prepare_retry_headers_and_attempts(msg)

        # Получаем конфиг сервиса
        service_config = get_service_config(task_message.target_services)
        if not service_config:
            await _handle_no_service_config(task_message, publisher, msg)
            return

        service_name = service_config["service_name"]
        logger.info(f"🎯 Target service: {service_name}")

        # Проверяем готовность сервиса
        ready = await check_service_ready(service_config)

        # Если сервис не готов (None или False) — только тогда готовим заголовки/attempts
        if not ready:
            headers, attempts = _prepare_retry_headers_and_attempts(msg)
        else:
            # ready == True — можем продолжать нормальную обработку
            headers = {}
            attempts = 0

        # **Важно**: различаем недоступность и занятость
        if not ready:
            # контейнер/сервис недоступен (например, упал)
            await _handle_service_unavailable_or_busy(
                task_message=task_message,
                publisher=publisher,
                msg=msg,
                headers=headers,
                attempts=attempts,
                service_name=service_name,
                ready=ready
            )
            return

        # Если ready == True — обрабатываем задачу
        result_message = await process_task(task_message, msg, service_config)

        # Финализируем (опубликовать результат если есть и ack)
        if result_message:
            # публикуем результат (может бросить)
            try:
                await publisher.publish_result(result_message)
            except Exception:
                logger.exception("Failed to publish result_message from process_task")

    except Exception as e:
        logger.error(f"❌ Message processing failed: {e}", exc_info=True)
        # По ошибке — nix сообщение без requeue (чтобы не зациклить)
        try:
            await msg.nack(requeue=False)
        except Exception:
            logger.exception("Failed to nack message on exception")



# === Lifespan контекст (очень компактный) ===

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """
    Запускает monitoring и AMQP consumer как background tasks. На shutdown
    аккуратно отменяет таски и закрывает ресурсы.
    """
    logger.info("LIFESPAN: startup — launching background tasks")
    monitor_task = asyncio.create_task(task_manager.start_monitoring(), name="task_manager_monitor")
    amqp_task = asyncio.create_task(_amqp_consumer_loop(app), name="amqp_consumer_loop")

    app.state.monitor_task = monitor_task
    app.state.amqp_task = amqp_task

    try:
        yield
    finally:
        logger.info("LIFESPAN: shutdown — cancelling background tasks")

        # отменяем и ожидаем, при CancelledError — re-raise после очистки
        try:
            await _cancel_task_and_maybe_reraise(getattr(app.state, "amqp_task", None), "amqp_consumer_loop")
        except asyncio.CancelledError:
            # повторно поднимаем чтобы uvicorn / внешний сигнал получили CancelledError
            logger.info("Re-raising CancelledError after amqp cleanup")
            raise

        try:
            await _cancel_task_and_maybe_reraise(getattr(app.state, "monitor_task", None), "task_manager_monitor")
        except asyncio.CancelledError:
            logger.info("Re-raising CancelledError after monitor cleanup")
            raise

        # дополнительный guard: если publisher/connection всё ещё есть — закроем их
        pub = getattr(app.state, "publisher", None)
        if pub:
            try:
                await pub.close()
                logger.info("Publisher closed in lifespan shutdown")
            except Exception:
                logger.exception("Failed to close publisher in lifespan shutdown")

        conn = getattr(app.state, "amqp_connection", None)
        if conn:
            try:
                await conn.close()
                logger.info("AMQP connection closed in lifespan shutdown")
            except Exception:
                logger.exception("Failed to close AMQP connection in lifespan shutdown")

        # очистка state
        for attr in ("publisher", "amqp_connection", "amqp_task", "monitor_task"):
            if hasattr(app.state, attr):
                try:
                    delattr(app.state, attr)
                except Exception:
                    pass

# Привяжем lifespan к приложению (если webhook_app уже определён выше — переопределим)
webhook_app = FastAPI(title=f"{WORKER_NAME}-webhook", version="1.0", lifespan=app_lifespan)

@webhook_app.post("/webhook/{message_id}")
async def webhook_handler(message_id: str, request: Request):
    """Обрабатывает вебхук уведомления от сервисов"""
    try:
        req = await request.json()
        logger.info(f"📥 Raw webhook request body for {message_id}")
        payload: ResultMessage = ResultMessage.model_validate(req) 
        logger.info(f"📬 Webhook received for {message_id}: {payload.success}")
        
        # Передаем в менеджер задач
        processed = await task_manager.handle_webhook(message_id, payload)
        
        if processed:
            return {"status": "processed"}
        else:
            logger.error(f"☢️ Webhook ignoring request: processed: {str(processed)}")
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

# === Вспомогательные функции для работы с AMQP / Publisher ===

async def _amqp_setup(app: FastAPI):
    """
    Создаёт connection, publisher, channel и очередь.
    Возвращает кортеж (connection, publisher, channel, queue) или
    (None, None, None, None) при ошибке.
    """
    connection = await connect_robust(RABBIT_URL)
    app.state.amqp_connection = connection

    declare = os.getenv("PUBLISHER_DECLARE", "true").lower() in ("1", "true", "yes")
    publisher = Publisher(
        connection,
        prefetch=int(os.getenv("PREFETCH_COUNT", "5")),
        declare_queues=declare,
        retry_queue_name=os.getenv("RETRY_QUEUE", f"{os.getenv('QUEUE_NAME','tasks')}_retry"),
        retry_ttl_ms=int(os.getenv("RETRY_TTL_MS", "5000")),
    )
    app.state.publisher = publisher

    if declare:
        try:
            await publisher.ensure_single_retry_queue()
        except Exception:
            logger.exception("Failed to ensure retry queue on startup (publisher)")

    channel = await connection.channel()
    try:
        await channel.set_qos(prefetch_count=int(os.getenv("PREFETCH_COUNT", "5")))
    except Exception:
        logger.debug("channel.set_qos failed (ignored)")

    # idempotent declare -- infra may have already created queues
    try:
        await channel.declare_queue(os.getenv("QUEUE_NAME", "tasks"), durable=True)
        await channel.declare_queue(os.getenv("RESULT_QUEUE", "results"), durable=True)
    except Exception:
        logger.debug("Queue declare attempted (may be disabled by infra)")

    queue = await channel.get_queue(os.getenv("QUEUE_NAME", "tasks"))
    return connection, publisher, channel, queue


async def _amqp_cleanup(app: FastAPI, connection, publisher, channel):
    """Закрывает publisher/channel/connection аккуратно (игнорирует ошибки)."""
    try:
        if publisher:
            await publisher.close()
            logger.info("Publisher closed in consumer finalizer")
    except Exception:
        logger.exception("Failed to close publisher in finalizer")

    try:
        if channel and not getattr(channel, "is_closed", False):
            await channel.close()
    except Exception:
        logger.exception("Failed to close channel in finalizer")

    try:
        if connection:
            await connection.close()
            logger.info("AMQP connection closed in consumer finalizer")
    except Exception:
        logger.exception("Failed to close AMQP connection in finalizer")

    # очистка app.state (без ошибки, если атрибут не существует)
    for attr in ("publisher", "amqp_connection"):
        if hasattr(app.state, attr):
            try:
                delattr(app.state, attr)
            except Exception:
                pass


async def _attempt_consume_loop(app: FastAPI):
    """
    Непосредственный loop: setup -> start consuming -> await cancel.
    Отдельная функция чтобы основной _amqp_consumer_loop остался коротким.
    """
    connection = publisher = channel = queue = None
    try:
        connection, publisher, channel, queue = await _amqp_setup(app)
        handler = functools.partial(handle_message, publisher=publisher)
        await queue.consume(handler)
        # блокируемся до отмены (shutdown отменит задачу)
        await asyncio.Future()
    finally:
        await _amqp_cleanup(app, connection, publisher, channel)


# === Управление отменой тасков: корректная обработка CancelledError ===

async def _cancel_task_and_maybe_reraise(task: asyncio.Task, name: str):
    """
    Отменяет task, ждёт её завершения.
    Если при ожидании возник asyncio.CancelledError — логируем и повторно поднимаем.
    Другие исключения просто логируются.
    """
    if not task:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("%s cancelled (propagating CancelledError)", name)
        # Sonar требует: re-raise CancelledError after cleanup
        raise
    except Exception:
        logger.exception("%s raised during cancel/wait", name)


# === Основная задача для AMQP: короткая функция с обработкой CancelledError ===

async def _amqp_consumer_loop(app: FastAPI):
    """
    Обёртка, которая перехватывает CancelledError для корректного логирования и повторного поднятия.
    Основая логика вынесена в _attempt_consume_loop чтобы снизить когнитивную сложность.
    """
    try:
        await _attempt_consume_loop(app)
    except asyncio.CancelledError:
        logger.info("AMQP consumer loop received CancelledError -> re-raising after cleanup")
        # Пробросим вверх — ожидаем, что caller обработает (lifespan finalizer)
        raise
    except Exception:
        logger.exception("AMQP consumer loop crashed unexpectedly")

# Точка входа: пусть uvicorn укажет lifespan="on" (uvicorn вызовет наш контекст)
if __name__ == "__main__":
    import uvicorn
    try:
        uvicorn.run(
            "main:webhook_app",  # замените module:app если файл не main.py
            host="0.0.0.0",
            port=WORKER_PORT,
            log_level="info",
            lifespan="on",
            # НЕ указывайте workers > 1 если в этом процессе потребляется очередь
        )
    except KeyboardInterrupt:
        logger.info("Stopped by user")