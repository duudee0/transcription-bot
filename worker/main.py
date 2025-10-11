#!/usr/bin/env python3
"""
Типизированный async воркер с Pydantic-моделями:
- Принимает и отправляет сообщения в строгом формате
- Валидация входящих/исходящих данных
- Работает с нашими моделями TaskMessage, ResultMessage
"""
import logging
import asyncio
import os
import json
import sys
import time
from typing import Optional, Dict, Any
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
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "36.0"))
WORKER_NAME = os.getenv("WORKER_NAME", "generic-worker")

# Конфигурация сервисов (добавляем новые сервисы сюда)
SERVICE_CONFIGS = {
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
        logging.FileHandler('/var/log/worker.log')  # если нужно в файл
    ]
)
logger = logging.getLogger("typed-worker")

print(f"🚀 Typed worker '{WORKER_NAME}' starting...", file=sys.stderr)
print(f"Config: RABBIT_URL={RABBIT_URL}, QUEUE={QUEUE_NAME}, SEND_METHOD={SEND_METHOD}", file=sys.stderr)


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
            
            # ИСПРАВЛЕНИЕ: используем time.time() для измерения времени
            start_time = time.time()
            resp = await client.post(
                url,
                content=json_data,
                headers={"Content-Type": "application/json"}
            )
            response_time = time.time() - start_time  # Просто вычитаем float значения
            
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
        
        # Отправляем задачу в сервис
        logger.info(f"🚀 Sending task to {service_name}...")
        service_result = await send_via_http(target_url, task.model_dump())
        
        # ОБРАБОТКА РЕЗУЛЬТАТА - ДОБАВЬ ПРОВЕРКИ
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
        
        logger.info(f"✅ Successfully received response from {service_name}")
        
        # Пытаемся создать ResultMessage из ответа
        try:
            if isinstance(service_result, dict) and "message_type" in service_result:
                result_msg = ResultMessage.model_validate(service_result)
                result_msg.source_service = WORKER_NAME
                result_msg.target_service = task.source_service
                return result_msg
            else:
                # Обертываем сырой ответ
                return ResultMessage(
                    source_service=WORKER_NAME,
                    target_service=task.source_service,
                    original_message_id=task.message_id,
                    data=ResultData(
                        success=True,
                        result=service_result,
                        execution_metadata={
                            "worker": WORKER_NAME,
                            "service": service_name,
                            "processed_via": "http"
                        }
                    )
                )
                
        except Exception as validation_error:
            logger.error(f"❌ Response validation failed: {validation_error}")
            return ResultMessage(
                source_service=WORKER_NAME,
                target_service=task.source_service,
                original_message_id=task.message_id,
                data=ResultData(
                    success=False,
                    error_message=f"Invalid response format: {validation_error}",
                    execution_metadata={"worker": WORKER_NAME, "service": service_name}
                )
            )
        
    except Exception as e:
        logger.error(f"💥 Unexpected error in process_task: {e}", exc_info=True)
        # ГАРАНТИРОВАННЫЙ ВОЗВРАТ ПРИ ЛЮБОЙ ОШИБКЕ
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
        
        service_name = service_config["service_name"]
        logger.info(f"🎯 Target service: {service_name}")
        
        # ПРОВЕРЯЕМ ГОТОВНОСТЬ СЕРВИСА
        if not await check_service_ready(service_config):
            logger.warning(f"⏸️ Service {service_name} not ready, requeuing...")
            await asyncio.sleep(5)  # Ждем перед повторной попыткой
            await msg.nack(requeue=True)  # Явно возвращаем в очередь
            return  # Сообщение вернется в очередь благодаря requeue=True
        
        # Обрабатываем задачу
        result_message = await process_task(task_message)
        
        # ПРОСТО ЛОГИРУЕМ РЕЗУЛЬТАТ вместо отправки
        #logger.info(f"✅ Task completed: {result_message.data.success}")

        
        # Подтверждаем вручную после УСПЕШНОЙ обработки
        await msg.ack()

        if result_message.data.error_message:
            logger.error(f"❌ Error: {result_message.data.error_message}")
                
    except Exception as e:
        logger.error(f"❌ Message processing failed: {e}")
        await asyncio.sleep(1)  # Короткая пауза перед повторной попыткой
        await msg.nack(requeue=False)  # Не возвращать


async def main():
    """Основная функция инициализации"""
    try:
        connection = await connect_robust(RABBIT_URL)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=0)

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