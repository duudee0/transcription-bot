#!/usr/bin/env python3
"""
Типизированный async воркер с Pydantic-моделями:
- Принимает и отправляет сообщения в строгом формате
- Валидация входящих/исходящих данных
- Работает с нашими моделями TaskMessage, ResultMessage
"""

import asyncio
import os
import json
import sys
from aio_pika import connect_robust, Message, IncomingMessage
import httpx

# Импортируем наши модели
from common.models import TaskMessage, ResultMessage, ResultData, MessageType

# Конфиг через env
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")
RESULT_QUEUE = os.getenv("RESULT_QUEUE", "results")
SEND_METHOD = os.getenv("SEND_METHOD", "http")
TARGET_URL = os.getenv("TARGET_URL", "http://llm-service:8000/api/v1/infer")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10.0"))
WORKER_NAME = os.getenv("WORKER_NAME", "generic-worker")

# Конфигурация сервисов (добавляем новые сервисы сюда)
SERVICE_CONFIGS = {
    "analyze_text": {
        "base_url": "http://llm-service:8000",
        "service_name": "llm-service"
    },
    # "process_image": {
    #     "base_url": "http://image-service:8000", 
    #     "service_name": "image-service"
    # },
}

print(f"🚀 Typed worker '{WORKER_NAME}' starting...", file=sys.stderr)
print(f"Config: RABBIT_URL={RABBIT_URL}, QUEUE={QUEUE_NAME}, SEND_METHOD={SEND_METHOD}", file=sys.stderr)


async def check_service_ready(service_config: dict) -> bool:
    """Проверяет что сервис готов к работе (health + status)"""
    base_url = service_config["base_url"]
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Проверяем здоровье
            health_response = await client.get(f"{base_url}/health")
            if health_response.status_code != 200:
                return False
            
            # Проверяем занятость
            status_response = await client.get(f"{base_url}/status")
            if status_response.status_code == 200:
                status_data = status_response.json()
                return not status_data.get("is_busy", False)
            
            return False
    except Exception:
        return False


def get_service_config(task_type: str, target_service: str = None) -> dict:
    """Определяет какой сервис должен обрабатывать задачу"""
    # Если явно указан целевой сервис
    if target_service:
        for config in SERVICE_CONFIGS.values():
            if config["service_name"] == target_service:
                return config
    
    # Определяем по типу задачи
    if task_type in SERVICE_CONFIGS:
        return SERVICE_CONFIGS[task_type]
    
    # Fallback - используем первый доступный сервис
    return next(iter(SERVICE_CONFIGS.values()))


async def send_via_http(payload) -> dict:
    """Исправленная функция отправки HTTP"""
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        try:
            # Сериализуем Pydantic модель в JSON строку
            if hasattr(payload, 'model_dump_json'):
                json_data = payload.model_dump_json()
            else:
                json_data = json.dumps(payload, ensure_ascii=False, default=str)
                
            print(f"🌐 Sending HTTP request", file=sys.stderr)
            
            resp = await client.post(
                TARGET_URL,
                content=json_data,
                headers={"Content-Type": "application/json"}
            )
            resp.raise_for_status()
            
            try:
                return resp.json()
            except Exception:
                return {"status": "ok", "raw_text": (await resp.aread()).decode(errors="ignore")}
                
        except Exception as e:
            print(f"❌ HTTP send failed: {e}", file=sys.stderr)
            return {"error": str(e)}


async def publish_to_queue(channel, message: ResultMessage, queue_name: str):
    """Опубликовать типизированное сообщение в очередь"""
    try:
        # Сериализуем Pydantic-модель в JSON
        body = message.model_dump_json().encode()
        await channel.default_exchange.publish(
            Message(body=body, delivery_mode=2),  # delivery_mode=2 для persistent
            routing_key=queue_name
        )
        print(f"✅ Published to '{queue_name}': {message.message_id}", file=sys.stderr)
    except Exception as e:
        print(f"❌ Queue publish failed: {e}", file=sys.stderr)
        raise


async def process_task(task: TaskMessage) -> ResultMessage:
    """Логика обработки задачи"""
    print(f"🔄 Processing task: {task.data.task_type}", file=sys.stderr)
    
    try:
        # Отправляем ВЕСЬ TaskMessage в llm-service
        llm_result = await send_via_http(task.model_dump())
        
        # llm-service вернет ResultMessage, который мы можем вернуть как есть
        # или преобразовать если нужно
        return ResultMessage.model_validate(llm_result)
        
    except Exception as e:
        return ResultMessage(
            source_service=WORKER_NAME,
            target_service=task.source_service,
            original_message_id=task.message_id,
            data=ResultData(
                success=False,
                error_message=str(e),
                execution_metadata={"worker": WORKER_NAME, "error": True}
            )
        )


async def handle_message(msg: IncomingMessage):
    """Обработка с проверкой состояния сервисов"""
    async with msg.process(requeue=True):  # Изменяем на requeue=True для возврата в очередь
        try:
            body = msg.body.decode("utf-8")
            task_message = TaskMessage.model_validate_json(body)
            
            print(f"📨 Received typed message: {task_message.message_id}")
            print(f"   Task: {task_message.data.task_type}")
            print(f"   From: {task_message.source_service}")
            
            # Определяем целевой сервис
            service_config = get_service_config(
                task_message.data.task_type,
                task_message.target_service
            )
            
            service_name = service_config["service_name"]
            print(f"🎯 Target service: {service_name}")
            
            # ПРОВЕРЯЕМ ГОТОВНОСТЬ СЕРВИСА
            if not await check_service_ready(service_config):
                print(f"⏸️ Service {service_name} not ready, requeuing...")
                await asyncio.sleep(5)  # Ждем перед повторной попыткой
                return  # Сообщение вернется в очередь благодаря requeue=True
            
            # Обрабатываем задачу
            result_message = await process_task(task_message)
            
            # ПРОСТО ЛОГИРУЕМ РЕЗУЛЬТАТ вместо отправки
            print(f"✅ Task completed: {result_message.data.success}")
            print(f"✅ Result: {result_message.data.result}")
            
            if result_message.data.error_message:
                print(f"❌ Error: {result_message.data.error_message}")
                
        except Exception as e:
            print(f"❌ Message processing failed: {e}")
            await asyncio.sleep(1)  # Короткая пауза перед повторной попыткой


async def main():
    """Основная функция инициализации"""
    try:
        connection = await connect_robust(RABBIT_URL)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)

            # Убедимся, что очереди существуют
            await channel.declare_queue(QUEUE_NAME, durable=True)
            await channel.declare_queue(RESULT_QUEUE, durable=True)

            queue = await channel.get_queue(QUEUE_NAME)
            print(f"🎯 Waiting for typed messages on '{QUEUE_NAME}'...", file=sys.stderr)
            await queue.consume(handle_message)

            # Держим программу живой
            await asyncio.Future()
            
    except Exception as e:
        print(f"❌ RabbitMQ connection failed: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("⏹️ Stopped by user", file=sys.stderr)
    except Exception as e:
        print(f"💥 Fatal error: {e}", file=sys.stderr)
        sys.exit(1)