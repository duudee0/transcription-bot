#!/usr/bin/env python3
"""
Минимальный async воркер:
- подключается к RabbitMQ (RABBIT_URL)
- слушает очередь QUEUE_NAME
- для каждого сообщения:
    - печатает JSON в консоль
    - отправляет payload другим контейнерам:
        * по HTTP (POST в TARGET_URL) если SEND_METHOD=http
        * в результатную очередь RESULT_QUEUE если SEND_METHOD=rabbit
- простая обработка ошибок (пока без retries/DLQ)
"""

import asyncio
import os
import json
import sys
from aio_pika import connect_robust, Message, IncomingMessage
import httpx

# Конфиг через env (примитивно)
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")
RESULT_QUEUE = os.getenv("RESULT_QUEUE", "results")
SEND_METHOD = os.getenv("SEND_METHOD", "http")  # "http" или "rabbit"
TARGET_URL = os.getenv("TARGET_URL", "http://llm-service:8000/api/v1/infer")
# Таймаут для HTTP вызовов в секундах
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10.0"))

print("Worker starting with config:",
      f"RABBIT_URL={RABBIT_URL}, QUEUE={QUEUE_NAME}, SEND_METHOD={SEND_METHOD}", file=sys.stderr)


async def send_via_http(payload: dict):
    """Отправить payload по HTTP POST в TARGET_URL"""

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        try:
            resp = await client.post(TARGET_URL, json=payload)
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception:
                return {"status": "ok", "raw_text": (await resp.aread()).decode(errors="ignore")}
        except Exception as e:
            # В тестовой версии просто печатаем ошибку
            print("HTTP send failed:", e, file=sys.stderr)
            return {"error": str(e)}


async def publish_to_queue(channel, payload: dict, queue_name: str):
    """Опубликовать payload в очередь queue_name (в том же RabbitMQ)"""

    body = json.dumps(payload, ensure_ascii=False).encode()
    await channel.default_exchange.publish(Message(body=body), routing_key=queue_name)


async def handle_message(msg: IncomingMessage):
    """Callback обработки входящего сообщения"""

    async with msg.process(requeue=False):
        try:
            body = msg.body.decode("utf-8")
            data = json.loads(body)
        except Exception as e:
            print("Invalid message, skipping. Error:", e, file=sys.stderr)
            return

        # Просто печатаем то, что пришло
        print("Received message:", json.dumps(data, ensure_ascii=False), file=sys.stderr)

        # В тестовом формате ожидаем, что message содержит dict (payload)
        # Ты можешь адаптировать под структуру: { "request_id": "...", "type":"...", "payload": {...} }
        payload_to_send = data  # если нужно — трансформируй здесь

        if SEND_METHOD.lower() == "http":
            result = await send_via_http(payload_to_send)
            print("HTTP result:", json.dumps(result, ensure_ascii=False), file=sys.stderr)
        else:
            # публикуем в RESULT_QUEUE
            channel = msg.channel  # get channel bound to message
            # но msg.channel это coroutine property in aio-pika, ensure get it:
            if callable(channel):
                channel = await channel
            await publish_to_queue(channel, payload_to_send, RESULT_QUEUE)
            print(f"Published to queue '{RESULT_QUEUE}'", file=sys.stderr)


async def main():
    connection = await connect_robust(RABBIT_URL)
    async with connection:
        channel = await connection.channel()
        # опционально: set QoS
        await channel.set_qos(prefetch_count=1)

        # Убедимся, что очереди существуют
        await channel.declare_queue(QUEUE_NAME, durable=True)
        await channel.declare_queue(RESULT_QUEUE, durable=True)

        queue = await channel.get_queue(QUEUE_NAME)
        print(f"Waiting for messages on '{QUEUE_NAME}' ...", file=sys.stderr)
        await queue.consume(handle_message)

        # Держим программу живой
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())   # main() — настройка подключения и подписки на очередь
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        # Если вдруг main() возвращается, удерживаем процесс (в тестовой среде)
        print("main returned — keeping process alive for debugging")
        import time
        while True:
            time.sleep(3600)

