# publisher.py
import os
import asyncio
import logging
from typing import Optional, Dict, Any
from aio_pika import Connection, Channel, Message, DeliveryMode
from aio_pika.exceptions import AMQPError

logger = logging.getLogger("typed-worker.publisher")


DEFAULT_RESULT_QUEUE = os.getenv("RESULT_QUEUE", "results")
DEFAULT_RETRY_TTL_MS = int(os.getenv("RETRY_TTL_MS", "5000"))   # 5s
DEFAULT_RETRY_QUEUE = os.getenv("RETRY_QUEUE", f"{os.getenv('QUEUE_NAME','tasks')}_retry")
DEFAULT_PREFETCH = 5

class Publisher:
    """
    Лёгкий инкапсулированный паблишер для aio-pika.

    Идея:
      - содержит ссылку на Connection (обычно connect_robust) и лениво/безопасно создаёт Channel,
        если он не валиден;
      - обеспечивает publish_message, publish_result и requeue_to_tail (publish->ack pattern);
      - на ошибках логирует и бросает RuntimeError (или возвращает False) — caller решает,
        как обработать (мы избегаем tight-loop внутри).
    """

    def __init__(self, connection: Connection, prefetch: int = DEFAULT_PREFETCH):
        self._connection = connection
        self._channel: Optional[Channel] = None
        self._prefetch = prefetch
        # internal lock to avoid races when (re)creating channel
        self._channel_lock = asyncio.Lock()
        
        # Кэш для single retry queue
        self._retry_queue_declared: bool = False
        self._retry_queue_name: Optional[str] = None


    async def _ensure_channel(self) -> Channel:
        """
        Удостовериться, что канал есть и открыт. Создаёт новый в случае отсутствия/закрытия.
        """
        if self._channel and not self._channel.is_closed:
            return self._channel

        async with self._channel_lock:
            # ещё раз проверить внутри локa
            if self._channel and not self._channel.is_closed:
                return self._channel

            logger.info("Publisher: creating new AMQP channel")
            self._channel = await self._connection.channel()
            try:
                await self._channel.set_qos(prefetch_count=self._prefetch)
            except Exception:
                # некоторые бэкэнды/версии могут не поддерживать set_qos, но это не fatal
                logger.debug("Publisher: set_qos failed (ignored)")
            return self._channel

    # Очередь для задач который пока что нельзя выполнить с фиксированным ttl ожидания
    async def ensure_single_retry_queue(self, retry_queue_name: str, ttl_ms: int, dead_letter_routing_key: Optional[str]):
        """Declare single retry queue (idempotent). Uses a boolean flag to avoid repeated declares."""
        if self._retry_queue_declared and self._retry_queue_name == retry_queue_name:
            return

        ch = await self._ensure_channel()
        args = {"x-message-ttl": int(ttl_ms)}
        if dead_letter_routing_key:
            args["x-dead-letter-exchange"] = ""
            args["x-dead-letter-routing-key"] = dead_letter_routing_key

        await ch.declare_queue(retry_queue_name, durable=True, arguments=args)
        self._retry_queue_declared = True
        self._retry_queue_name = retry_queue_name

    async def publish_to_retry_single(self, body: bytes, headers: Optional[Dict[str, Any]] = None, retry_queue_name: str = DEFAULT_RETRY_QUEUE):
        # ленивое объявление: если очередь ещё не отмечена как объявленная — объявляем с дефолтами
        if not self._retry_queue_declared or self._retry_queue_name != retry_queue_name:
            main_q = os.getenv("QUEUE_NAME", "tasks")
            await self.ensure_single_retry_queue(retry_queue_name=retry_queue_name, ttl_ms=int(os.getenv("RETRY_TTL_MS", DEFAULT_RETRY_TTL_MS)), dead_letter_routing_key=main_q)

        ch = await self._ensure_channel()
        msg_headers = dict(headers) if headers and isinstance(headers, dict) else {}
        msg = Message(body, headers=msg_headers, delivery_mode=DeliveryMode.PERSISTENT)
        await ch.default_exchange.publish(msg, routing_key=retry_queue_name)

    # Закинуть задачу в таски обратно    
    async def publish_message(self, body: bytes, routing_key: str,
                              headers: Optional[Dict[str, Any]] = None,
                              priority: Optional[int] = None) -> None:
        """
        Публикация arbitrary сообщения в exchange -> routing_key.
        Бросает исключение в случае фатальной ошибки.
        """
        ch = await self._ensure_channel()
        msg = Message(body, headers=(headers or {}), delivery_mode=DeliveryMode.PERSISTENT)
        if priority is not None:
            try:
                msg.priority = int(priority)
            except Exception:
                # silently ignore if underlying Message doesn't support priority attribute
                logger.debug("priority not applied: %r", priority)
        try:
            await ch.default_exchange.publish(msg, routing_key=routing_key)
        except AMQPError as e:
            logger.exception("AMQP publish error")
            raise

    async def publish_result(self, result_message, routing_key: str | None = None) -> None:
        """
        Публикует ResultMessage в очередь результатов.
        Не обращаемся к несуществующим атрибутам модели; сериализуем корректно.
        """
        rk = routing_key or DEFAULT_RESULT_QUEUE
        try:
            # Сериализация: поддерживаем pydantic v2 (model_dump_json) и v1 (json)
            if hasattr(result_message, "model_dump_json"):
                payload = result_message.model_dump_json().encode()
            elif hasattr(result_message, "json"):
                payload = result_message.json().encode()
            else:
                payload = str(result_message).encode()

            await self.publish_message(body=payload, routing_key=rk)
            logger.info("Published result %s -> %s", getattr(result_message, "original_message_id", "?"), rk)
        except Exception:
            logger.exception("Failed to publish result")
            # Не пробрасываем дальше — верхний уровень решит, что делать.
            # Но можно пробросить, если хотите, чтобы caller обрабатывал ошибку.
            raise

    async def requeue_to_tail(self, msg_body: bytes, headers: Optional[Dict[str, Any]] = None) -> None:
        """
        Републикация копии сообщения в конец очереди (publish -> caller должен ack оригинал).
        Возвращает None или бросает исключение при ошибке.
        (Здесь мы не сами ack — это ответственность вызывающего.)
        """
        logger.warning(f" ⤴️ Republish: headers: {str(headers)}")
        await self.publish_message(body=msg_body, routing_key=self._default_queue_name(), headers=headers)

    def _default_queue_name(self) -> str:
        # Этот метод может быть переопределён/переопределён пользователем.
        # По умолчанию ожидаем стандартное имя в окружении
        import os
        return os.getenv("QUEUE_NAME", "tasks")

    async def close(self) -> None:
        """Закрытие канала (не закрывает соединение)."""
        try:
            if self._channel and not self._channel.is_closed:
                await self._channel.close()
        except Exception:
            logger.exception("Error closing publisher channel")
        finally:
            self._channel = None
