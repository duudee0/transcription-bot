import os
import asyncio
import logging
import json
import time
from typing import Optional, Dict, Any, Callable
from aio_pika import Connection, Channel, Message, DeliveryMode
from aio_pika.exceptions import AMQPError

logger = logging.getLogger("typed-worker.publisher")

DEFAULT_RESULT_QUEUE = os.getenv("RESULT_QUEUE", "results")
DEFAULT_RETRY_TTL_MS = int(os.getenv("RETRY_TTL_MS", "5000"))   # 5s
DEFAULT_RETRY_QUEUE = os.getenv("RETRY_QUEUE", f"{os.getenv('QUEUE_NAME','tasks')}_retry")
DEFAULT_PREFETCH = int(os.getenv("PREFETCH_COUNT", "5"))

class PublisherError(RuntimeError):
    pass

class Publisher:
    def __init__(
        self,
        connection: Connection,
        prefetch: int = DEFAULT_PREFETCH,
        *,
        declare_queues: bool = True,
        retry_queue_name: Optional[str] = None,
        retry_ttl_ms: Optional[int] = None,
        rate_limit: Optional[int] = None,
        claim_check_threshold_bytes: Optional[int] = None,
        injected_channel: Optional[Channel] = None,
        upload_hook: Optional[Callable[[bytes], str]] = None,  # sync or async fn -> returns url/key
    ):
        """
        :param connection: aio_pika connection (per-process)
        :param declare_queues: если True — Publisher может делать declare_queue (требует configure права).
                               В prod обычно False и infra создает очереди заранее.
        :param retry_queue_name, retry_ttl_ms: конфиг для single retry queue
        :param rate_limit: Максимум параллельных publish (None — не ограничивать)
        :param claim_check_threshold_bytes: если тело > threshold — вызвать upload_hook и отправить pointer
        :param injected_channel: опционально инжектить Channel (для тестов)
        :param upload_hook: callable(data: bytes) -> url/key (если claim-check нужен)
        """
        self._connection = connection
        self._prefetch = prefetch
        self._channel: Optional[Channel] = injected_channel
        self._channel_lock = asyncio.Lock()
        self._declare_queues = bool(declare_queues)
        self._retry_queue_name = retry_queue_name or DEFAULT_RETRY_QUEUE
        self._retry_ttl_ms = int(retry_ttl_ms or DEFAULT_RETRY_TTL_MS)
        self._rate_limit = asyncio.Semaphore(rate_limit) if rate_limit else None
        self._claim_check_threshold = claim_check_threshold_bytes
        self._upload_hook = upload_hook  # may be None -> claim-check disabled
        # simple state flags
        self._retry_queue_declared = False
        # lightweight metrics (in-memory); integrate with Prometheus externally if needed
        self.metrics = {
            "publish_success": 0,
            "publish_fail": 0,
            "publish_retry": 0,
            "publish_bytes": 0,
            "republishes": 0,
        }

    # -------------------------
    # context manager
    # -------------------------
    async def __aenter__(self):
        # ensure channel lazily
        await self._ensure_channel()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    # -------------------------
    # internal helpers
    # -------------------------
    async def _ensure_channel(self) -> Channel:
        if self._channel and not getattr(self._channel, "is_closed", False):
            return self._channel

        async with self._channel_lock:
            if self._channel and not getattr(self._channel, "is_closed", False):
                return self._channel
            logger.info("Publisher: creating new AMQP channel")
            self._channel = await self._connection.channel()
            try:
                await self._channel.set_qos(prefetch_count=self._prefetch)
            except Exception:
                logger.debug("Publisher: set_qos failed (ignored)")
            return self._channel

    async def ensure_single_retry_queue(self):
        """
        Declares single retry queue (idempotent). Honours self._declare_queues flag.
        If declare_queues == False -> just log and return (no exception).
        """
        if self._retry_queue_declared:
            return

        if not self._declare_queues:
            logger.info("Publisher: declare_queues disabled — skipping retry queue declaration")
            return

        ch = await self._ensure_channel()
        args = {"x-message-ttl": int(self._retry_ttl_ms)}
        main_q = os.getenv("QUEUE_NAME", "tasks")
        args["x-dead-letter-exchange"] = ""
        args["x-dead-letter-routing-key"] = main_q

        # Attempt with small retry/backoff to avoid race on broker
        attempts = 3
        backoff = 0.05
        for i in range(attempts):
            try:
                logger.info("Publisher: declaring retry queue %s ttl=%sms (attempt %d)", self._retry_queue_name, self._retry_ttl_ms, i+1)
                await ch.declare_queue(self._retry_queue_name, durable=True, arguments=args)
                self._retry_queue_declared = True
                return
            except Exception as e:
                logger.warning("Publisher: declare_queue attempt %d failed: %s", i+1, e)
                if i + 1 == attempts:
                    logger.exception("Publisher: failed to declare retry queue after retries")
                    # If declare_queues is required for your service, raise; otherwise continue
                    raise
                await asyncio.sleep(backoff)
                backoff *= 2

    async def _maybe_claim_check(self, body: bytes) -> tuple[bytes, bool]:
        """If body size > threshold and upload_hook provided, upload and return small pointer payload."""
        if self._claim_check_threshold and self._upload_hook and len(body) > self._claim_check_threshold:
            logger.info("Publisher: claim-check triggered for %d bytes", len(body))
            # support sync or async upload_hook
            if asyncio.iscoroutinefunction(self._upload_hook):
                url = await self._upload_hook(body)
            else:
                url = self._upload_hook(body)
            pointer = {"claim_check": True, "url": url, "original_size": len(body)}
            return (json.dumps(pointer).encode(), True)
        return body, False

    # -------------------------
    # publish with retry/backoff
    # -------------------------
    async def publish_message(self, body: bytes, routing_key: str,
                              headers: Optional[Dict[str, Any]] = None,
                              priority: Optional[int] = None,
                              max_attempts: int = 3) -> None:
        """
        Robust publish: tries a few times on AMQP errors with exponential backoff.
        Raises PublisherError on fatal failure.
        """
        if self._rate_limit:
            await self._rate_limit.acquire()
        try:
            ch = await self._ensure_channel()
            body_to_send, cc = await self._maybe_claim_check(body)
            msg_headers = dict(headers or {})
            msg = Message(body_to_send, headers=msg_headers, delivery_mode=DeliveryMode.PERSISTENT)
            if priority is not None:
                try:
                    msg.priority = int(priority)
                except Exception:
                    logger.debug("priority not applied: %r", priority)

            attempt = 0
            backoff = 0.05
            while True:
                try:
                    await ch.default_exchange.publish(msg, routing_key=routing_key)
                    self.metrics["publish_success"] += 1
                    self.metrics["publish_bytes"] += len(body_to_send)
                    logger.debug("Published message -> %s (bytes=%d claim_check=%s headers=%s)",
                                 routing_key, len(body_to_send), cc, list(msg_headers.keys()))
                    return
                except AMQPError as e:
                    attempt += 1
                    self.metrics["publish_retry"] += 1
                    logger.warning("Publish attempt %d failed to %s: %s", attempt, routing_key, e)
                    if attempt >= max_attempts:
                        self.metrics["publish_fail"] += 1
                        logger.exception("Publish failed after %d attempts", attempt)
                        raise PublisherError(f"failed to publish to {routing_key}: {e}")
                    await asyncio.sleep(backoff)
                    backoff *= 2
        finally:
            if self._rate_limit:
                try:
                    self._rate_limit.release()
                except Exception:
                    pass

    # -------------------------
    # higher-level helpers
    # -------------------------
    async def publish_json(self, obj: Any, routing_key: str, headers: Optional[Dict[str, Any]] = None):
        body = json.dumps(obj, ensure_ascii=False).encode()
        await self.publish_message(body=body, routing_key=routing_key, headers=headers)

    async def publish_task(self, task_message, routing_key: Optional[str] = None, headers: Optional[Dict[str,Any]] = None) -> Optional[str]:
        """
        Serializes pydantic task model (v2/v1) and publishes to routing_key or default QUEUE_NAME.
        Returns message_id as string if present on the model.
        """
        if hasattr(task_message, "model_dump_json"):
            payload = task_message.model_dump_json().encode()
        elif hasattr(task_message, "json"):
            payload = task_message.json().encode()
        else:
            payload = json.dumps(task_message).encode()

        rk = routing_key or os.getenv("QUEUE_NAME", "tasks")
        await self.publish_message(payload, routing_key=rk, headers=headers)
        try:
            return str(task_message.message_id)
        except Exception:
            return None

    async def publish_result(self, result_message, routing_key: Optional[str] = None):
        rk = routing_key or DEFAULT_RESULT_QUEUE
        if hasattr(result_message, "model_dump_json"):
            payload = result_message.model_dump_json().encode()
        elif hasattr(result_message, "json"):
            payload = result_message.json().encode()
        else:
            payload = json.dumps(result_message).encode()
        await self.publish_message(payload, routing_key=rk)

    async def publish_to_retry_single(self, body: bytes, headers: Optional[Dict[str, Any]] = None):
        """Publish to single retry queue; ensures it if declare allowed."""
        # ensure only once
        if not self._retry_queue_declared:
            # don't raise when declare is disabled — caller can decide fallback
            if self._declare_queues:
                await self.ensure_single_retry_queue()
            else:
                # if declare disabled but queue might exist (infra-created), still attempt to publish
                logger.debug("Publisher: declare disabled, assuming retry queue exists: %s", self._retry_queue_name)

        await self.publish_message(body, routing_key=self._retry_queue_name, headers=headers)
        self.metrics["republishes"] += 1

    async def requeue_to_tail(self, msg_body: bytes, headers: Optional[Dict[str, Any]] = None):
        """
        Compatibility wrapper: publish original body back to default queue.
        NOTE: caller should ack original message AFTER invoking this method.
        """
        rk = os.getenv("QUEUE_NAME", "tasks")
        await self.publish_message(body=msg_body, routing_key=rk, headers=headers)
        self.metrics["republishes"] += 1

    # -------------------------
    # lifecycle
    # -------------------------
    async def close(self) -> None:
        try:
            if self._channel and not getattr(self._channel, "is_closed", False):
                await self._channel.close()
        except Exception:
            logger.exception("Error closing publisher channel")
        finally:
            self._channel = None
