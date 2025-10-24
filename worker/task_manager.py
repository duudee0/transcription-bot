import os
import time
import asyncio
import logging
import httpx
from aio_pika import IncomingMessage
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from common.models import PayloadType, TaskMessage, ResultMessage, Data

logger = logging.getLogger("typed-worker.task-manager")

WORKER_NAME = os.getenv("WORKER_NAME", "generic-worker")


# Выполняется если нету publisher'а
def send_to_result_queue(result_message: ResultMessage):
    """Отправляет результат в очередь результатов (fallback)."""
    id_msg = result_message.data.original_message_id if  result_message.data.original_message_id else result_message.message_id
    logger.info(f"📤 Would send result to queue: {id_msg}")
    if result_message.success:
        logger.info(f"✅ (no publisher) Task {id_msg} completed successfully")
    else:
        logger.error(f"❌ (no publisher) Task {id_msg} failed: {result_message.error_message}")

@dataclass
class AsyncTaskState:
    """Состояние асинхронной задачи"""
    message: IncomingMessage
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
        self.check_interval = 5   # проверка каждые 5 секунд
        self.max_attempts = 3
        self._semaphore = asyncio.Semaphore(int(os.getenv("MAX_CONCURRENT_ASYNC", "5")))

        # Publisher (инжектируется извне, например в main: task_manager.publisher = publisher)
        # Если None — используем send_to_result_queue() как fallback.
        self.publisher: Optional[Any] = None
    
    async def start_monitoring(self):
        """Основной цикл мониторинга"""
        while True:
            try:
                await self._check_active_tasks()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"❌ Monitor loop error: {e}", exc_info=True)
                await asyncio.sleep(10)  # пауза при ошибке
    
    async def _check_active_tasks(self):
        """Проверяет все активные задачи"""
        current_time = time.time()
        completed_tasks = []
        
        for task_id in [*self.active_tasks]:
            task_state = self.active_tasks.get(task_id)
            if not task_state:
                continue
            
            try:
                should_complete = await self._process_single_task(task_id, task_state, current_time)
                if should_complete:
                    completed_tasks.append(task_id)
            except Exception as e:
                logger.error(f"❌ Error monitoring task {task_id}: {e}", exc_info=True)
                should_complete = await self._handle_task_monitoring_error(task_state)
                if should_complete:
                    completed_tasks.append(task_id)
        
        self._cleanup_completed_tasks(completed_tasks)
    
    async def _process_single_task(self, task_id: str, task_state: AsyncTaskState, current_time: float) -> bool:
        """Обрабатывает одну задачу и возвращает нужно ли её завершить"""
        # Пропускаем недавно созданные задачи
        if current_time - task_state.start_time < 10:
            return False
            
        # Проверяем таймаут
        if current_time - task_state.start_time > self.max_wait_time:
            logger.warning(f"⏰ Task {task_id} timeout")
            await self._handle_task_timeout(task_id)
            return True
        
        # Проверяем статус сервиса
        service_check_result = await self._check_service_health(task_id, task_state)
        if service_check_result is not None:  # Сервис недоступен и задача завершена
            return service_check_result
        
        # Если вебхук не пришел, проверяем статус задачи
        if not task_state.callback_received:
            return await self._check_task_progress(task_id, task_state, current_time)
        else:
            # вебхук пришёл — задача уже обработана в handle_webhook/_handle_task_completed
            return True
    
    async def _check_service_health(self, task_id: str, task_state: AsyncTaskState) -> Optional[bool]:
        """Проверяет здоровье сервиса и возвращает нужно ли завершить задачу"""
        if await self._is_service_alive(task_state.service_config):
            return None  # Сервис жив, продолжаем обработку
        
        task_state.attempts += 1
        logger.warning(f"🚨 Service {task_state.service_config['service_name']} down for task {task_id} (attempts={task_state.attempts})")
        
        if task_state.attempts >= self.max_attempts:
            await self._handle_service_down(task_id)
            return True
        
        return False
    
    async def _check_task_progress(self, task_id: str, task_state: AsyncTaskState, current_time: float) -> bool:
        """Проверяет прогресс задачи и возвращает нужно ли её завершить"""
        task_state.last_check = current_time
        await self._check_task_status(task_id, task_state)
        return False  # Задача продолжает обрабатываться
    
    def _handle_task_monitoring_error(self, task_state: AsyncTaskState) -> bool:
        """Обрабатывает ошибки мониторинга задачи и возвращает нужно ли её завершить"""
        task_state.attempts += 1
        return task_state.attempts >= self.max_attempts
    
    def _cleanup_completed_tasks(self, completed_tasks: List[str]):
        """Очищает завершенные задачи из активного списка"""
        for task_id in completed_tasks:
            if task_id in self.active_tasks:
                self.active_tasks.pop(task_id, None)
    
    async def _is_service_alive(self, service_config: Dict) -> bool:
        """Проверяет жив ли сервис"""
        try:
            health_url = f"{service_config['base_url']}/health"
            logger.debug(f"❔ Check health: url - {health_url}")
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(health_url)
                return response.status_code == 200
        except Exception:
            return False
    
    async def _check_task_status(self, task_id: str, task_state: AsyncTaskState):
        """Проверяет статус задачи в сервисе"""
        try:
            status_url = f"{task_state.service_config['base_url']}/status"
            logger.debug(f"❓ Check status task: url - {status_url}")
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(status_url)
                if response.status_code == 200:
                    status_data = response.json()
                    # Если сервис свободен, наша задача должна быть завершена
                    if not status_data.get("is_busy", False):
                        logger.info(f"✅ Service free, checking task {task_id} completion")
                        await self._verify_task_completion(task_id, task_state)
        except Exception as e:
            logger.warning(f"⚠️ Status check failed for {task_id}: {e}", exc_info=True)
    
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
                        await self._handle_task_completed(task_id, history_data)
                    elif history_data.get("status") == "failed":
                        logger.error(f"❌ Task {task_id} failed (via history)")
                        await self._handle_task_failed(task_id, history_data)
        except Exception as e:
            logger.warning(f"⚠️ History check failed for {task_id}: {e}", exc_info=True)
    
    # ----------------------------
    # Обработчики результатов (унифицированный поток публикации)
    # ----------------------------
    async def _handle_task_completed(self, task_id: str, result_data: ResultMessage):
        """Обработка завершенной задачи"""
        task_state = self.active_tasks.get(task_id)
        if not task_state:
            logger.warning("Tried to complete unknown task %s", task_id)
            return

        result_message = ResultMessage(
            source_service=WORKER_NAME,
            target_service=task_state.task.source_service,
            original_message_id=task_state.task.message_id,
            success=True,
            data=Data(
                payload_type = PayloadType.TEXT,
                payload=result_data.data.payload,
                execution_metadata={
                    "worker": WORKER_NAME,
                    "service": task_state.service_config["service_name"],
                    "processed_via": "async_polling"
                }
            )
        )

        try:
            # ПОДТВЕРЖДАЕМ СООБЩЕНИЕ ТОЛЬКО ЗДЕСЬ
            await task_state.message.ack()
            logger.info(f"✅ Message acknowledged for completed task {task_id}")
        except Exception as e:
            logger.exception(f"⛔ Failed to ack service-down result for task {task_id}: {e}")

        try:
            if self.publisher:
                await self.publisher.publish_result(result_message)
            else:
                send_to_result_queue(result_message)
        except Exception as e:
            logger.exception(f"Failed to publish completed result for task {task_id}: {e}")
        finally:
            self._finalize_task(task_id)
    
    async def _handle_task_failed(self, task_id: str, error: str):
        """Обработка неудачной задачи"""
        task_state = self.active_tasks.get(task_id)
        if not task_state:
            logger.warning("Tried to fail unknown task %s", task_id)
            return

        result_message = ResultMessage(
            source_service=WORKER_NAME,
            target_service=task_state.task.source_service,
            original_message_id=task_state.task.message_id,
            success=False,
            error_message=error,
        )

        try:
            # ПОДТВЕРЖДАЕМ СООБЩЕНИЕ ТОЛЬКО ЗДЕСЬ
            await task_state.message.nack(requeue=False) #! НЕ Вернуть в очередь
            logger.info(f"✅ Message acknowledged for completed task {task_id}")
        except Exception as e:
            logger.exception(f"⛔ Failed to ack service-down result for task {task_id}: {e}")

        try:
            if self.publisher:
                await self.publisher.publish_result(result_message)
            else:
                send_to_result_queue(result_message)
        except Exception as e:
            logger.exception(f"Failed to publish failed result for task {task_id}: {e}",)
        finally:
            self._finalize_task(task_id)
    
    async def _handle_task_timeout(self, task_id: str):
        """Обработка таймаута задачи"""
        task_state = self.active_tasks.get(task_id)
        if not task_state:
            logger.warning(f"Tried to timeout unknown task {task_id}")
            return

        logger.error(f"⏰ Task {task_id} timeout after {self.max_wait_time}s")
        result_message = ResultMessage(
            source_service=WORKER_NAME,
            target_service=task_state.task.source_service,
            original_message_id=task_state.task.message_id,
            success=False,
            error_message=f"Task timeout after {self.max_wait_time}s",
        )

        try:
            # ПОДТВЕРЖДАЕМ СООБЩЕНИЕ ТОЛЬКО ЗДЕСЬ
            await task_state.message.nack(requeue=True) # Вернуть в очередь
            logger.info(f"✅ Message acknowledged for completed task {task_id}")
        except Exception as e:
            logger.exception(f"⛔ Failed to ack service-down result for task {task_id}: {e}")

        try:
            if self.publisher:
                await self.publisher.publish_result(result_message)
            else:
                send_to_result_queue(result_message)
        except Exception as e:
            logger.exception(f"Failed to publish timeout result for task {task_id}: {e}")
        finally:
            self._finalize_task(task_id)
    
    async def _handle_service_down(self, task_id: str):
        """Обработка недоступности сервиса"""
        task_state = self.active_tasks.get(task_id)
        if not task_state:
            logger.warning(f"Tried to handle service down for unknown task {task_id}")
            return

        logger.error(f"🚨 Service down for task {task_id}")
        result_message = ResultMessage(
            source_service=WORKER_NAME,
            target_service=task_state.task.source_service,
            original_message_id=task_state.task.message_id,
            success=False,
            error_message=f"Service {task_state.service_config['service_name']} unavailable",
        )

        try:
            # ПОДТВЕРЖДАЕМ СООБЩЕНИЕ ТОЛЬКО ЗДЕСЬ
            await task_state.message.nack(requeue=True) # Вернуть в очередь
            logger.info(f"✅ Message acknowledged for completed task {task_id}")
        except Exception as e:
            logger.exception(f"⛔ Failed to ack service-down result for task {task_id}: {e}")
        try:
            if self.publisher:
                await self.publisher.publish_result(result_message)
            else:
                send_to_result_queue(result_message)
        except Exception as e:
            logger.exception(f"Failed to publish service-down result for task {task_id}: {e}")
        finally:
            self._finalize_task(task_id)

    # ----------------------------
    # финализация: удалить state и release семафор
    # ----------------------------
    def _finalize_task(self, task_id: str):
        """Снять задачу и освободить слот семафора — вернуть state или None."""
        task_state = self.active_tasks.pop(task_id, None)
        if task_state:
            try:
                self._semaphore.release()
            except ValueError:
                logger.warning(f"Semaphore release issue for {task_id}")
        return task_state

    async def register_async_task(self, task: TaskMessage, service_config: Dict, message: IncomingMessage):
        """Register async task but obey semaphore limits (await this)."""
        await self._semaphore.acquire()
        task_id = str(task.message_id)
        self.active_tasks[task_id] = AsyncTaskState(
            message=message,
            task=task,
            service_config=service_config,
            start_time=time.time(),
            last_check=time.time(),
            status="waiting"
        )
        logger.info(f"📝 Registered async task: {task_id} (active={len(self.active_tasks)})")

    # ----------------------------
    # webhook handler (robust parsing)
    # ----------------------------
    async def handle_webhook(self, message_id: str, payload: ResultMessage) -> bool:
        """
        Обрабатывает вебхук уведомление.
        Поддерживает несколько форматов:
          - payload может содержать "original_message_id"
          - payload может иметь вложенную структуру
          - payload может иметь поля success/status/result на верхнем уровне
        """
        # try to locate task_state by provided message_id (URL param)
        task_state = self.active_tasks.get(message_id)

        # If not found, look into payload for common identifiers
        if not task_state:
            # try original_message_id in payload
            orig_id = None
            if isinstance(payload, dict):
                orig_id = payload.get("message_id") or payload.get("data", {}).get("original_message_id")
            if orig_id:
                task_state = self.active_tasks.get(str(orig_id))

        if not task_state:
            logger.warning(f"🤔 Webhook for unknown task: {message_id}")
            return False

        # mark callback received
        task_id = str(task_state.task.message_id)
        task_state.callback_received = True
        task_state.last_check = time.time()

        # interpret status
        if payload.success == True:
            logger.info(f"✅ Webhook: task {task_id} completed")
            await self._handle_task_completed(task_id, payload)
            return True
        else:
            logger.error(f"❌ Webhook: task {task_id} failed")
            await self._handle_task_failed(task_id, payload.error_message or "Unknown error")
            return True
