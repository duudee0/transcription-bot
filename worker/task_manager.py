import os
import time
import asyncio
import logging
import httpx

from typing import Dict, Any
from dataclasses import dataclass

from common.models import TaskMessage, ResultMessage, ResultData


logger = logging.getLogger("typed-worker.task-manager")

WORKER_NAME = os.getenv("WORKER_NAME", "generic-worker")


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
        self._semaphore = asyncio.Semaphore(int(os.getenv("MAX_CONCURRENT_ASYNC", "5")))

        
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
            
            await send_to_result_queue(result_message)
            # Удаляем из активных задач
            # финализируем и освобождаем семафор
            await self._finalize_task(task_id)
    
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
            # финализируем и освобождаем семафор
            await self._finalize_task(task_id)
    
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
            # финализируем и освобождаем семафор
            await self._finalize_task(task_id)
    
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
            await self._finalize_task(task_id)

    
    async def _finalize_task(self, task_id: str):
        """Снять задачу и освободить слот семафора — вернуть state или None."""
        task_state = self.active_tasks.pop(task_id, None)
        if task_state:
            try:
                self._semaphore.release()
            except ValueError:
                logger.warning(f"Semaphore release issue for {task_id}")
        return task_state

    async def register_async_task(self, task: TaskMessage, service_config: Dict):
        """Register async task but obey semaphore limits (await this)."""
        await self._semaphore.acquire()
        task_id = str(task.message_id)
        self.active_tasks[task_id] = AsyncTaskState(
            task=task,
            service_config=service_config,
            start_time=time.time(),
            last_check=time.time(),
            status="waiting"
        )
        logger.info(f"📝 Registered async task: {task_id} (active={len(self.active_tasks)})")

    
    async def handle_webhook(self, message_id: str, payload: dict) -> bool:
        """Обрабатывает вебхук уведомление"""
        task_state = self.active_tasks.get(message_id)
        if not task_state:
            logger.warning(f"🤔 Webhook for unknown task: {message_id}")
            return False

        task_state.callback_received = True
        task_state.last_check = time.time()

        # Примем несколько форматов: {"status":"completed"} или {"success":true}
        success_flag = payload.get("data").get("success")

        if success_flag is True:
            logger.info(f"✅ Webhook: task {message_id} completed")
            await self._handle_task_completed(message_id, payload.get("result", {}))
            return True
        elif success_flag is False or payload.get("error"):
            logger.error(f"❌ Webhook: task {message_id} failed")
            await self._handle_task_failed(message_id, payload.get("error_message", "Unknown error"))
            return True

        # Если webhook пришёл, но не определён статус — просто отмечаем callback_received (монитор может дальше проверять)
        logger.info(f"ℹ️ Webhook for {message_id} received but status unknown; will be polled")
        return True
