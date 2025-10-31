"""Сервисы приложения."""
import time
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any

import httpx
from aiogram import Bot

from config import config
from models import UserTask, TaskStatus
from services.wrapper import WrapperService

class TaskManager:
    """Менеджер задач пользователей."""
    
    def __init__(self, bot: Bot):
        self.wrapper = WrapperService()
        self.bot = bot
        self.user_tasks: Dict[str, UserTask] = {}
        self.user_task_map: Dict[int, List[str]] = {}
        self.polling_tasks: Dict[str, asyncio.Task] = {}
    
    async def create_task(
        self,
        user_id: int,
        chat_id: int,
        task_type: str,
        input_data: Dict[str, Any],
        service_chain: List[str],
        parameters: Optional[Dict[str, Any]] = None
    ) -> UserTask:
        """Создает новую задачу."""
        callback_url = f"{config.callback_url}/{user_id}"
        
        try:
            response = await self.wrapper.create_task(
                task_type=task_type,
                input_data=input_data,
                service_chain=service_chain,
                parameters=parameters,
                callback_url=callback_url
            )
        except Exception as error:
            print(f"Ошибка создания задачи: {error}")
            raise
        
        return self._create_user_task(
            response["task_id"], user_id, chat_id, task_type,
            service_chain, input_data, parameters
        )
    
    def _create_user_task(
        self,
        task_id: str,
        user_id: int,
        chat_id: int,
        task_type: str,
        service_chain: List[str],
        input_data: Dict[str, Any],
        parameters: Optional[Dict[str, Any]]
    ) -> UserTask:
        """Создает объект пользовательской задачи."""
        user_task = UserTask(
            task_id=task_id,
            user_id=user_id,
            chat_id=chat_id,
            task_type=task_type,
            status=TaskStatus.PENDING,
            service_chain=service_chain,
            input_data=input_data,
            parameters=parameters or {},
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.user_tasks[task_id] = user_task
        self.user_task_map.setdefault(user_id, []).append(task_id)
        
        self._start_polling(task_id, chat_id)
        
        return user_task
    
    def _start_polling(self, task_id: str, chat_id: int):
        """Запускает отслеживание статуса задачи."""
        task = asyncio.create_task(self._poll_task_status(task_id, chat_id))
        self.polling_tasks[task_id] = task
    
    async def _poll_task_status(self, task_id: str, chat_id: int):
        """Поллинг статуса задачи."""
        start_time = time.time()
        timeout = config.DEFAULT_TIMEOUT
        
        while time.time() - start_time < timeout:
            if self._check_webhook_completion(task_id):
                return
            
            if await self._check_wrapper_status(task_id, chat_id):
                return
            
            await asyncio.sleep(2)
        
        await self._handle_timeout(task_id, chat_id)
    
    def _check_webhook_completion(self, task_id: str) -> bool:
        """Проверяет завершение задачи через webhook."""
        if task_id not in self.user_tasks:
            return False
            
        task = self.user_tasks[task_id]
        is_completed = task.status in [TaskStatus.COMPLETED, TaskStatus.ERROR]
        
        if is_completed:
            print(f"✅ Task {task_id} completed via webhook")
        
        return is_completed
    
    async def _check_wrapper_status(self, task_id: str, chat_id: int) -> bool:
        """Проверяет статус задачи в wrapper."""
        try:
            status_data = await self.wrapper.get_task_status(task_id)
            status = status_data.get("status")
            
            print(f"🔍 Polling {task_id}: {status}")
            
            if status in ["completed", "error", "timeout"]:
                self._update_task_from_status(task_id, status_data)
                await self._send_task_result(chat_id, task_id, status_data)
                return True
                
        except Exception as error:
            print(f"⚠️ Polling error for {task_id}: {error}")
        
        return False
    
    def _update_task_from_status(self, task_id: str, status_data: Dict[str, Any]):
        """Обновляет задачу из данных статуса."""
        if task_id not in self.user_tasks:
            return
            
        task = self.user_tasks[task_id]
        # TODO: ПОФИКСИТЬ ТИПЫ НОРМАЛЬНЫЕ
        task.status = TaskStatus(status_data.get("status"))
        task.result = status_data.get("result")
        task.error = status_data.get("error")
        task.updated_at = datetime.now()
    
    async def _handle_timeout(self, task_id: str, chat_id: int):
        """Обрабатывает таймаут задачи."""
        if task_id in self.user_tasks:
            self.user_tasks[task_id].status = TaskStatus.TIMEOUT
            await self.bot.send_message(
                chat_id, 
                f"⏰ Задача {task_id} превысила время выполнения"
            )
    
    async def _send_task_result(self, chat_id: int, task_id: str, status_data: Dict[str, Any]):
        """Отправляет результат пользователю через FileSender."""
        try:
            from file_sender import FileSender  # Локальный импорт

            status = status_data.get("status", "unknown")
            result = status_data.get("result")
            error = status_data.get("error")
            
            file_sender = FileSender(self.bot)  # Создаем инстанс когда нужно

            await file_sender.send_task_result(
                chat_id=chat_id,
                task_id=task_id,
                status=status,
                result=result,
                error=error
            )
            print(f"📨 Sent result to user for task {task_id}")
        except Exception as error:
            print(f"❌ Error sending result: {error}")
    
    def get_user_tasks(self, user_id: int) -> List[UserTask]:
        """Возвращает задачи пользователя."""
        task_ids = self.user_task_map.get(user_id, [])
        return [
            self.user_tasks[task_id] 
            for task_id in task_ids 
            if task_id in self.user_tasks
        ]
    
    def get_task(self, task_id: str) -> Optional[UserTask]:
        """Возвращает задачу по ID."""
        return self.user_tasks.get(task_id)
    
    async def close(self):
        """Закрывает ресурсы."""
        await self.wrapper.close()