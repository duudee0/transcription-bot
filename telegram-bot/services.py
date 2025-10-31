import time
from aiogram import Bot
import httpx
import asyncio
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import asdict

from config import config
from models import UserTask, TaskStatus


class WrapperService:
    """Сервис для работы с Wrapper API."""
    
    def __init__(self):
        self.base_url = config.WRAPPER_URL
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def create_task(
        self,
        task_type: str,
        input_data: Dict[str, Any],
        service_chain: List[str],
        parameters: Optional[Dict[str, Any]] = None,
        callback_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """Создает задачу в wrapper."""
        
        payload = {
            "task_type": task_type,
            "input_data": input_data,
            "service_chain": service_chain,
            "parameters": parameters or {},
            "timeout": config.DEFAULT_TIMEOUT
        }
        
        if callback_url:
            payload["callback_url"] = callback_url
            
        response = await self.client.post(
            f"{self.base_url}/api/v1/tasks",
            json=payload
        )
        response.raise_for_status()
        
        return response.json()
    
    async def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Получает статус задачи."""
        response = await self.client.get(
            f"{self.base_url}/api/v1/tasks/{task_id}"
        )
        response.raise_for_status()
        return response.json()


class TaskManager:
    """Менеджер задач пользователей."""
    
    def __init__(self, bot: Bot=None):
        self.wrapper = WrapperService()
        self.bot: Bot = bot  # Сохраняем бота
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
        
        # Создаем задачу в wrapper
        callback_url = f"{config.callback_url}/{user_id}"
        
        try:
            response = await self.wrapper.create_task(
                task_type=task_type,
                input_data=input_data,
                service_chain=service_chain,
                parameters=parameters,
                callback_url=callback_url
            )
        except Exception as e:
            print(f"Ошибка создания задачи: {e}")
            raise
        
        # Сохраняем задачу пользователя
        task_id = response["task_id"]
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
        
        # Запускаем отслеживание
        self._start_polling(task_id, chat_id)
        
        return user_task
    
    def _start_polling(self, task_id: str, chat_id: int):
        """Запускает отслеживание статуса задачи."""
        task = asyncio.create_task(
            self._poll_task_status(task_id, chat_id)
        )
        self.polling_tasks[task_id] = task
    
    async def _poll_task_status(self, task_id: str, chat_id: int):
        """Поллинг статуса задачи"""
        start_time = time.time()
        timeout = config.DEFAULT_TIMEOUT
        
        while time.time() - start_time < timeout:
            try:
                # Проверяем, не обновилась ли задача через webhook
                if task_id in self.user_tasks:
                    task = self.user_tasks[task_id]
                    if task.status in [TaskStatus.COMPLETED, TaskStatus.ERROR]:
                        print(f"✅ Task {task_id} completed via webhook")
                        return
                
                # Запрашиваем статус у wrapper
                status_data = await self.wrapper.get_task_status(task_id)
                status = status_data.get("status")
                
                print(f"🔍 Polling {task_id}: {status}")
                
                if status in ["completed", "error", "timeout"]:
                    # Обновляем задачу
                    if task_id in self.user_tasks:
                        self.user_tasks[task_id].status = TaskStatus(status)
                        self.user_tasks[task_id].result = status_data.get("result")
                        self.user_tasks[task_id].error = status_data.get("error")
                        self.user_tasks[task_id].updated_at = datetime.now()
                    
                    # Отправляем результат пользователю
                    await self._send_task_result(chat_id, task_id, status_data)
                    return
                    
            except Exception as e:
                print(f"⚠️ Polling error for {task_id}: {e}")
            
            await asyncio.sleep(2)
        
        # Таймаут
        if task_id in self.user_tasks:
            self.user_tasks[task_id].status = TaskStatus.TIMEOUT
            await self.bot.send_message(chat_id, f"⏰ Задача {task_id} превысила время выполнения")
    
    async def _send_task_result(self, chat_id: int, task_id: str, status_data: Dict[str, Any]):
        """Отправляет результат пользователю"""
        from utils import format_task_result
        
        try:
            message = format_task_result(task_id, status_data)
            await self.bot.send_message(chat_id, message)
            print(f"📨 Sent result to user for task {task_id}")
        except Exception as e:
            print(f"❌ Error sending result: {e}")
            
    
    async def _send_task_result(self, chat_id: int, task_id: str, status_data: Dict[str, Any]):
        """Отправляет результат задачи пользователю."""
        from main import bot
        from utils import format_task_result
        
        try:
            message = format_task_result(task_id, status_data)
            await bot.send_message(chat_id, message)
        except Exception as e:
            print(f"Error sending result to user: {e}")
    
    def get_user_tasks(self, user_id: int) -> List[UserTask]:
        """Возвращает задачи пользователя."""
        task_ids = self.user_task_map.get(user_id, [])
        return [self.user_tasks[task_id] for task_id in task_ids if task_id in self.user_tasks]
    
    def get_task(self, task_id: str) -> Optional[UserTask]:
        """Возвращает задачу по ID."""
        return self.user_tasks.get(task_id)


# Глобальные экземпляры будут созданы в main.py
wrapper_service = None
task_manager = TaskManager()