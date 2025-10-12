# models/common/base_service.py
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Any, Dict
import uvicorn
import time
import sys
import asyncio
import httpx
from uuid import UUID

# Импортируем общие модели
from common.models import TaskMessage, ResultMessage, ResultData, MessageType


class BaseService:
    """
    БАЗОВЫЙ КЛАСС ДЛЯ ВСЕХ СЕРВИСОВ
    
    Содержит общую функциональность:
    - FastAPI приложение
    - Стандартные эндпоинты (health, status, requests)
    - Управление состоянием обработки
    - Логику вебхуков
    - Историю запросов
    """
    
    def __init__(self, service_name: str, version: str = "1.0"):
        """
        Инициализация базового сервиса
        
        Args:
            service_name: Название сервиса (например, "llm-service", "gigachat-service")
            version: Версия сервиса
        """
        self.service_name = service_name
        self.app = FastAPI(title=service_name, version=version)
        
        # Общее состояние
        self.processing_history = {}
        self.is_processing = False
        self.current_task_id = None
        self.processing_start_time = None
        
        # Регистрируем стандартные эндпоинты
        self._register_common_endpoints()
    
    def _register_common_endpoints(self):
        """Регистрирует общие эндпоинты для всех сервисов"""
        
        @self.app.get("/health")
        async def health():
            return await self._health_handler()
        
        @self.app.get("/status")
        async def status():
            return await self._status_handler()
        
        @self.app.get("/requests")
        async def list_requests():
            return await self._list_requests_handler()
        
        @self.app.get("/requests/{request_id}")
        async def get_request(request_id: str):
            return await self._get_request_handler(request_id)
        
        @self.app.post("/api/v1/process")
        async def process_task_endpoint(request: Request, background_tasks: BackgroundTasks):
            return await self._process_task_handler(request, background_tasks)
    
    async def _health_handler(self) -> Dict[str, Any]:
        """Обработчик health check"""
        return {
            "status": "ok", 
            "service": self.service_name,
            "timestamp": time.time()
        }
    
    async def _status_handler(self) -> Dict[str, Any]:
        """Обработчик статуса сервиса"""
        status_info = {
            "is_busy": self.is_processing,
            "timestamp": time.time(),
            "service": self.service_name
        }
        
        if self.is_processing:
            status_info.update({
                "current_task_id": str(self.current_task_id),
                "processing_since": self.processing_start_time,
                "processing_time_seconds": time.time() - self.processing_start_time if self.processing_start_time else 0
            })
        
        return status_info
    
    async def _list_requests_handler(self) -> Dict[str, Any]:
        """Обработчик списка запросов"""
        return {
            "total_requests": len(self.processing_history),
            "requests": self.processing_history
        }
    
    async def _get_request_handler(self, request_id: str) -> Dict[str, Any]:
        """Обработчик получения конкретного запроса"""
        if request_id in self.processing_history:
            return self.processing_history[request_id]
        raise HTTPException(status_code=404, detail="Request not found")
    
    async def _process_task_handler(self, request: Request, background_tasks: BackgroundTasks) -> ResultMessage:
        """
        Базовый обработчик задач
        
        Дочерние классы должны переопределить:
        - _validate_task
        - _process_task_logic
        """
        start_time = time.time()
        
        try:
            # Парсим входящий JSON и валидируем как TaskMessage
            body = await request.json()
            task_message = TaskMessage.model_validate(body)
            
            # Сохраняем в историю
            self.processing_history[str(task_message.message_id)] = {
                "received_at": time.time(),
                "source_service": task_message.source_service,
                "task_type": task_message.data.task_type,
                "input_data": task_message.data.input_data,
                "status": "processing"
            }
            
            # Логируем получение
            print(f"[{time.time()}] {self.service_name} received task: {task_message.message_id}", file=sys.stderr)
            print(f"  From: {task_message.source_service}", file=sys.stderr)
            print(f"  Task: {task_message.data.task_type}", file=sys.stderr)
            
            # Валидация задачи (должна быть реализована в дочернем классе)
            await self._validate_task(task_message)
            
            # Проверяем поддержку вебхука
            callback_url = task_message.data.input_data.get("callback_url")
            webhook_supported = task_message.data.input_data.get("webhook_supported", False)
            
            if callback_url and webhook_supported and not self.is_processing:
                print(f"🔔 Webhook mode activated for {task_message.message_id}", file=sys.stderr)
                
                # Запускаем фоновую задачу
                background_tasks.add_task(
                    self._process_with_webhook,
                    task_message,
                    callback_url
                )
                
                # Немедленный ответ
                return ResultMessage(
                    message_id=task_message.message_id,
                    message_type=MessageType.RESULT,
                    source_service=self.service_name,
                    target_service=task_message.source_service,
                    original_message_id=task_message.message_id,
                    data=ResultData(
                        success=True,
                        result={"status": "accepted", "message": "Processing in background via webhook"},
                        execution_metadata={
                            "processing_mode": "async_webhook",
                            "service": self.service_name
                        }
                    )
                )
            
            # Синхронная обработка
            result_data = await self._process_task_sync(task_message)
            
            # Создаем ResultMessage
            result_message = ResultMessage(
                message_id=task_message.message_id,
                message_type=MessageType.RESULT,
                source_service=self.service_name,
                target_service=task_message.source_service,
                original_message_id=task_message.message_id,
                data=result_data
            )
            
            # Обновляем историю
            self.processing_history[str(task_message.message_id)]["completed_at"] = time.time()
            self.processing_history[str(task_message.message_id)]["status"] = "completed"
            self.processing_history[str(task_message.message_id)]["result"] = result_data.model_dump()
            
            processing_time = (time.time() - start_time) * 1000
            print(f"✅ {self.service_name} processed in {processing_time:.2f}ms", file=sys.stderr)
            
            return result_message
            
        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            print(f"❌ {self.service_name} error: {e}", file=sys.stderr)
            
            error_result = ResultMessage(
                message_type=MessageType.RESULT,
                source_service=self.service_name,
                original_message_id=getattr(task_message, 'message_id', None),
                data=ResultData(
                    success=False,
                    error_message=str(e),
                    execution_metadata={
                        "processing_time_ms": processing_time,
                        "error": True,
                        "service": self.service_name
                    }
                )
            )
            return error_result
    
    async def _validate_task(self, task_message: TaskMessage):
        """
        Валидация задачи
        
        Дочерние классы должны переопределить этот метод
        для проверки поддерживаемых типов задач и конфигурации
        """
        pass
    
    async def _process_task_logic(self, task_message: TaskMessage) -> ResultData:
        """
        Основная логика обработки задачи
        
        Должна быть переопределена в дочерних классах
        """
        raise NotImplementedError("Дочерний класс должен реализовать этот метод")
    
    async def _process_task_sync(self, task_message: TaskMessage) -> ResultData:
        """Синхронная обработка задачи"""
        if self.is_processing:
            raise HTTPException(
                status_code=423,
                detail=f"Service is busy processing task {self.current_task_id}"
            )
        
        self.is_processing = True
        self.current_task_id = task_message.message_id
        self.processing_start_time = time.time()

        try:
            result_data = await self._process_task_logic(task_message)
            return result_data
        
        finally:
            self.is_processing = False
            self.current_task_id = None
            self.processing_start_time = None
    
    async def _process_with_webhook(self, task_message: TaskMessage, callback_url: str):
        """Фоновая обработка с вебхуком"""
        print(f"🔄 {self.service_name} starting background processing: {task_message.message_id}", file=sys.stderr)
        
        self.is_processing = True
        self.current_task_id = task_message.message_id
        self.processing_start_time = time.time()
        
        try:
            result_data = await self._process_task_logic(task_message)
            
            # Обновляем историю
            self.processing_history[str(task_message.message_id)]["completed_at"] = time.time()
            self.processing_history[str(task_message.message_id)]["status"] = "completed"
            self.processing_history[str(task_message.message_id)]["result"] = result_data.model_dump()
            
            # Создаем ResultMessage для вебхука
            result_message = ResultMessage(
                message_id=task_message.message_id,
                message_type=MessageType.RESULT,
                source_service=self.service_name,
                target_service=task_message.source_service,
                original_message_id=task_message.message_id,
                data=result_data
            )
            
            # Отправляем вебхук
            await self._send_webhook(callback_url, result_message)
            
            processing_time = (time.time() - self.processing_start_time) * 1000
            print(f"✅ {self.service_name} background task completed in {processing_time:.2f}ms", file=sys.stderr)
            
        except Exception as e:
            print(f"❌ {self.service_name} background processing failed: {e}", file=sys.stderr)
            
            error_result = ResultMessage(
                message_type=MessageType.RESULT,
                source_service=self.service_name,
                original_message_id=task_message.message_id,
                data=ResultData(
                    success=False,
                    error_message=str(e),
                    execution_metadata={"error": True, "service": self.service_name}
                )
            )
            await self._send_webhook(callback_url, error_result)
        
        finally:
            self.is_processing = False
            self.current_task_id = None
            self.processing_start_time = None
    
    async def _send_webhook(self, callback_url: str, result_message: ResultMessage):
        """Отправляет вебхук"""
        try:
            print(f"📤 {self.service_name} sending webhook to: {callback_url}", file=sys.stderr)
            webhook_data = result_message.model_dump(mode='json')
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    callback_url,
                    json=webhook_data,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    print(f"✅ {self.service_name} webhook delivered", file=sys.stderr)
                    return True
                else:
                    print(f"⚠️ {self.service_name} webhook failed: {response.status_code}", file=sys.stderr)
                    return False
                    
        except Exception as e:
            print(f"❌ {self.service_name} webhook sending failed: {e}", file=sys.stderr)
            return False
    
    def run(self, host: str = "0.0.0.0", port: int = 8000):
        """Запускает сервис"""
        uvicorn.run(self.app, host=host, port=port)