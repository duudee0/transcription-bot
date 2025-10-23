# models/common/base_service.py
from datetime import datetime
from enum import Enum
import json
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Any, Dict, List
from aio_pika import connect_robust
import uvicorn
import time
import os
import sys
import asyncio
import httpx
from uuid import UUID, uuid4


from common.publisher import Publisher
# Импортируем общие модели
from common.models import PayloadType, TaskMessage, ResultMessage, MessageType, Data


class BaseService:
    """
    БАЗОВЫЙ КЛАСС ДЛЯ ВСЕХ СЕРВИСОВ
    Адаптирован для работы с target_services (цепочкой сервисов)
    """
    
    def __init__(self, service_name: str, version: str = "1.0"):
        self.service_name = service_name
        self.app = FastAPI(title=service_name, version=version)
        
        # Общее состояние
        self.processing_history: Dict[str, Any] = {}
        self.is_processing: bool = False
        self.current_task_id: Optional[UUID] = None
        self.processing_start_time: Optional[float] = None
        
        # Регистрируем стандартные эндпоинты
        self._register_common_endpoints()

        # Отправка в rabbitmq
        self.publisher: Optional[Publisher] = None
    
    def _register_common_endpoints(self):
        """Регистрирует общие эндпоинты для всех сервисов"""
        
        @self.app.get("/health")
        def health():
            return self._health_handler()
        
        @self.app.get("/status")
        def status():
            return self._status_handler()
        
        @self.app.get("/requests")
        def list_requests():
            return self._list_requests_handler()
        
        @self.app.get("/requests/{request_id}")
        def get_request(request_id: str):
            return self._get_request_handler(request_id)
        
        @self.app.post("/api/v1/process")
        async def process_task_endpoint(request: Request, background_tasks: BackgroundTasks):
            return await self._process_task_handler(request, background_tasks)
    
    async def ensure_publisher(self) -> Publisher:
        """Обеспечивает наличие publisher (создает при необходимости)"""
        if self.publisher is None or not self._publisher_initialized:
            print(f"🔄 Creating RabbitMQ connection for {self.service_name}", file=sys.stderr)
            
            try:
                rabbit_url = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
                print(f"   Connecting to RabbitMQ: {rabbit_url}", file=sys.stderr)
                
                connection = await connect_robust(rabbit_url)
                print(f"   ✅ RabbitMQ connected for {self.service_name}", file=sys.stderr)
                
                self.publisher = Publisher(
                    connection, 
                    prefetch=5, 
                    declare_queues=True
                )
                self._publisher_initialized = True
                
                print(f"   ✅ Publisher created for {self.service_name}", file=sys.stderr)
                
            except Exception as e:
                print(f"❌ Failed to create publisher for {self.service_name}: {e}", file=sys.stderr)
                raise
        
        return self.publisher

    def run(self, host: str = "0.0.0.0", port: int = 8000):
        """Синхронный запуск сервиса (совместимость с вашим текущим кодом)"""

        # Запускаем асинхронную инициализацию в синхронном контексте
        asyncio.run(self._run_with_publisher(host, port))
    
    async def _run_with_publisher(self, host: str, port: int):
        """Асинхронная подготовка и запуск"""
        try:
            # Убеждаемся, что publisher создан ДО запуска сервера
            await self.ensure_publisher()
            print(f"✅ {self.service_name} publisher initialized", file=sys.stderr)
            
            # Запускаем uvicorn
            config = uvicorn.Config(self.app, host=host, port=port, log_level="info")
            server = uvicorn.Server(config)
            await server.serve()
            
        except Exception as e:
            print(f"❌ Failed to start {self.service_name}: {e}", file=sys.stderr)
            raise

    def _should_process_message(self, task_message: TaskMessage) -> bool:
        """
        ОПРЕДЕЛЯЕТ, ДОЛЖЕН ЛИ СЕРВИС ОБРАБОТАТЬ СООБЩЕНИЕ
        """
        if not task_message.target_services:
            return False
        return task_message.target_services[0] == self.service_name
        
    def _can_handle_task_type(self, task_type: str) -> bool:
        """
        ОПРЕДЕЛЯЕТ, МОЖЕТ ЛИ СЕРВИС ОБРАБОТАТЬ ТИП ЗАДАЧИ
        
        Дочерние классы должны переопределить этот метод
        """
        raise NotImplementedError("Дочерний класс должен реализовать этот метод")
    
    async def _process_task_handler(self, request: Request, background_tasks: BackgroundTasks) -> ResultMessage:
        """
        ОБРАБОТЧИК ЗАДАЧ С ПОДДЕРЖКОЙ ЦЕПОЧЕК
        """
        
        try:
            # Парсим входящий JSON и валидируем как TaskMessage
            body = await request.json()
            task_message: TaskMessage = TaskMessage.model_validate(body)
            
            # Сохраняем в историю
            self.processing_history[str(task_message.message_id)] = {
                "received_at": time.time(),
                "source_service": task_message.source_service,
                "target_services": task_message.target_services,
                "task_type": task_message.data.task_type if task_message.data else None,
                "payload": task_message.data.payload if task_message.data else None,
                "status": "processing"
            }

            
            # Логируем получение
            print(f"[{time.time()}] {self.service_name} received task: {task_message.message_id}", file=sys.stderr)
            print(f"  From: {task_message.source_service}", file=sys.stderr)
            print(f"  Target chain: {task_message.target_services}", file=sys.stderr)
            print(f"  Task: {task_message.data.task_type}", file=sys.stderr)
            
            # Валидация задачи
            await self._validate_task(task_message)
            
            # Проверяем поддержку вебхука
            callback_url = task_message.data.callback_url
            
            # if callback_url and webhook_supported and not self.is_processing:
            print(f"🔔 Webhook mode activated for {task_message.message_id}", file=sys.stderr)
            
            # Запускаем фоновую задачу
            background_tasks.add_task(
                self._process_with_webhook_and_chain,
                task_message,
                callback_url
            )
            
            # Немедленный ответ
            return ResultMessage(
                message_id=task_message.message_id,
                message_type=MessageType.RESULT,
                source_service=self.service_name,
                target_services=task_message.target_services,
                original_message_id=task_message.message_id,
                success=True,
                data=Data(
                    task_type=task_message.data.task_type,
                    payload_type = PayloadType.TEXT,
                    payload={"status": "accepted", "text": "Processing in background via webhook"},
                    execution_metadata={
                        "processing_mode": "async_webhook",
                        "service": self.service_name,
                        "remaining_chain": self._get_remaining_chain(task_message)
                    }
                )
            )
        except HTTPException:
            # Пробрасываем HTTPException дальше - они вернут правильный статус код
            raise
            
        except Exception as e:
            print(f"❌ {self.service_name} error: {e}", file=sys.stderr)

            raise HTTPException(
                status_code=500,
                detail=f"Internal server error: {str(e)}"
            )       
    
    async def _handle_service_chain(self, task_message: TaskMessage, result_data: Data) -> ResultMessage | TaskMessage:
        """
        ОБРАБАТЫВАЕТ ЦЕПОЧКУ СЕРВИСОВ С ПРАВИЛЬНОЙ ОТПРАВКОЙ ВЕБХУКОВ
        """
        remaining_services = self._get_remaining_chain(task_message)
        
        if remaining_services:
            # Есть следующие сервисы - передаем задачу дальше через RabbitMQ
            next_service = remaining_services[0]
            
            print(f"🔄 ⚙️ Chain: {self.service_name} -> {next_service}", file=sys.stderr)


            # Создаем новую задачу для следующего сервиса
            next_task = TaskMessage(
                message_id=uuid4(),
                message_type=MessageType.TASK,
                source_service=self.service_name,
                target_services=remaining_services,
                data= Data(
                    task_type=task_message.data.task_type,
                    payload_type=result_data.payload_type if result_data.payload_type else task_message.data.payload_type,
                    payload=result_data.payload,
                    wrapper_callback_url=task_message.data.wrapper_callback_url,
                    original_message_id=task_message.data.original_message_id,
                    parameters=task_message.data.parameters,
                    execution_metadata={**task_message.data.execution_metadata, 
                                        **result_data.execution_metadata},
                    callback_url=None
                )
            )
            
            # Отправляем через RabbitMQ
            success = await self._send_task_to_next_service(next_task)
            if not success:
                # Если не удалось отправить, возвращаем ошибку
                return ResultMessage(
                    message_id=task_message.message_id,
                    message_type=MessageType.RESULT,
                    source_service=self.service_name,
                    target_services=[task_message.source_service],
                    original_message_id=task_message.message_id,
                    success=False,
                    error_message=f"Failed to send task to next service: {next_service}",
                    data=result_data
                )
            
            # Возвращаем промежуточный результат
            return ResultMessage(
                message_id=task_message.message_id,
                message_type=MessageType.RESULT,
                source_service=self.service_name,
                target_services=[task_message.source_service],
                original_message_id=task_message.message_id,
                success=True,
                data=Data(
                    task_type=task_message.data.task_type if task_message.data else None,
                    payload={"status": "passed_to_next_service", "next_service": next_service},
                    execution_metadata={"service": self.service_name, "chain_continued": True}
                )
            )
        else:
            # МЫ ПОСЛЕДНИЙ СЕРВИС В ЦЕПОЧКЕ - отправляем финальный результат в wrapper
            final_result = ResultMessage(
                message_id=task_message.message_id,
                message_type=MessageType.RESULT,
                source_service=self.service_name,
                target_services=[],
                original_message_id=task_message.message_id,
                success=True if result_data.payload_type != PayloadType.ERROR else False,
                data=result_data
            )
            
            # Отправляем вебхук в wrapper
            wrapper_callback_url = task_message.data.wrapper_callback_url
            if wrapper_callback_url:
                await self._send_webhook_to_wrapper(wrapper_callback_url, final_result)
                print(f"📤 Final result sent to wrapper: {wrapper_callback_url}", file=sys.stderr)
            else:
                print("⚠️ No wrapper_callback_url for final result", file=sys.stderr)
            
            return final_result
        
    async def _send_webhook_to_wrapper(self, wrapper_url: str, result_message: ResultMessage):
        """Отправляет вебхук в wrapper"""
        try:
            print(f"📤 {self.service_name} sending final result to wrapper: {wrapper_url}", file=sys.stderr)
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    wrapper_url,
                    json=result_message.model_dump(mode='json'),
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    print(f"✅ {self.service_name} wrapper webhook delivered", file=sys.stderr)
                    return True
                else:
                    print(f"⚠️ {self.service_name} wrapper webhook failed: {response.status_code}", file=sys.stderr)
                    return False
                    
        except Exception as e:
            print(f"❌ {self.service_name} wrapper webhook ({wrapper_url}) failed: {e}", file=sys.stderr)
            return False
    
    def _get_remaining_chain(self, task_message: TaskMessage) -> List[str]:
        """
        ВОЗВРАЩАЕТ ОСТАВШУЮСЯ ЧАСТЬ ЦЕПОЧКИ ПОСЛЕ ТЕКУЩЕГО СЕРВИСА
        """
        if not task_message.target_services:
            return []
        
        # Удаляем текущий сервис из цепочки
        remaining = task_message.target_services[1:]
        return remaining
    
    async def _process_with_webhook_and_chain(self, task_message: TaskMessage, callback_url: str):
        """
        ФОНОВАЯ ОБРАБОТКА С ВЕБХУКОМ И ПОДДЕРЖКОЙ ЦЕПОЧЕК
        """
        print(f"🔄 {self.service_name} starting background processing: {task_message.message_id}", file=sys.stderr)
        
        self.is_processing = True
        self.current_task_id = task_message.message_id
        self.processing_start_time = time.time()
        
        try:
            result_data = await self._process_task_logic(task_message)
            
            # Обрабатываем цепочку
            next_message = await self._handle_service_chain(task_message, result_data)
            
            # Обновляем историю
            self.processing_history[str(task_message.message_id)]["completed_at"] = time.time()
            self.processing_history[str(task_message.message_id)]["status"] = "completed"
            self.processing_history[str(task_message.message_id)]["result"] = result_data.model_dump()
            
                
            # Если цепочка продолжается, отправляем задачу следующему сервису
            if isinstance(next_message, TaskMessage):
                await self._send_task_to_next_service(next_message)

                # Переопределяем next_message как результат для воркера
                next_message = ResultMessage(
                    message_id=task_message.message_id,
                    message_type=MessageType.RESULT,
                    source_service=self.service_name,
                    target_services=[],  # цепочка завершена
                    original_message_id=task_message.message_id,
                    success=True if result_data.payload_type != PayloadType.ERROR else False,
                )

            await self._send_webhook(callback_url, next_message)

            
            processing_time = (time.time() - self.processing_start_time) * 1000
            print(f"✅ {self.service_name} background task completed in {processing_time:.2f}ms", file=sys.stderr)
                
        except Exception as e:
            print(f"❌ {self.service_name} background processing failed: {e}", file=sys.stderr)
            
            error_result = ResultMessage(
                message_type=MessageType.RESULT,
                source_service=self.service_name,
                target_services=task_message.target_services,
                original_message_id=task_message.message_id,
                success=False,
                data=Data(
                    payload_type = PayloadType.ERROR,
                    payload={"text":str(type(e).__name__)},
                    execution_metadata={"error": True, "service": self.service_name}
                )
            )
            # Ошибку отправим сервис не смог справиться
            await self._send_webhook(callback_url, error_result)
            await self._send_webhook_to_wrapper(task_message.data.wrapper_callback_url, error_result)
        
        finally:
            self.is_processing = False
            self.current_task_id = None
            self.processing_start_time = None
    
    async def _send_task_to_next_service(self, task_message: TaskMessage) -> bool:
        """
        ОТПРАВЛЯЕТ ЗАДАЧУ СЛЕДУЮЩЕМУ СЕРВИСУ В ЦЕПОЧКЕ ЧЕРЕЗ RABBITMQ
        """
        if not task_message.target_services:
            print("⚠️ No target services for next task", file=sys.stderr)
            return False
        
        try:
            # Обеспечиваем наличие publisher
            publisher = await self.ensure_publisher()
            
            next_service = task_message.target_services[0]
            print(f"📤 {self.service_name} publishing task to RabbitMQ for: {next_service}", file=sys.stderr)

            # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ ДЛЯ ДЕБАГА
            print(f"   Publisher type: {type(publisher)}", file=sys.stderr)
            print(f"   Task message type: {type(task_message)}", file=sys.stderr)
            print(f"   Task message ID: {task_message.message_id}", file=sys.stderr)
            
            # Пробуем отправить с явным указанием routing_key
            routing_key = os.getenv("QUEUE_NAME", "tasks")
            print(f"   Using routing key: {routing_key}", file=sys.stderr)
            
            await publisher.publish_task(task_message, routing_key=routing_key)
            
            print(f"✅ {self.service_name} task published to RabbitMQ for {next_service}", file=sys.stderr)
            return True
                
        except Exception as e:
            print(f"❌ {self.service_name} failed to publish task to RabbitMQ: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            return False

    def _health_handler(self) -> Dict[str, Any]:
        """Обработчик health check"""
        return {
            "status": "ok", 
            "service": self.service_name,
            "timestamp": time.time()
        }
    
    def _status_handler(self) -> Dict[str, Any]:
        """Обработчик статуса сервиса"""
        status_info = {
            "is_busy": self.is_processing,
            "timestamp": time.time(),
            "service": self.service_name
        }
        
        if self.is_processing:
            status_info.update({
                "current_task_id": self.current_task_id,
                "processing_since": self.processing_start_time,
                "processing_time_seconds": time.time() - self.processing_start_time if self.processing_start_time else 0
            })
        
        return status_info
    
    def _list_requests_handler(self) -> Dict[str, Any]:
        """Обработчик списка запросов"""
        return {
            "total_requests": len(self.processing_history),
            "requests": self.processing_history
        }
    
    def _get_request_handler(self, request_id: str) -> Dict[str, Any]:
        """Обработчик получения конкретного запроса"""
        if request_id in self.processing_history:
            return self.processing_history[request_id]
        raise HTTPException(status_code=404, detail="Request not found")
    
    async def _validate_task(self, task_message: TaskMessage):
        """
        Валидация задачи
        
        Дочерние классы должны переопределить этот метод
        """
        pass
    
    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        """
        Основная логика обработки задачи
        
        Должна быть переопределена в дочерних классах
        """
        raise NotImplementedError("Дочерний класс должен реализовать этот метод")
    
    async def _process_task_sync(self, task_message: TaskMessage) -> Data:
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
    
    async def _send_webhook(self, callback_url: str, result_message: ResultMessage):
        """Отправляет вебхук"""
        if not callback_url:
            print("❌ no send to webhook - no callback url")

        try:
            print(f"📤 {self.service_name} sending webhook to: {callback_url}", file=sys.stderr)
            json_body = result_message.model_dump(mode='json')

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    callback_url,
                    json=json_body,
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