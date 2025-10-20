from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data
from fastapi import HTTPException
import os
from typing import Dict, Any

# Импортируем GigaChat
from gigachat import GigaChat


class GigaChatService(BaseService):
    """GigaChat Service с интеграцией GigaChat API"""
    
    def __init__(self):
        super().__init__("gigachat-service", "1.0")
        
        # Конфигурация GigaChat
        self.gigachat_token = os.getenv("GIGACHAT_TOKEN")
        self.gigachat_model = os.getenv("GIGACHAT_MODEL", "GigaChat")
        self.gigachat_verify_ssl = os.getenv("GIGACHAT_VERIFY_SSL", "True").lower() == "true"

    def _can_handle_task_type(self, task_type: str) -> bool:
        """
        ОПРЕДЕЛЯЕТ, МОЖЕТ ЛИСЕРВИС ОБРАБОТАТЬ ТИП ЗАДАЧИ
        
        Этот метод НУЖНО ДОБАВИТЬ - он теперь обязателен в BaseService
        """
        supported_task_types = [
            "generate_response", 
        ]
        return task_type in supported_task_types
    
    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задачи для сервиса"""
        # Теперь используем _can_handle_task_type для валидации
        # if not self._can_handle_task_type(task_message.data.task_type):
        #     raise HTTPException(
        #         status_code=400,
        #         detail=f"LLM service does not support task type: {task_message.data.task_type}"
        #     )
        pass

    #TODO воркер не видел если не было токена надо исправить
    async def _health_handler(self):
        """Переопределяем health handler для проверки токена"""
        status = "ok" if self.gigachat_token else "no_token"
        return {
            "status": status, 
            "service": self.service_name, 
            "model": self.gigachat_model,
            "token_configured": bool(self.gigachat_token)
        }
    
    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задачи для GigaChat сервиса"""
        # Проверяем токен
        if not self.gigachat_token:
            raise HTTPException(status_code=500, detail="GIGACHAT_TOKEN not configured")
        
        # Проверяем тип задачи
        # if task_message.data.task_type != "generate_response":
        #     raise HTTPException(
        #         status_code=400, 
        #         detail=f"Unsupported task type: {task_message.data.task_type}. Only 'generate_response' is supported"
        #     )
        
        # Проверяем наличие промпта
        prompt = task_message.data.input_data.get("prompt", "")
        if not prompt:
            raise HTTPException(status_code=500, detail=f"Prompt is required for generate_response task")
    # TODO воркер не хочет нормально обрабатывать ошибки

    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        """Логика обработки задачи через GigaChat"""
        prompt = task_message.data.input_data.get("prompt", "")
        
        # Вызываем GigaChat
        response = await self._call_gigachat(prompt)
        
        return Data(
            payload_type = PayloadType.TEXT,
            payload={
                "task": "response_generation",
                "original_prompt": prompt,
                "text": response,
                "model_used": self.gigachat_model
            },
            execution_metadata={
                "task_type": "generate_response",
                "service": "gigachat-service",
                "model": self.gigachat_model
            }
        )
    
    async def _call_gigachat(self, prompt: str) -> str:
        """Вызов GigaChat API"""
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: self._sync_gigachat_call(prompt)
            )
            return response
        except Exception as e:
            raise Exception(f"GigaChat API error: {str(e)}")
    
    def _sync_gigachat_call(self, prompt: str) -> str:
        """Синхронный вызов GigaChat"""
        with self._get_gigachat_client() as giga:
            response = giga.chat(prompt)
            return response.choices[0].message.content
    
    def _get_gigachat_client(self):
        """Создает клиент GigaChat"""
        return GigaChat(
            credentials=self.gigachat_token,
            model=self.gigachat_model,
            verify_ssl_certs=self.gigachat_verify_ssl,
            timeout=30
        )


# Создаем и запускаем сервис
service = GigaChatService()

if __name__ == "__main__":
    service.run()