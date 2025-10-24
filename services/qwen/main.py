from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data
from fastapi import HTTPException
import os
from typing import Dict, Any
from openai import OpenAI

class QwenService(BaseService):
    """Qwen Service с интеграцией через OpenRouter API"""
    
    def __init__(self):
        super().__init__("qwen-service", "1.0")
        
        # Конфигурация OpenRouter
        self.openrouter_api_key = os.getenv("OPENROUTER_API_KEY")
        self.openrouter_model = os.getenv("QWEN_MODEL", "qwen/qwen3-30b-a3b:free") #qwen/qwen3-30b-a3b:free
        self.openrouter_base_url = "https://openrouter.ai/api/v1"
        
        # OpenRouter специфичные настройки
        self.openrouter_site_url = os.getenv("OPENROUTER_SITE_URL", "https://github.com/ivan-proger")
        self.openrouter_site_name = os.getenv("OPENROUTER_SITE_NAME", "My AI Service")
        
        # Поддерживаемые модели Qwen на OpenRouter
        self.supported_models = [
            "qwen/qwen3-30b-a3b:free", # Сейчас оно через open router
            "qwen/qwen2.5-72b-instruct",
            "qwen/qwen2.5-32b-instruct", 
            "qwen/qwen2.5-7b-instruct",
            "qwen/qwen2.5-1.5b-instruct",
            "qwen/qwen2.5-coder-32b-instruct",
            "qwen/qwen2.5-vl-72b-instruct",
            "qwen/qwen2-vl-72b-instruct",
            "qwen/qwen2-72b-instruct",
            "qwen/qwen2-7b-instruct",
            "qwen/qwen2-1.5b-instruct",
            "qwen/qwen2-0.5b-instruct"
        ]
        
        # Инициализация клиента OpenAI для OpenRouter
        self.client = None
        if self.openrouter_api_key:
            self.client = OpenAI(
                base_url=self.openrouter_base_url,
                api_key=self.openrouter_api_key,
            )

    def _can_handle_task_type(self, task_type: str) -> bool:
        """
        Определяет, может ли сервис обработать тип задачи
        """
        supported_task_types = [
            "generate_response", 
            "text_completion",
            "chat_completion"
        ]
        return task_type in supported_task_types

    def _health_handler(self):
        """Проверка здоровья сервиса и конфигурации"""
        status = "ok" if self.openrouter_api_key and self.client else "no_token"
        model_status = "supported" if self.openrouter_model in self.supported_models else "unsupported"
        
        return {
            "status": status,
            "service": self.service_name,
            "model": self.openrouter_model,
            "model_status": model_status,
            "token_configured": bool(self.openrouter_api_key),
            "client_initialized": bool(self.client),
            "supported_models": self.supported_models,
            "provider": "OpenRouter"
        }
    
    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задачи для Qwen сервиса"""
        # Проверяем API ключ
        if not self.openrouter_api_key:
            print(" ‼️ OPENROUTER_API_KEY not configured")
            raise HTTPException(
                status_code=500, 
                detail="OPENROUTER_API_KEY environment variable not configured"
            )
        
        if not self.client:
            print(" ‼️ OpenRouter client not initialized")
            raise HTTPException(
                status_code=500,
                detail="OpenRouter client initialization failed"
            )
        
        # Проверяем поддерживаемую модель
        if self.openrouter_model not in self.supported_models:
            print(f" ⛔ Unsupported model: {self.openrouter_model}")
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported model: {self.openrouter_model}. Supported models: {', '.join(self.supported_models)}"
            )
        
        # Проверяем наличие промпта
        prompt = task_message.data.payload.get("text", "") or task_message.data.payload.get("prompt", "")
        if not prompt:
            print(" ⛔ Prompt 'text' or 'prompt' in payload is required")
            raise HTTPException(
                status_code=400, 
                detail="Prompt 'text' or 'prompt' in payload is required for generate_response task"
            )

    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        """Логика обработки задачи через OpenRouter API"""
        prompt = task_message.data.payload.get("text", "") or task_message.data.payload.get("prompt", "")
        
        # Дополнительные параметры из запроса
        max_tokens = task_message.data.payload.get("max_tokens", 2000)
        temperature = task_message.data.payload.get("temperature", 0.8)
        
        # Вызываем Qwen через OpenRouter
        response = await self._call_openrouter(prompt, max_tokens, temperature)
        
        return Data(
            payload_type=PayloadType.TEXT,
            payload={
                "task": "response_generation",
                "original_prompt": prompt,
                "text": response,
                "model_used": self.openrouter_model,
                "max_tokens": max_tokens,
                "temperature": temperature
            },
            execution_metadata={
                "task_type": task_message.data.payload.get("task_type", "generate_response"),
                "service": "qwen-service",
                "model": self.openrouter_model,
                "api_provider": "OpenRouter"
            }
        )
    
    async def _call_openrouter(self, prompt: str, max_tokens: int = 2000, temperature: float = 0.8) -> str:
        """Асинхронный вызов OpenRouter API"""
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: self._sync_openrouter_call(prompt, max_tokens, temperature)
            )
            return response
        except Exception as e:
            print(f" 🚨 OpenRouter API error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"OpenRouter API error: {str(e)}"
            )
    
    def _sync_openrouter_call(self, prompt: str, max_tokens: int, temperature: float) -> str:
        """Синхронный вызов OpenRouter API"""
        try:
            completion = self.client.chat.completions.create(
                extra_headers={
                    "HTTP-Referer": self.openrouter_site_url,
                    "X-Title": self.openrouter_site_name,
                },
                model=self.openrouter_model,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=max_tokens,
                temperature=temperature,
                timeout=60  # Таймаут в секундах
            )
            
            return completion.choices[0].message.content
            
        except Exception as e:
            print(f" 🚨 OpenRouter call failed: {str(e)}")
            raise RuntimeError(f"OpenRouter API call failed: {str(e)}") from e


# Создаем и запускаем сервис
service = QwenService()

if __name__ == "__main__":
    service.run()