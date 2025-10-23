import httpx
from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data
from fastapi import HTTPException
import os
import requests


class LocalModelService(BaseService):
    """Local Model Service с использованием Ollama"""
    
    def __init__(self):
        super().__init__("local-model-service", "1.0")
        
        # Конфигурация локальной модели
        self.ollama_host = os.getenv("OLLAMA_HOST", "http://ollama:11434")
        self.model_name = os.getenv("LOCAL_MODEL", "llama2")  # или "mistral", "codellama" и т.д.
        self.timeout = int(os.getenv("MODEL_TIMEOUT", str(60*5)))

        # Создаем асинхронный клиент httpx
        self.client = httpx.AsyncClient(timeout=self.timeout)
    
    def _can_handle_task_type(self, task_type: str) -> bool:
        """Определяет, может ли сервис обработать тип задачи"""
        supported_task_types = [
            "generate_response", 
            "text_generation",
            "code_generation"
        ]
        return task_type in supported_task_types

    def _health_handler(self):
        """Проверка здоровья сервиса и доступности модели"""
        try:
            # Проверяем доступность Ollama
            response = requests.get(f"{self.ollama_host}/api/tags", timeout=5)
            models_available = response.status_code == 200
            
            print(f' ❔ Access ollama: {models_available}')

            # # Проверяем, загружена ли наша модель
            # if models_available:
            #     models_data = response.json()
            #     model_loaded = any(self.model_name in model['name'] for model in models_data.get('models', []))
            # else:
            #     model_loaded = False
            
            # print(f' ❔ Is busy ollama: {models_available}')

            status = "ok" if models_available else "unhealthy"
            
            return {
                "status": status,
                "service": self.service_name,
                "model": self.model_name,
                "ollama_available": models_available,
                #"model_loaded": model_loaded,
                "host": self.ollama_host
            }
        except Exception as e:
            return {
                "status": "error",
                "service": self.service_name,
                "error": str(e)
            }

    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задачи для локального сервиса"""
        print(f" Type message: {task_message.data.payload_type}")
        
        if task_message.data.payload_type != PayloadType.TEXT:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported payload type: {task_message.data.payload_type}"
            )
        
        prompt = task_message.data.payload.get("text", "")
        if not prompt:
            raise HTTPException(status_code=400, detail="Prompt is required")

    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        """Логика обработки задачи через локальную модель"""
        prompt = task_message.data.payload.get("text", "")
        
        # Дополнительные параметры
        max_tokens = task_message.data.payload.get("max_tokens", 512)
        temperature = task_message.data.payload.get("temperature", 0.7)
        
        # Вызываем локальную модель
        response = await self._call_local_model(prompt, max_tokens, temperature)
        
        return Data(
            payload_type=PayloadType.TEXT,
            payload={
                "task": "response_generation",
                "original_prompt": prompt,
                "text": response,
                "model_used": self.model_name,
                "parameters": {
                    "max_tokens": max_tokens,
                    "temperature": temperature
                }
            },
            execution_metadata={
                "task_type": "generate_response",
                "service": "local-model-service",
                "model": self.model_name
            }
        )
    
    async def _call_local_model(self, prompt: str, max_tokens: int, temperature: float) -> str:
        """Асинхронный вызов локальной модели через httpx"""
        try:
            url = f"{self.ollama_host}/api/generate"
            
            payload = {
                "model": self.model_name,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "num_predict": max_tokens,
                    "temperature": temperature
                }
            }
            
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
            result = response.json()
            return result.get("response", "").strip()
                
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=503, 
                detail=f"Ollama API error: {str(e)}"
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=503, 
                detail=f"Ollama service unavailable: {str(e)}"
            )

    async def __del__(self):
        """Закрываем клиент при завершении"""
        await self.client.aclose()

# Создаем и запускаем сервис
service = LocalModelService()

if __name__ == "__main__":
    service.run()