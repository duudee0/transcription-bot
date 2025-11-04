from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data
from fastapi import HTTPException
import os
import requests
import json
from typing import Dict, Any
import asyncio
from datetime import datetime, timedelta

class YandexGPTService(BaseService):
    """Yandex GPT Service с интеграцией через Yandex Cloud API"""
    
    def __init__(self):
        super().__init__("yandex-gpt-service", "1.0")
        
        # Конфигурация Yandex Cloud
        self.oauth_token = os.getenv("YANDEX_OAUTH_TOKEN")
        self.folder_id = os.getenv("YANDEX_FOLDER_ID")
        
        # IAM токен и время его истечения
        self.iam_token = None
        self.iam_token_expiry = None
        
        # Параметры модели
        self.model_uri = os.getenv("YANDEX_MODEL_URI", "gpt://{folder_id}/yandexgpt/latest")
        self.default_temperature = float(os.getenv("YANDEX_TEMPERATURE", "0.7"))
        self.default_max_tokens = int(os.getenv("YANDEX_MAX_TOKENS", "1000"))
        
        # API endpoints
        self.iam_token_url = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
        self.completion_url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        
        # Поддерживаемые модели
        self.supported_models = [
            "yandexgpt/latest",
            "yandexgpt-lite/latest",
            "summarization/latest"
        ]
    
    def _can_handle_task_type(self, task_type: str) -> bool:
        """
        Определяет, может ли сервис обработать тип задачи
        """
        supported_task_types = [
            "generate_response", 
            "text_completion",
            "chat_completion",
            "summarization"
        ]
        return task_type in supported_task_types
    
    def _health_handler(self):
        """Проверка здоровья сервиса и конфигурации"""
        token_status = "configured" if self.oauth_token and self.folder_id else "missing_config"
        iam_status = "valid" if self._is_iam_token_valid() else "needs_refresh"
        
        return {
            "status": "ok" if token_status == "configured" else "degraded",
            "service": self.service_name,
            "token_status": token_status,
            "iam_token_status": iam_status,
            "model_uri": self.model_uri.format(folder_id=self.folder_id),
            "supported_models": self.supported_models,
            "provider": "Yandex Cloud"
        }
    
    def _is_iam_token_valid(self):
        """Проверяет, действителен ли текущий IAM токен"""
        if not self.iam_token or not self.iam_token_expiry:
            return False
        return datetime.now() < self.iam_token_expiry
    
    async def _refresh_iam_token(self):
        """Получает новый IAM токен"""
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(
                    self.iam_token_url,
                    json={"yandexPassportOauthToken": self.oauth_token}
                )
            )
            
            response.raise_for_status()
            token_data = response.json()
            
            self.iam_token = token_data["iamToken"]
            # Устанавливаем время истечения за 5 минут до фактического для надежности
            expires_in = token_data.get("expiresIn", 3600) - 300
            self.iam_token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            print(f" ✓ IAM token refreshed, expires at {self.iam_token_expiry}")
            return True
            
        except Exception as e:
            print(f" 🚨 Failed to refresh IAM token: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to refresh IAM token: {str(e)}"
            )
    
    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задачи для Yandex GPT сервиса"""
        # Проверяем конфигурацию
        if not self.oauth_token or not self.folder_id:
            print(" ‼️ YANDEX_OAUTH_TOKEN or YANDEX_FOLDER_ID not configured")
            raise HTTPException(
                status_code=500, 
                detail="Yandex Cloud credentials not configured properly"
            )
        
        # Обновляем IAM токен, если необходимо
        if not self._is_iam_token_valid():
            await self._refresh_iam_token()
        
        # Проверяем наличие промпта
        prompt = task_message.data.payload.get("text", "") or task_message.data.payload.get("prompt", "")
        if not prompt:
            print(" ⛔ Prompt 'text' or 'prompt' in payload is required")
            raise HTTPException(
                status_code=400, 
                detail="Prompt 'text' or 'prompt' in payload is required for generate_response task"
            )
        
        # Проверяем поддерживаемый тип задачи
        task_type = task_message.data.payload.get("task_type", "generate_response")
        if not self._can_handle_task_type(task_type):
            print(f" ⛔ Unsupported task type: {task_type}")
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported task type: {task_type}"
            )

    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        """Логика обработки задачи через Yandex GPT API"""
        prompt = task_message.data.payload.get("text", "") or task_message.data.payload.get("prompt", "")
        
        # Дополнительные параметры из запроса
        max_tokens = task_message.data.payload.get("max_tokens", self.default_max_tokens)
        temperature = task_message.data.payload.get("temperature", self.default_temperature)
        task_type = task_message.data.payload.get("task_type", "generate_response")
        
        # Определяем модель в зависимости от типа задачи
        model_name = "yandexgpt/latest"
        if task_type == "summarization":
            model_name = "summarization/latest"
        elif task_type == "lite":
            model_name = "yandexgpt-lite/latest"
        
        # Формируем полный URI модели
        model_uri = self.model_uri.format(folder_id=self.folder_id).replace("yandexgpt/latest", model_name)
        
        # Вызываем Yandex GPT
        response = await self._call_yandex_gpt(prompt, model_uri, max_tokens, temperature)
        
        return Data(
            payload_type=PayloadType.TEXT,
            payload={
                "task": "response_generation",
                "original_prompt": prompt,
                "text": response,
                "model_used": model_name,
                "max_tokens": max_tokens,
                "temperature": temperature
            },
            execution_metadata={
                "task_type": task_type,
                "service": "yandex-gpt-service",
                "model": model_name,
                "api_provider": "Yandex Cloud"
            }
        )
    
    async def _call_yandex_gpt(self, prompt: str, model_uri: str, max_tokens: int = 1000, temperature: float = 0.7) -> str:
        """Асинхронный вызов Yandex GPT API"""
        try:
            # Убеждаемся, что у нас есть валидный IAM токен
            if not self._is_iam_token_valid():
                await self._refresh_iam_token()
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: self._sync_yandex_gpt_call(prompt, model_uri, max_tokens, temperature)
            )
            return response
        except Exception as e:
            print(f" 🚨 Yandex GPT API error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Yandex GPT API error: {str(e)}"
            )
    
    def _sync_yandex_gpt_call(self, prompt: str, model_uri: str, max_tokens: int, temperature: float) -> str:
        """Синхронный вызов Yandex GPT API"""
        try:
            headers = {
                "Authorization": f"Bearer {self.iam_token}",
                "Content-Type": "application/json"
            }

            data = {
                "modelUri": model_uri,
                "completionOptions": {
                    "temperature": temperature,
                    "maxTokens": str(max_tokens)
                },
                "messages": [
                    {
                        "role": "user",
                        "text": prompt
                    }
                ]
            }

            response = requests.post(self.completion_url, headers=headers, json=data, timeout=60)
            response.raise_for_status()
            
            result = response.json()
            if "result" in result and "alternatives" in result["result"]:
                return result["result"]["alternatives"][0]["message"]["text"]
            else:
                raise RuntimeError(f"Unexpected API response structure: {result}")
            
        except Exception as e:
            print(f" 🚨 Yandex GPT call failed: {str(e)}")
            raise RuntimeError(f"Yandex GPT API call failed: {str(e)}") from e


# Создаем и запускаем сервис
service = YandexGPTService()

if __name__ == "__main__":
    service.run()