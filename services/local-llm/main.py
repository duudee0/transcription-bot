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
        """Проверка здоровья сервиса и доступности модели с подробным логированием"""
        try:
            # Логируем попытку подключения
            health_check_url = f"{self.ollama_host}/api/tags"
            print(f"🔍 Проверка здоровья Ollama...")
            print(f"   Хост: {self.ollama_host}")
            print(f"   Модель: {self.model_name}")
            print(f"   URL запроса: {health_check_url}")
            
            # Проверяем доступность Ollama
            response = requests.get(health_check_url, timeout=5)
            
            # Детальная информация о HTTP-ответе
            print(f"✅ HTTP запрос выполнен")
            print(f"   Статус код: {response.status_code}")
            print(f"   Время ответа: {response.elapsed.total_seconds():.2f} секунд")
            
            models_available = response.status_code == 200
            
            if models_available:
                print(f"🎉 Ollama доступен и отвечает!")
                
                # Проверяем, загружена ли наша модель (раскомментируйте если нужно)
                try:
                    models_data = response.json()
                    all_models = [model['name'] for model in models_data.get('models', [])]
                    model_loaded = any(self.model_name in model_name for model_name in all_models)
                    
                    print(f"📋 Доступные модели в Ollama: {', '.join(all_models) if all_models else 'Нет моделей'}")
                    print(f"🔎 Ищем модель '{self.model_name}': {'НАЙДЕНА' if model_loaded else 'НЕ НАЙДЕНА'}")
                    
                    if not model_loaded:
                        print(f"⚠️  Внимание: Модель '{self.model_name}' не найдена в Ollama!")
                        print(f"   Используйте команду: ollama pull {self.model_name}")
                except Exception as parse_error:
                    print(f"⚠️  Не удалось разобрать ответ от Ollama: {str(parse_error)}")
                    print(f"   Ответ сервера: {response.text[:200]}...")
                    model_loaded = False
            else:
                print(f"❌ Ollama недоступен! Статус код: {response.status_code}")
                print(f"   Ответ сервера: {response.text[:200]}...")
                model_loaded = False
            
            status = "ok" if (models_available and model_loaded) else "unhealthy"
            
            result = {
                "status": status,
                "service": self.service_name,
                "model": self.model_name,
                "ollama_available": models_available,
                "model_loaded": model_loaded,
                "host": self.ollama_host,
                "http_status": response.status_code,
                "response_time_seconds": response.elapsed.total_seconds(),
                "available_models": all_models if models_available else []
            }
            
            print(f"📊 Итоговый статус: {status.upper()}")
            print("-" * 50)
            
            return result
            
        except requests.exceptions.Timeout:
            error_msg = f"Таймаут подключения к Ollama ({self.ollama_host}) через 5 секунд"
            print(f"⏰ {error_msg}")
            print(f"   Проверьте:")
            print(f"   1. Запущен ли Ollama на хосте: ollama serve")
            print(f"   2. Правильно ли настроен OLLAMA_HOST: сейчас '{self.ollama_host}'")
            print(f"   3. Доступен ли порт 11434 из контейнера")
            
            return {
                "status": "error",
                "service": self.service_name,
                "error": error_msg,
                "error_type": "Timeout",
                "host": self.ollama_host,
                "model": self.model_name
            }
            
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Не удалось подключиться к Ollama ({self.ollama_host})"
            print(f"🔌 {error_msg}")
            print(f"   Детали: {str(e)}")
            print(f"   Возможные причины:")
            print(f"   1. Ollama не запущена. Запустите: ollama serve")
            print(f"   2. Неправильный адрес. Проверьте OLLAMA_HOST: сейчас '{self.ollama_host}'")
            print(f"   3. Огненная стена блокирует порт 11434")
            
            return {
                "status": "error",
                "service": self.service_name,
                "error": error_msg,
                "error_type": "ConnectionError",
                "details": str(e),
                "host": self.ollama_host,
                "model": self.model_name
            }
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Ошибка при запросе к Ollama: {str(e)}"
            print(f"🚨 {error_msg}")
            
            return {
                "status": "error",
                "service": self.service_name,
                "error": error_msg,
                "error_type": type(e).__name__,
                "host": self.ollama_host,
                "model": self.model_name
            }
            
        except Exception as e:
            error_msg = f"Неожиданная ошибка: {str(e)}"
            print(f"💥 {error_msg}")
            import traceback
            traceback.print_exc()
            
            return {
                "status": "error",
                "service": self.service_name,
                "error": error_msg,
                "error_type": type(e).__name__,
                "host": self.ollama_host,
                "model": self.model_name
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