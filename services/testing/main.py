from common.base_service import BaseService
from common.models import PayloadType, TaskMessage, Data
from fastapi import HTTPException
import asyncio
from typing import Dict, Any

# ТЕСТОВАЯ ЗАДЕРЖКА УКАЗЫВАТЬ
TESTING_SLEEP = 20

class LLMService(BaseService):
    """LLM Service с тестовой реализацией"""
    
    def __init__(self):
        super().__init__("llm-service", "0.3")
    
    def _can_handle_task_type(self, task_type: str) -> bool:
        """
        ОПРЕДЕЛЯЕТ, МОЖЕТ ЛИ LLM СЕРВИС ОБРАБОТАТЬ ТИП ЗАДАЧИ
        
        Этот метод НУЖНО ДОБАВИТЬ - он теперь обязателен в BaseService
        """
        supported_task_types = [
            "analyze_text", 
            "prompt_response",
        ]
        return task_type in supported_task_types
    
    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задачи для LLM сервиса"""
        # Проверяем тип задачи
        if task_message.data.payload_type != PayloadType.TEXT:
            raise HTTPException( # Этот сервис ожидает текстовый запрос
                status_code=400, 
                detail=f"Unsupported task type: {task_message.data.payload_type}. Only is supported"
            )
    
    async def _process_task_logic(self, task_message: TaskMessage) -> Data:
        """Логика обработки задачи для LLM сервиса"""
        await asyncio.sleep(TESTING_SLEEP)  # Имитация обработки
        
        task_type = task_message.data.task_type
        input_data = task_message.data
        
        if task_type == "analyze_text":
            result = await self._analyze_text(input_data)
        elif task_type == "generate_response":
            result = await self._generate_response(input_data)
        else:
            result = await self._analyze_text(input_data)
        
        return Data(
            payload_type = PayloadType.TEXT,
            payload = result,
            execution_metadata={
                "processing_time_ms": 150.0,
                "task_type": task_type,
                "service": "llm-service"
            }
        )
    
    async def _analyze_text(self, input_data: Data) -> Dict[str, Any]:
        """Анализ текста"""
        text = input_data.payload.get('text', '')
        language = input_data.payload.get("language", "ru")
        
        words = text.split()
        
        prompt = f"{text} \nin this is text word:{len(words)}"

        return {
            "task": "text_analysis",
            "word_count": len(words),
            "text": prompt,
            "language": language,
            "estimated_reading_time_sec": max(1, len(words) // 3),
            "contains_questions": "?" in text,
            "sample_analysis": {
                "sentiment": "positive" if any(word in text.lower() for word in ["хорош", "отлич", "прекрас"]) else "neutral"
            }
        }
    
    async def _generate_response(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Генерация ответа"""
        prompt = input_data.get("prompt", "")
        
        return {
            "task": "response_generation",
            "original_prompt": prompt,
            "text": f"Это тестовый ответ на: '{prompt}'. [Здесь будет реальный LLM]",
            "response_length": len(prompt) + 50
        }


# Создаем и запускаем сервис
service = LLMService()

if __name__ == "__main__":
    service.run()