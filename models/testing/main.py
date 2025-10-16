from common.base_service import BaseService
from common.models import TaskMessage, ResultData
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
            "generate_response",
            "process_text",
            "chat_completion"
        ]
        return task_type in supported_task_types
    
    async def _validate_task(self, task_message: TaskMessage):
        """Валидация задачи для LLM сервиса"""
        # Теперь используем _can_handle_task_type для валидации
        if not self._can_handle_task_type(task_message.data.task_type):
            raise HTTPException(
                status_code=400,
                detail=f"LLM service does not support task type: {task_message.data.task_type}"
            )
    
    async def _process_task_logic(self, task_message: TaskMessage) -> ResultData:
        """Логика обработки задачи для LLM сервиса"""
        await asyncio.sleep(TESTING_SLEEP)  # Имитация обработки
        
        task_type = task_message.data.task_type
        input_data = task_message.data.input_data
        
        if task_type == "analyze_text":
            result = await self._analyze_text(input_data)
        elif task_type == "generate_response":
            result = await self._generate_response(input_data)
        else:
            result = {
                "status": "unknown_task_type",
                "received_data": input_data,
                "note": "This task type is not implemented yet"
            }
        
        return ResultData(
            success=True,
            result=result,
            execution_metadata={
                "processing_time_ms": 150.0,
                "task_type": task_type,
                "service": "llm-service"
            }
        )
    
    async def _analyze_text(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Анализ текста"""
        text = input_data.get("text", "")
        language = input_data.get("language", "ru")
        
        words = text.split()
        
        return {
            "task": "text_analysis",
            "word_count": len(words),
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
            "generated_response": f"Это тестовый ответ на: '{prompt}'. [Здесь будет реальный LLM]",
            "response_length": len(prompt) + 50
        }


# Создаем и запускаем сервис
service = LLMService()

if __name__ == "__main__":
    service.run()