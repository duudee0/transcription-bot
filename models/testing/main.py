from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional, Any, Dict
import uvicorn
import time
import sys
import json
import asyncio

# Импортируем ТОЛЬКО наши существующие модели
from common.models import TaskMessage, ResultMessage, ResultData, MessageType

app = FastAPI(title="LLM Service", version="0.2")

# НИКАКИХ НОВЫХ МОДЕЛЕЙ! Используем только то что уже есть в common.models

# "База данных" в памяти для демонстрации (просто словарь)
processing_history = {}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "llm-service"}

@app.get("/requests")
async def list_requests():
    """Посмотреть историю обработки запросов"""
    return {
        "total_requests": len(processing_history),
        "requests": processing_history
    }

@app.get("/requests/{request_id}")
async def get_request(request_id: str):
    """Получить информацию о конкретном запросе"""
    if request_id in processing_history:
        return processing_history[request_id]
    raise HTTPException(status_code=404, detail="Request not found")

@app.post("/api/v1/infer")
async def infer(request: Request) -> ResultMessage:
    """
    Endpoint для обработки задач.
    Принимает TaskMessage, возвращает ResultMessage.
    """
    start_time = time.time()
    
    try:
        # Парсим входящий JSON и валидируем как TaskMessage
        body = await request.json()
        task_message = TaskMessage.model_validate(body)
        
        # Сохраняем в историю
        processing_history[str(task_message.message_id)] = {
            "received_at": time.time(),
            "source_service": task_message.source_service,
            "task_type": task_message.data.task_type,
            "input_data": task_message.data.input_data,
            "status": "processing"
        }
        
        # Логируем получение
        print(f"[{time.time()}] Received task: {task_message.message_id}", file=sys.stderr)
        print(f"  From: {task_message.source_service}", file=sys.stderr)
        print(f"  Task: {task_message.data.task_type}", file=sys.stderr)
        
        # Обрабатываем задачу
        result_data = await process_task(task_message)
        
        # Создаем ResultMessage используя наши существующие модели
        result_message = ResultMessage(
            message_id=task_message.message_id,  # Можно сохранить тот же ID или создать новый
            message_type=MessageType.RESULT,
            source_service="llm-service",
            target_service=task_message.source_service,  # Отвечаем отправителю
            original_message_id=task_message.message_id,
            data=result_data
        )
        
        # Обновляем историю с результатом
        processing_history[str(task_message.message_id)]["completed_at"] = time.time()
        processing_history[str(task_message.message_id)]["status"] = "completed"
        processing_history[str(task_message.message_id)]["result"] = result_data.model_dump()
        
        processing_time = (time.time() - start_time) * 1000
        print(f"✅ Processed in {processing_time:.2f}ms", file=sys.stderr)
        
        return result_message
        
    except Exception as e:
        processing_time = (time.time() - start_time) * 1000
        print(f"❌ Error processing: {e}", file=sys.stderr)
        
        # Возвращаем ошибку тоже в формате ResultMessage
        error_result = ResultMessage(
            message_type=MessageType.RESULT,
            source_service="llm-service",
            original_message_id=getattr(task_message, 'message_id', None),
            data=ResultData(
                success=False,
                error_message=str(e),
                execution_metadata={"processing_time_ms": processing_time, "error": True}
            )
        )
        return error_result

async def process_task(task_message: TaskMessage) -> ResultData:
    """
    Обработка задачи используя существующие модели.
    Возвращаем ResultData - часть нашей существующей модели ResultMessage.
    """
    await asyncio.sleep(0.1)  # Имитация обработки
    
    task_type = task_message.data.task_type
    input_data = task_message.data.input_data
    
    if task_type == "analyze_text":
        result = await analyze_text(input_data)
    elif task_type == "process_image":
        result = await process_image(input_data)
    elif task_type == "generate_response":
        result = await generate_response(input_data)
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

async def analyze_text(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Анализ текста используя существующие форматы"""
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

async def process_image(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Обработка изображения используя существующие форматы"""
    image_url = input_data.get("image_url", "")
    
    return {
        "task": "image_processing", 
        "original_url": image_url,
        #"processed_url": f"https://storage.example.com/processed/{task_message.message_id}",
        "dimensions": {"width": 800, "height": 600},
        "format": "jpeg"
    }

async def generate_response(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Генерация ответа используя существующие форматы"""
    prompt = input_data.get("prompt", "")
    
    return {
        "task": "response_generation",
        "original_prompt": prompt,
        "generated_response": f"Это тестовый ответ на: '{prompt}'. [Здесь будет реальный LLM]",
        "response_length": len(prompt) + 50
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)