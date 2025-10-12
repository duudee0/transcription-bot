from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
import uvicorn
import time
import sys
import asyncio
import httpx
import os

# Импортируем общие модели
from common.models import TaskMessage, ResultMessage, ResultData, MessageType

# Импортируем GigaChat
from gigachat import GigaChat

app = FastAPI(title="GigaChat Service", version="1.0")

# Конфигурация
GIGACHAT_TOKEN = os.getenv("GIGACHAT_TOKEN")
GIGACHAT_MODEL = os.getenv("GIGACHAT_MODEL", "GigaChat")
GIGACHAT_VERIFY_SSL = os.getenv("GIGACHAT_VERIFY_SSL", "True").lower() == "true"

# Инициализация GigaChat клиента
def get_gigachat_client():
    """Создает и возвращает клиент GigaChat"""
    return GigaChat(
        credentials=GIGACHAT_TOKEN,
        model=GIGACHAT_MODEL,
        verify_ssl_certs=GIGACHAT_VERIFY_SSL,
        timeout=30
    )

# "База данных" в памяти
processing_history = {}
is_processing = False
current_task_id = None
processing_start_time = None

@app.get("/health")
async def health():
    status = "ok" if GIGACHAT_TOKEN else "no_token"
    return {
        "status": status, 
        "service": "gigachat-service", 
        "model": GIGACHAT_MODEL,
        "token_configured": bool(GIGACHAT_TOKEN)
    }

@app.get("/status")
async def status():
    global is_processing, current_task_id, processing_start_time
    
    status_info = {
        "is_busy": is_processing,
        "timestamp": time.time(),
        "model": GIGACHAT_MODEL
    }
    
    if is_processing:
        status_info.update({
            "current_task_id": str(current_task_id),
            "processing_since": processing_start_time,
            "processing_time_seconds": time.time() - processing_start_time if processing_start_time else 0
        })
    
    return status_info

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

@app.post("/api/v1/process")
async def process_task_endpoint(request: Request, background_tasks: BackgroundTasks) -> ResultMessage:
    """
    Endpoint для обработки задач через GigaChat.
    Поддерживает только generate_response задачи.
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
        print(f"[{time.time()}] Received GigaChat task: {task_message.message_id}", file=sys.stderr)
        print(f"  From: {task_message.source_service}", file=sys.stderr)
        print(f"  Task: {task_message.data.task_type}", file=sys.stderr)
        
        # Проверяем токен
        if not GIGACHAT_TOKEN:
            raise HTTPException(status_code=500, detail="GIGACHAT_TOKEN not configured")
        
        # Проверяем тип задачи - поддерживаем только generate_response
        if task_message.data.task_type != "generate_response":
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported task type: {task_message.data.task_type}. Only 'generate_response' is supported"
            )
        
        # Проверяем поддержку вебхука
        callback_url = task_message.data.input_data.get("callback_url")
        webhook_supported = task_message.data.input_data.get("webhook_supported", False)
        
        if callback_url and webhook_supported and not is_processing:
            print(f"🔔 Webhook mode activated for {task_message.message_id}", file=sys.stderr)
            
            # Запускаем фоновую задачу
            background_tasks.add_task(
                process_with_webhook,
                task_message,
                callback_url
            )
            
            # Немедленный ответ
            return ResultMessage(
                message_id=task_message.message_id,
                message_type=MessageType.RESULT,
                source_service="gigachat-service",
                target_service=task_message.source_service,
                original_message_id=task_message.message_id,
                data=ResultData(
                    success=True,
                    result={"status": "accepted", "message": "Processing in background via webhook"},
                    execution_metadata={
                        "processing_mode": "async_webhook",
                        "service": "gigachat-service",
                        "model": GIGACHAT_MODEL
                    }
                )
            )
        
        # Синхронная обработка
        result_data = await process_task_sync(task_message)
        
        # Создаем ResultMessage
        result_message = ResultMessage(
            message_id=task_message.message_id,
            message_type=MessageType.RESULT,
            source_service="gigachat-service",
            target_service=task_message.source_service,
            original_message_id=task_message.message_id,
            data=result_data
        )
        
        # Обновляем историю
        processing_history[str(task_message.message_id)]["completed_at"] = time.time()
        processing_history[str(task_message.message_id)]["status"] = "completed"
        processing_history[str(task_message.message_id)]["result"] = result_data.model_dump()
        
        processing_time = (time.time() - start_time) * 1000
        print(f"✅ GigaChat processed in {processing_time:.2f}ms", file=sys.stderr)
        
        return result_message
        
    except Exception as e:
        processing_time = (time.time() - start_time) * 1000
        print(f"❌ GigaChat error: {e}", file=sys.stderr)
        error_result = ResultMessage(
            message_type=MessageType.RESULT,
            source_service="gigachat-service",
            original_message_id=getattr(task_message, 'message_id', None),
            data=ResultData(
                success=False,
                error_message=str(e),
                execution_metadata={
                    "processing_time_ms": processing_time, 
                    "error": True,
                    "service": "gigachat-service"
                }
            )
        )
        return error_result

async def process_with_webhook(task_message: TaskMessage, callback_url: str):
    """Фоновая обработка с вебхуком"""
    global is_processing, current_task_id, processing_start_time
    
    print(f"🔄 Starting GigaChat background processing: {task_message.message_id}", file=sys.stderr)
    
    is_processing = True
    current_task_id = task_message.message_id
    processing_start_time = time.time()
    
    try:
        result_data = await process_task_logic(task_message)
        
        # Обновляем историю
        processing_history[str(task_message.message_id)]["completed_at"] = time.time()
        processing_history[str(task_message.message_id)]["status"] = "completed"
        processing_history[str(task_message.message_id)]["result"] = result_data.model_dump()
        
        # Создаем ResultMessage для вебхука
        result_message = ResultMessage(
            message_id=task_message.message_id,
            message_type=MessageType.RESULT,
            source_service="gigachat-service",
            target_service=task_message.source_service,
            original_message_id=task_message.message_id,
            data=result_data
        )
        
        # Отправляем вебхук
        await send_webhook(callback_url, result_message)
        
        processing_time = (time.time() - processing_start_time) * 1000
        print(f"✅ GigaChat background task completed in {processing_time:.2f}ms", file=sys.stderr)
        
    except Exception as e:
        print(f"❌ GigaChat background processing failed: {e}", file=sys.stderr)
        
        error_result = ResultMessage(
            message_type=MessageType.RESULT,
            source_service="gigachat-service",
            original_message_id=task_message.message_id,
            data=ResultData(
                success=False,
                error_message=str(e),
                execution_metadata={"error": True, "service": "gigachat-service"}
            )
        )
        await send_webhook(callback_url, error_result)
    
    finally:
        is_processing = False
        current_task_id = None
        processing_start_time = None

async def send_webhook(callback_url: str, result_message: ResultMessage):
    """Отправляет вебхук"""
    try:
        print(f"📤 Sending GigaChat webhook to: {callback_url}", file=sys.stderr)
        
        webhook_data = result_message.model_dump(mode='json')
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                callback_url,
                json=webhook_data,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                print(f"✅ GigaChat webhook delivered", file=sys.stderr)
                return True
            else:
                print(f"⚠️ GigaChat webhook failed: {response.status_code}", file=sys.stderr)
                return False
                
    except Exception as e:
        print(f"❌ GigaChat webhook sending failed: {e}", file=sys.stderr)
        return False

async def process_task_sync(task_message: TaskMessage) -> ResultData:
    """Синхронная обработка задачи"""
    global is_processing, current_task_id, processing_start_time
    
    if is_processing:
        raise HTTPException(
            status_code=423,
            detail=f"Service is busy processing task {current_task_id}"
        )
    
    is_processing = True
    current_task_id = task_message.message_id
    processing_start_time = time.time()

    try:
        result_data = await process_task_logic(task_message)
        return result_data
    
    finally:
        is_processing = False
        current_task_id = None
        processing_start_time = None

async def process_task_logic(task_message: TaskMessage) -> ResultData:
    """Основная логика обработки через GigaChat"""
    start_time = time.time()
    
    try:
        # Получаем промпт из входных данных
        prompt = task_message.data.input_data.get("prompt", "")
        
        if not prompt:
            raise ValueError("Prompt is required for generate_response task")
        
        # Вызываем GigaChat
        response = await call_gigachat(prompt)
        
        processing_time = (time.time() - start_time) * 1000
        
        return ResultData(
            success=True,
            result={
                "task": "response_generation",
                "original_prompt": prompt,
                "generated_response": response,
                "model_used": GIGACHAT_MODEL
            },
            execution_metadata={
                "processing_time_ms": processing_time,
                "task_type": "generate_response",
                "service": "gigachat-service",
                "model": GIGACHAT_MODEL
            }
        )
        
    except Exception as e:
        processing_time = (time.time() - start_time) * 1000
        return ResultData(
            success=False,
            error_message=str(e),
            execution_metadata={
                "processing_time_ms": processing_time,
                "task_type": "generate_response",
                "service": "gigachat-service",
                "model": GIGACHAT_MODEL,
                "error": True
            }
        )

async def call_gigachat(prompt: str) -> str:
    """Вызов GigaChat API"""
    try:
        # Используем asyncio.to_thread для синхронных вызовов
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None, 
            lambda: sync_gigachat_call(prompt)
        )
        return response
    except Exception as e:
        raise Exception(f"GigaChat API error: {str(e)}")

def sync_gigachat_call(prompt: str) -> str:
    """Синхронный вызов GigaChat"""
    with get_gigachat_client() as giga:
        response = giga.chat(prompt)
        return response.choices[0].message.content

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)