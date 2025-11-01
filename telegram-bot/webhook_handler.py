"""Webhook handler для приема callback от Wrapper."""
from fastapi import FastAPI, Request, HTTPException
from datetime import datetime

from config import config
from dependencies import ServiceContainer
from utils import format_task_result
from models import TaskStatus
from logger import get_logger

# Инициализируем логгер для модуля
logger = get_logger(__name__)

class WebhookHandler:
    """Обработчик вебхуков от Wrapper API."""
    
    def __init__(self):
        self.app = FastAPI()
        self._setup_routes()
    
    def _setup_routes(self) -> None:
        """Настройка маршрутов FastAPI."""
        
        @self.app.post("/client/webhook/{user_id}")
        async def handle_wrapper_webhook(user_id: str, request: Request) -> dict:
            """Обработчик webhook от wrapper."""
            try:
                payload = await request.json()
                
                logger.info(f"📨 Webhook received for user {user_id}: {payload.get('status')}")
                
                task_id = payload.get("task_id")
                status = payload.get("status")
                result = payload.get("result")
                error = payload.get("error")
                
                if not task_id:
                    raise HTTPException(status_code=400, detail="Missing task_id")
                
                # Получаем сервисы через контейнер
                container = ServiceContainer.get_instance()
                if container.task_manager is None:
                    logger.error("❌ Task manager not available")
                    return {"status": "error", "message": "Task manager not available"}
                
                # Обновляем задачу
                if task_id in container.task_manager.user_tasks:
                    task = container.task_manager.user_tasks[task_id]
                    task.status = TaskStatus(status)
                    task.result = result
                    task.error = error
                    task.updated_at = datetime.now()
                    
                    # Отправляем результат пользователю через FileSender
                    try:
                        from services.file_sender import FileSender

                        file_sender = FileSender(container.bot)  # Создаем напрямую
        
                        await file_sender.send_task_result(
                            chat_id=task.chat_id,
                            task_id=task_id,
                            status=status,
                            result=result,
                            error=error
                        )
                        logger.info(f"✅ Webhook processed for task {task_id}")
                    except Exception as e:
                        logger.error(f"❌ Error sending result for task {task_id}: {e}")
                else:
                    logger.warning(f"⚠️ Task {task_id} not found in user tasks")
                
                return {"status": "ok"}
                
            except Exception as error:
                logger.error(f"❌ Webhook error: {error}")
                raise HTTPException(status_code=500, detail=str(error))

    @property
    def application(self) -> FastAPI:
        """Получить FastAPI приложение."""
        return self.app