"""Webhook handler для приема callback от Wrapper."""
from fastapi import FastAPI, Request, HTTPException
from datetime import datetime

from config import config
from dependencies import ServiceContainer
from utils import format_task_result


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
                
                print(f"📨 Webhook received for user {user_id}: {payload}")
                
                task_id = payload.get("task_id")
                status = payload.get("status")
                result = payload.get("result")
                error = payload.get("error")
                
                if not task_id:
                    raise HTTPException(status_code=400, detail="Missing task_id")
                
                # Получаем сервисы через контейнер
                container = ServiceContainer.get_instance()
                if container.task_manager is None:
                    print("❌ Task manager not available")
                    return {"status": "error", "message": "Task manager not available"}
                
                # Обновляем задачу
                if task_id in container.task_manager.user_tasks:
                    task = container.task_manager.user_tasks[task_id]
                    task.status = status
                    task.result = result
                    task.error = error
                    task.updated_at = datetime.now()
                    
                    # Отправляем уведомление пользователю
                    message_text = format_task_result(task_id, payload)
                    if container.bot:
                        await container.bot.send_message(task.chat_id, message_text)
                        print(f"✅ Webhook processed for task {task_id}")
                    else:
                        print("❌ Bot not available for sending message")
                else:
                    print(f"⚠️ Task {task_id} not found in user tasks")
                
                return {"status": "ok"}
                
            except Exception as error:
                print(f"❌ Webhook error: {error}")
                raise HTTPException(status_code=500, detail=str(error))

    @property
    def application(self) -> FastAPI:
        """Получить FastAPI приложение."""
        return self.app