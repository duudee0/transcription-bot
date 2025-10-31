from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot
from datetime import datetime

from config import config
from services import task_manager
from utils import format_task_result


class WebhookHandler:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.app = FastAPI()
        self._setup_routes()
    
    def _setup_routes(self):
        @self.app.post("/client/webhook/{user_id}")
        async def handle_wrapper_webhook(user_id: str, request: Request):
            """Обработчик webhook от wrapper"""
            try:
                payload = await request.json()
                
                print(f"📨 Webhook received for user {user_id}: {payload}")
                
                task_id = payload.get("task_id")
                status = payload.get("status")
                result = payload.get("result")
                error = payload.get("error")
                
                if not task_id:
                    raise HTTPException(status_code=400, detail="Missing task_id")
                
                # Обновляем задачу в менеджере
                if task_id in task_manager.user_tasks:
                    task = task_manager.user_tasks[task_id]
                    task.status = status
                    task.result = result
                    task.error = error
                    task.updated_at = datetime.now()
                    
                    # Отправляем уведомление пользователю
                    message_text = format_task_result(task_id, payload)
                    await self.bot.send_message(task.chat_id, message_text)
                    
                    print(f"✅ Webhook processed for task {task_id}")
                
                return {"status": "ok"}
                
            except Exception as e:
                print(f"❌ Webhook error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

    @property
    def application(self):
        return self.app