import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import config
from handlers import router
from services import WrapperService, TaskManager
from webhook_handler import WebhookHandler

import uvicorn


async def main():
    """Основная функция запуска бота."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Инициализация бота
    bot = Bot(
        token=config.TELEGRAM_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    # Инициализация сервисов с передачей бота
    wrapper_service = WrapperService()
    task_manager = TaskManager(bot)  # Передаем бота
    
    import services
    services.task_manager = task_manager

    # Инициализация диспетчера
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    
    # Инициализация webhook handler с передачей бота
    webhook_handler = WebhookHandler(bot)
    
    async def start_web_server():
        """Запускает FastAPI сервер для webhook"""
        server_config = uvicorn.Config(
            webhook_handler.application,
            host="0.0.0.0",
            port=config.BOT_PORT,
            log_level="info"
        )
        server = uvicorn.Server(server_config)
        await server.serve()
    
    try:
        logging.info("Starting Telegram bot and web server...")
        
        # Запускаем оба сервиса параллельно
        await asyncio.gather(
            dp.start_polling(bot),
            start_web_server(),
            return_exceptions=True
        )
        
    except Exception as e:
        logging.error(f"Bot stopped with error: {e}")
    finally:
        await wrapper_service.client.aclose()
        await bot.session.close()
        logging.info("Bot stopped")


if __name__ == "__main__":
    asyncio.run(main())