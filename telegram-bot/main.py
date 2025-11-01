"""Главный модуль приложения."""
import asyncio
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

import uvicorn

from config import config
from handlers import router
from dependencies import ServiceContainer
from webhook_handler import WebhookHandler

from logger import get_logger, setup_logger

# Инициализируем логгер
logger = get_logger(__name__)

@asynccontextmanager
async def app_lifespan():
    """Управление жизненным циклом приложения."""
    # Инициализация
    bot = Bot(
        token=config.TELEGRAM_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    # Инициализируем контейнер сервисов
    container = ServiceContainer.get_instance()
    container.bot = bot
    
    logger.info("Application initialized", extra={"component": "main"})
    
    try:
        yield {
            'bot': bot,
            'container': container
        }
    finally:
        # Завершение работы
        await bot.session.close()
        if container.task_manager:
            await container.task_manager.close()
        logger.info("Application shutdown complete", extra={"component": "main"})


async def start_web_server(webhook_handler: WebhookHandler) -> None:
    """Запускает FastAPI сервер для webhook."""
    server_config = uvicorn.Config(
        webhook_handler.application,
        host="0.0.0.0",
        port=config.BOT_PORT,
        log_level="info"
    )
    server = uvicorn.Server(server_config)
    await server.serve()


async def main() -> None:
    """Основная функция приложения."""
    # Настройка логирования
    setup_logger(
        name="app",
        log_level=config.LOG_LEVEL,
        log_file=config.LOG_FILE,
        format_string=config.LOG_FORMAT
    )
    
    async with app_lifespan() as app_context:
        bot = app_context['bot']
        
        # Инициализация диспетчера
        dp = Dispatcher(storage=MemoryStorage())
        dp.include_router(router)
        
        # Инициализация webhook handler
        webhook_handler = WebhookHandler()
        
        # Запуск сервисов
        logger.info(
            "Starting application", 
            extra={
                "log_level": config.LOG_LEVEL,
                "log_file": config.LOG_FILE
            }
        )
        await asyncio.gather(
            dp.start_polling(bot),
            start_web_server(webhook_handler),
            return_exceptions=True
        )


if __name__ == "__main__":
    asyncio.run(main())