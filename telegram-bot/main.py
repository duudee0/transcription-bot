"""Главный модуль приложения."""
import asyncio
import logging
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
    
    logging.info("Application initialized")
    
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
        logging.info("Application shutdown complete")


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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    async with app_lifespan() as app_context:
        bot = app_context['bot']
        
        # Инициализация диспетчера
        dp = Dispatcher(storage=MemoryStorage())
        dp.include_router(router)
        
        # Инициализация webhook handler
        webhook_handler = WebhookHandler()
        
        # Запуск сервисов
        logging.info("Starting application services...")
        await asyncio.gather(
            dp.start_polling(bot),
            start_web_server(webhook_handler),
            return_exceptions=True
        )


if __name__ == "__main__":
    asyncio.run(main())