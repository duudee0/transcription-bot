import asyncio
from aiogram import Bot, Dispatcher

from config import TOKEN
from app.handlers import router
from app.database import db

bot = Bot(token=TOKEN)
dp = Dispatcher()

async def on_startup():
    await db.create_pool()

async def main():
    dp.startup.register(on_startup)
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Exit')
