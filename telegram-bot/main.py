from html import escape
import os
import asyncio
import json
import logging
from typing import Optional, Dict, Any, List, Set
from contextlib import asynccontextmanager

import httpx
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ---------- Конфигурация ----------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN must be set in environment")

WRAPPER_URL = os.getenv("WRAPPER_URL", "http://localhost:8003")
BOT_CALLBACK_HOST = os.getenv("BOT_CALLBACK_HOST", "0.0.0.0")
BOT_CALLBACK_PORT = int(os.getenv("BOT_CALLBACK_PORT", "9000"))
BOT_CALLBACK_HOST_DOCKER = os.getenv("BOT_CALLBACK_HOST_DOCKER", "telegram-bot")

CLIENT_CALLBACK_URL_FOR_WRAPPER = os.getenv(
    "CLIENT_CALLBACK_URL_FOR_WRAPPER",
    f"http://{BOT_CALLBACK_HOST_DOCKER}:{BOT_CALLBACK_PORT}/client/webhook"
)

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))
GLOBAL_TIMEOUT = int(os.getenv("GLOBAL_TIMEOUT", "60"))

# Глобальные переменные для управления задачами поллинга
polling_tasks: Dict[str, asyncio.Task] = {}  # task_id -> polling task
completed_tasks: Set[str] = set()  # task_id которые уже завершены через вебхук

# ---------- Логирование ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tg-wrapper-bot")

# ---------- Состояния FSM (Finite State Machine) ----------
class TaskStates(StatesGroup):
    """Состояния для создания задач через FSM"""
    waiting_for_task_type = State()
    waiting_for_input_data = State()
    waiting_for_parameters = State()

# ---------- Инициализация aiogram ----------
# Используем MemoryStorage для FSM (в продакшене лучше Redis)
storage = MemoryStorage()

# Инициализируем бота
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

# Создаем диспетчер и роутер
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# HTTP клиент для запросов к wrapper
# Создаем на уровне модуля — закроем в lifespan
http_client: Optional[httpx.AsyncClient] = httpx.AsyncClient(timeout=30.0)

# В памяти: map task_id -> list of chat_ids
task_to_chats: Dict[str, List[int]] = {}
task_meta: Dict[str, Dict[str, Any]] = {}

# ---------- Утилиты ----------
def _safe_truncate(text: str, limit: int = 3500) -> str:
    """Безопасное обрезание текста для Telegram"""
    if len(text) <= limit:
        return text
    return text[:limit-200] + "\n\n... (truncated)"

def make_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает основную клавиатуру с командами"""
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/test1"), KeyboardButton(text="/test2")],
            [KeyboardButton(text="/task"), KeyboardButton(text="/mytasks")],
            [KeyboardButton(text="/help")]
        ],
        resize_keyboard=True
    )
    return kb

# --- Helpers to normalize wrapper response ---
def _unwrap_wrapper_response(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводит ответ клиента create_task_on_wrapper к телу (body) если он обёрнут:
      - {"ok": True, "status_code": 200, "body": {...}}
      - {...}  (прямое тело)
      - текст / строка -> {"raw": "<text>"}
    """
    if not isinstance(resp, dict):
        return {"raw": resp}
    if "body" in resp:
        body = resp["body"]
        if isinstance(body, dict):
            return body
        else:
            return {"raw": body}
    # already a body dict
    return resp if isinstance(resp, dict) else {"raw": resp}

def _get_task_id_from_wrapper_response(resp: Dict[str, Any]) -> Optional[str]:
    """Универсально извлекает task_id из разных форм ответов"""
    body = _unwrap_wrapper_response(resp)
    if isinstance(body, dict):
        for key in ("task_id", "id", "taskId"):
            if key in body and body[key]:
                return str(body[key])
    return None
# --- end helpers ---

async def create_task_on_wrapper(
    task_type: str,
    input_data: Dict[str, Any],
    parameters: Optional[Dict[str, Any]] = None,
    service_chain: Optional[List[str]] = None,
    timeout: int = 30,
    client_callback_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Создаёт задачу в wrapper и возвращает стандартную обёртку:
      {"ok": bool, "status_code": int, "body": dict|str|null, "error": str|None}
    """
    global http_client
    payload = {
        "task_type": task_type,
        "input_data": input_data or {},
        "parameters": parameters or {},
        "timeout": timeout
    }
    if service_chain:
        payload["service_chain"] = service_chain
    if client_callback_url:
        # wrapper ожидает поле "callback_url" в запросе от клиента
        payload["callback_url"] = client_callback_url

    url = f"{WRAPPER_URL.rstrip('/')}/api/v1/tasks"
    logger.info("Posting task to wrapper: %s (task_type=%s)", url, task_type)

    try:
        resp = await http_client.post(url, json=payload)
    except Exception as e:
        logger.exception("Network error posting to wrapper: %s", e)
        return {"ok": False, "status_code": None, "body": None, "error": f"network_error: {e}"}

    status = resp.status_code
    body = None
    try:
        body = resp.json()
    except Exception:
        # если wrapper вернул не-json
        try:
            body = (await resp.aread()).decode(errors="ignore")
        except Exception:
            body = resp.text if hasattr(resp, "text") else None

    if 200 <= status < 300:
        logger.info("Wrapper returned %s: %s", status, body)
        return {"ok": True, "status_code": status, "body": body, "error": None}
    else:
        logger.warning("Wrapper error %s: %s", status, body)
        return {"ok": False, "status_code": status, "body": body, "error": f"wrapper_status_{status}"}

async def poll_task_result(task_id: str, poll_timeout: int = 30) -> Dict[str, Any]:
    """
    Поллинг результата задачи с wrapper.
    Возвращает словарь: {task_id, status, result, error}
    """
    global http_client
    url = f"{WRAPPER_URL.rstrip('/')}/api/v1/tasks/{task_id}"
    start = asyncio.get_event_loop().time()
    while True:
        try:
            resp = await http_client.get(url)
        except Exception as e:
            logger.warning("Error requesting wrapper status for %s: %s", task_id, e)
            # короткий бэофф, потом повтор
            await asyncio.sleep(POLL_INTERVAL)
            if asyncio.get_event_loop().time() - start > poll_timeout:
                return {"task_id": task_id, "status": "timeout", "result": None, "error": "network_error"}
            continue

        # parse
        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception:
                logger.warning("Non-json response from wrapper for %s: %s", task_id, resp.text)
                data = {"status": "unknown", "result": None}

            status = data.get("status")
            if status in ("completed", "error", "timeout"):
                return {
                    "task_id": task_id,
                    "status": status,
                    "result": data.get("result"),
                    "error": data.get("error")
                }
            # still processing -> loop
        else:
            logger.warning("Wrapper returned %s for task %s", resp.status_code, task_id)

        # timeout check
        if asyncio.get_event_loop().time() - start > poll_timeout:
            return {"task_id": task_id, "status": "timeout", "result": None, "error": "poll_timeout"}

        await asyncio.sleep(POLL_INTERVAL)

# ---------- FastAPI эндпоинты (lifespan и client webhook) ----------
from fastapi import FastAPI, Request, HTTPException

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan для управления ресурсами FastAPI"""
    global http_client
    logger.info("Starting FastAPI application")
    # ensure http_client exists
    if http_client is None:
        http_client = httpx.AsyncClient(timeout=30.0)
    yield
    # Shutdown
    await http_client.aclose()
    http_client = None
    logger.info("FastAPI application shutdown")

app = FastAPI(title="TG Wrapper Bot Server", lifespan=lifespan)

@app.post("/client/webhook")
async def client_webhook(request: Request):
    """
    Эндпоинт для callback'ов от wrapper'а (client callbacks).
    Ожидаем тело: {"task_id": "...", "status": "...", "result": ..., "error": ...}
    """
    try:
        payload = await request.json()
    except Exception as e:
        logger.error("Invalid JSON in client webhook: %s", e)
        raise HTTPException(status_code=400, detail="invalid json")

    task_id = payload.get("task_id")
    if not task_id:
        logger.warning("Webhook missing task_id")
        raise HTTPException(status_code=400, detail="missing task_id")

    status = payload.get("status")
    result = payload.get("result")
    error = payload.get("error")

    logger.info("Received client webhook for %s status=%s", task_id, status)

    # Mark as completed (so poll fallback stops)
    completed_tasks.add(task_id)

    # cancel polling task if present (safe cancel)
    polling_task = polling_tasks.pop(task_id, None)
    if polling_task:
        if not polling_task.done():
            polling_task.cancel()
            try:
                await polling_task
            except asyncio.CancelledError:
                logger.debug("Polling task cancelled for %s", task_id)
            except Exception as e:
                logger.warning("Error while cancelling polling task for %s: %s", task_id, e)

    # deliver message to mapped chats if any
    chats = task_to_chats.get(task_id, [])
    if not chats:
        logger.info("No chat mapping for task %s (client webhook received)", task_id)
        return {"status": "no_mapping"}

    # prepare message text
    text = f"📬 Результат задачи {task_id}:\nStatus: {status}\n"
    if error:
        text += f"Error: {error}\n"
    if result is not None:
        pretty = json.dumps(result, ensure_ascii=False, indent=2)
        pretty = _safe_truncate(pretty, 3500)
        text += f"\nResult:\n<pre>{escape(pretty)}</pre>"

    # send messages asynchronously
    for chat_id in chats:
        asyncio.create_task(bot.send_message(chat_id, text, parse_mode=ParseMode.HTML))

    # store last webhook for the task
    task_meta.setdefault(task_id, {})["last_webhook"] = payload
    return {"status": "delivered"}

# ---------- Обработчики Telegram (aiogram) ----------
@router.message(CommandStart())
@router.message(Command("help"))
async def handle_start(message: Message):
    """Обработчик команд /start и /help"""
    txt = (
        "Привет! Я бот-интерфейс к Task API Wrapper.\n\n"
        "Можешь отправить задачу в ручном формате:\n"
        "/task 'task_type' 'json_input_data' ['json_parameters']\n\n"
        "Или воспользуйся тестовыми кнопками ниже.\n\n"
        "Пример ручной команды:\n"
        "/task analyze_text {\"text\":\"Привет мир\"} {\"detailed_analysis\":true}\n"
    )
    await message.answer(txt, reply_markup=make_main_keyboard(), parse_mode='HTML')

@router.message(Command("ollama"))
async def handle_test1(message: Message):
    """Обработчик текста который шлется в ламу"""

    request = message.text.removeprefix("/ollama")
    if not request:
        await message.answer(f"Вы не передали команде запрос!")
        return
    
    chat_id = message.chat.id
    task_type = "local-llm"
    input_data = {"text": request, "language": "ru"}
    parameters = {"detailed_analysis": True}
    service_chain = ["local-llm"]

    info_msg = await message.answer("Передаю ламе ваш запрос...")
    
    try:
        resp = await create_task_on_wrapper(
            task_type=task_type,
            input_data=input_data,
            parameters=parameters,
            service_chain=service_chain,
            timeout=GLOBAL_TIMEOUT,
            client_callback_url=CLIENT_CALLBACK_URL_FOR_WRAPPER
        )
    except Exception as e:
        logger.exception("Failed to create test1 task: %s", e)
        await message.answer(f"Ошибка при создании задачи: {e}")
        return

    task_id = _get_task_id_from_wrapper_response(resp)
    if not task_id:
        logger.warning("Wrapper returned unexpected response while creating task: %s", resp)
        await message.answer(f"Wrapper ответил без task_id: {resp}")
        return

    # Сохраняем mapping task->chat
    task_to_chats.setdefault(task_id, []).append(chat_id)
    task_meta.setdefault(task_id, {}).update({"type": task_type, "created_by": chat_id})

    # Запускаем polling fallback и сохраняем ссылку на задачу
    polling_task = asyncio.create_task(poll_fallback(task_id, chat_id, GLOBAL_TIMEOUT))
    polling_tasks[task_id] = polling_task

    await info_msg.edit_text("Тестовая задача отправлена. Ожидаю результат (вы получите push, когда wrapper пришлёт callback).")

@router.message(Command("test1"))
async def handle_test1(message: Message):
    """Обработчик тестовой задачи 1 - анализ текста"""
    chat_id = message.chat.id
    task_type = "analyze_text"
    input_data = {"text": message.text, "language": "ru"}
    parameters = {"detailed_analysis": True}
    service_chain = ["llm-service"]

    info_msg = await message.answer("Запускаю тестовую задачу 1 (анализ текста)...")
    
    try:
        resp = await create_task_on_wrapper(
            task_type=task_type,
            input_data=input_data,
            parameters=parameters,
            service_chain=service_chain,
            timeout=GLOBAL_TIMEOUT*30,
            client_callback_url=CLIENT_CALLBACK_URL_FOR_WRAPPER
        )
    except Exception as e:
        logger.exception("Failed to create test1 task: %s", e)
        await message.answer(f"Ошибка при создании задачи: {e}")
        return

    task_id = _get_task_id_from_wrapper_response(resp)
    if not task_id:
        logger.warning("Wrapper returned unexpected response while creating task: %s", resp)
        await message.answer(f"Wrapper ответил без task_id: {resp}")
        return

    # Сохраняем mapping task->chat
    task_to_chats.setdefault(task_id, []).append(chat_id)
    task_meta.setdefault(task_id, {}).update({"type": task_type, "created_by": chat_id})

    # Запускаем polling fallback и сохраняем ссылку на задачу
    polling_task = asyncio.create_task(poll_fallback(task_id, chat_id, GLOBAL_TIMEOUT))
    polling_tasks[task_id] = polling_task

    await info_msg.edit_text("Тестовая задача отправлена. Ожидаю результат (вы получите push, когда wrapper пришлёт callback).")

@router.message(Command("test2"))
async def handle_test2(message: Message):
    """Обработчик тестовой задачи 2 - генерация ответа"""
    chat_id = message.chat.id
    task_type = "generate_response"
    input_data = {"prompt": "Придумай смешной твит про программистов."}
    parameters = {"max_tokens": 80}
    service_chain = ["gigachat-service"]

    info_msg = await message.answer("Запускаю тестовую задачу 2 (генерация ответа)...")
    
    try:
        resp = await create_task_on_wrapper(
            task_type=task_type,
            input_data=input_data,
            parameters=parameters,
            service_chain=service_chain,
            timeout=GLOBAL_TIMEOUT,
            client_callback_url=CLIENT_CALLBACK_URL_FOR_WRAPPER
        )
    except Exception as e:
        logger.exception("Failed to create test2 task: %s", e)
        await message.answer(f"Ошибка при создании задачи: {e}")
        return

    task_id = _get_task_id_from_wrapper_response(resp)
    if not task_id:
        await message.answer(f"Wrapper ответил без task_id: {resp}")
        return

    task_to_chats.setdefault(task_id, []).append(chat_id)
    task_meta.setdefault(task_id, {}).update({"type": task_type, "created_by": chat_id})

    # Запускаем polling fallback и сохраняем ссылку на задачу
    polling_task = asyncio.create_task(poll_fallback(task_id, chat_id, GLOBAL_TIMEOUT))
    polling_tasks[task_id] = polling_task

    await info_msg.edit_text("Тестовая задача отправлена. Ожидаю результат (push будет отправлен при callback от wrapper).")

@router.message(Command("task"))
async def handle_task_command(message: Message, state: FSMContext):
    """
    Обработчик команды /task - начинает процесс создания задачи через FSM
    """
    await message.answer(
        "Давайте создадим задачу. Введите тип задачи (например, 'analyze_text'):",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(TaskStates.waiting_for_task_type)

@router.message(TaskStates.waiting_for_task_type)
async def handle_task_type(message: Message, state: FSMContext):
    """Получаем тип задачи и запрашиваем input_data"""
    await state.update_data(task_type=message.text.strip())
    await message.answer("Отлично! Теперь введите input_data в формате JSON:")
    await state.set_state(TaskStates.waiting_for_input_data)

@router.message(TaskStates.waiting_for_input_data)
async def handle_input_data(message: Message, state: FSMContext):
    """Получаем input_data и запрашиваем parameters (опционально)"""
    try:
        input_data = json.loads(message.text)
    except json.JSONDecodeError:
        await message.answer("Неверный формат JSON. Попробуйте еще раз:")
        return

    await state.update_data(input_data=input_data)
    await message.answer("Введите parameters в формате JSON (или отправьте 'skip' для пропуска):")
    await state.set_state(TaskStates.waiting_for_parameters)

@router.message(TaskStates.waiting_for_parameters)
async def handle_parameters(message: Message, state: FSMContext):
    """Получаем parameters и создаем задачу"""
    user_data = await state.get_data()
    
    # Обрабатываем parameters
    parameters = {}
    if message.text.lower() != 'skip':
        try:
            parameters = json.loads(message.text)
        except json.JSONDecodeError:
            await message.answer("Неверный формат JSON. Задача будет создана без parameters.")
    
    # Создаем задачу
    status_msg = await message.answer("Отправляю задачу в wrapper...")
    
    try:
        wrapper_resp = await create_task_on_wrapper(
            task_type=user_data['task_type'],
            input_data=user_data['input_data'],
            parameters=parameters,
            timeout=GLOBAL_TIMEOUT,
            client_callback_url=CLIENT_CALLBACK_URL_FOR_WRAPPER
        )
    except Exception as e:
        logger.exception("Failed to create task: %s", e)
        await message.answer(f"Ошибка при создании задачи: {e}")
        await state.clear()
        return

    task_id = _get_task_id_from_wrapper_response(wrapper_resp)
    if not task_id:
        await message.answer(f"Wrapper ответил без task_id: {wrapper_resp}")
        await state.clear()
        return

    # Сохраняем mapping и мета
    task_to_chats.setdefault(task_id, []).append(message.chat.id)
    task_meta.setdefault(task_id, {}).update({
        "type": user_data['task_type'], 
        "created_by": message.chat.id
    })

    await status_msg.edit_text(
        f"Задача отправлена, task_id: {task_id}. "
        f"Результат придёт сюда при callback от wrapper (push)."
    )

    # Запускаем polling fallback и сохраняем ссылку на задачу
    polling_task = asyncio.create_task(poll_fallback(task_id, message.chat.id, GLOBAL_TIMEOUT))
    polling_tasks[task_id] = polling_task
    
    await state.clear()
    await message.answer("Что дальше?", reply_markup=make_main_keyboard())

async def poll_fallback(task_id: str, chat_id: int, timeout: int):
    """
    Fallback polling на случай если wrapper не пришлет callback
    С проверкой, не пришел ли уже вебхук
    """
    try:
        # Проверяем, не пришел ли уже вебхук для этой задачи
        if task_id in completed_tasks:
            logger.info("Skipping polling for %s - already completed via webhook", task_id)
            return

        start_time = asyncio.get_event_loop().time()
        
        while True:
            # Еще раз проверяем перед каждым запросом
            if task_id in completed_tasks:
                logger.info("Polling cancelled for %s - webhook received", task_id)
                return

            # Проверяем таймаут
            if asyncio.get_event_loop().time() - start_time > timeout:
                await bot.send_message(chat_id, f"⏰ (poll) Таймаут ожидания результата для {escape(task_id)}.")
                break

            try:
                status_obj = await poll_task_result(task_id=task_id, poll_timeout=5)  # короткий таймаут на итерацию
                st = status_obj.get("status")
                
                if st == "completed":
                    result = status_obj.get("result") or {}
                    pretty = _safe_truncate(json.dumps(result, ensure_ascii=False, indent=2), 3500)
                    pretty_escaped = escape(pretty)
                    await bot.send_message(
                        chat_id, 
                        f"✅ (poll) Задача {escape(task_id)} выполнена:\n<pre>{pretty_escaped}</pre>",
                        parse_mode=ParseMode.HTML
                    )
                    break
                elif st == "error":
                    err = status_obj.get("error") or "unknown"
                    await bot.send_message(
                        chat_id, 
                        f"❌ (poll) Задача {escape(task_id)} завершилась с ошибкой: {escape(err)}"
                    )
                    break
                elif st == "timeout":
                    await bot.send_message(
                        chat_id, 
                        f"⏰ (poll) Таймаут выполнения задачи {escape(task_id)}."
                    )
                    break
                else:
                    # Задача еще выполняется, продолжаем поллинг
                    await asyncio.sleep(POLL_INTERVAL)
                    
            except asyncio.CancelledError:
                # Задача была отменена (вероятно, пришел вебхук)
                logger.info("Polling task for %s was cancelled", task_id)
                return
            except Exception as e:
                logger.warning("Error during polling for %s: %s", task_id, e)
                await asyncio.sleep(POLL_INTERVAL)  # Ждем перед повторной попыткой

    except asyncio.CancelledError:
        logger.info("Polling task for %s was cancelled", task_id)
    except Exception as e:
        logger.exception("Error in poll fallback for %s: %s", task_id, e)
        await bot.send_message(chat_id, f"Ошибка при polling для {task_id}: {e}")
    finally:
        # Очищаем ресурсы
        polling_tasks.pop(task_id, None)
        # Не очищаем completed_tasks сразу, они могут пригодиться для повторных проверок

@router.message(Command("mytasks"))
async def handle_mytasks(message: Message):
    """Показывает задачи пользователя"""
    chat_id = message.chat.id
    tasks = [tid for tid, chats in task_to_chats.items() if chat_id in chats]
    if not tasks:
        await message.answer("У вас нет запущенных задач (в текущей сессии).")
        return
    
    out_lines = []
    for tid in tasks:
        meta = task_meta.get(tid, {})
        out_lines.append(f"{tid} — type={meta.get('type','?')}")
    
    await message.answer("Ваши задачи (локальная привязка):\n" + "\n".join(out_lines))

async def cleanup_old_tasks():
    """Периодически очищает старые завершенные задачи чтобы избежать утечек памяти"""
    while True:
        await asyncio.sleep(3600)  # Каждый час
        # Пока просто ограничим размер completed_tasks
        if len(completed_tasks) > 1000:
            tasks_list = list(completed_tasks)
            for task_id in tasks_list[:-500]:
                completed_tasks.discard(task_id)
                task_meta.pop(task_id, None)
                task_to_chats.pop(task_id, None)
            logger.info("Cleaned up old completed tasks")

# ---------- Запуск приложения ----------
async def run_fastapi():
    """Запускает FastAPI сервер"""
    import uvicorn
    config = uvicorn.Config(
        app, 
        host=BOT_CALLBACK_HOST, 
        port=BOT_CALLBACK_PORT, 
        log_level="info"
    )
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    """Основная функция запуска"""
    logger.info("Starting combined FastAPI + Aiogram bot...")
    
    # Запускаем обе службы параллельно
    fastapi_task = asyncio.create_task(run_fastapi())
    bot_task = asyncio.create_task(dp.start_polling(bot))
    cleanup_tasks = asyncio.create_task(cleanup_old_tasks())
    
    try:
        await asyncio.gather(fastapi_task, bot_task, cleanup_tasks)
    except KeyboardInterrupt:
        logger.info("Shutdown requested by KeyboardInterrupt")
    finally:
        # Корректное завершение
        await bot.session.close()
        global http_client
        if http_client:
            await http_client.aclose()

if __name__ == "__main__":
    asyncio.run(main())
