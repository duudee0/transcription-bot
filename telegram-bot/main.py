import os
import asyncio
import json
import logging
from typing import Optional, Dict, Any, List
import httpx
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from telebot.async_telebot import AsyncTeleBot
from telebot.types import Message, ReplyKeyboardMarkup, KeyboardButton

# ---------- Конфигурация (без плейсхолдеров, устанавливайте через env) ----------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN must be set in environment")

# URL на ваш wrapper (тот, который вы показывали в коде)
WRAPPER_URL = os.getenv("WRAPPER_URL", "http://localhost:8003")

# Где будет слушать HTTP сервер бота (для wrapper'а нужен доступ к этому хосту)
# Для локальной разработки: BOT_CALLBACK_HOST="localhost", BOT_CALLBACK_PORT=9000
# Для Docker-сети: укажите имя контейнера здесь в BOT_CALLBACK_HOST_DOCKER (например "bot-wrapper")
BOT_CALLBACK_HOST = os.getenv("BOT_CALLBACK_HOST", "0.0.0.0")
BOT_CALLBACK_PORT = int(os.getenv("BOT_CALLBACK_PORT", "9000"))
BOT_CALLBACK_HOST_DOCKER = os.getenv("BOT_CALLBACK_HOST_DOCKER", "telegram-bot")  # имя контейнера/hostname внутри docker-net

# Адрес, который мы передаём в wrapper как callback_url (wrapper будет POSTить туда результат)
# Wrapper в вашем коде вызывает client_callback_url напрямую, передавая объект {"task_id":..., "status":..., ...}
CLIENT_CALLBACK_URL_FOR_WRAPPER = os.getenv(
    "CLIENT_CALLBACK_URL_FOR_WRAPPER",
    f"http://{BOT_CALLBACK_HOST_DOCKER}:{BOT_CALLBACK_PORT}/client/webhook"
)

# Polling / timeout settings
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))  # seconds between polls
GLOBAL_TIMEOUT = int(os.getenv("GLOBAL_TIMEOUT", "60"))  # seconds max wait when polling

# ---------- Логирование ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tg-wrapper-bot")

# ---------- HTTP и Telegram объекты ----------
app = FastAPI(title="TG Wrapper Bot Server")
bot = AsyncTeleBot(TELEGRAM_TOKEN)
http_client = httpx.AsyncClient(timeout=30.0)

# В памяти: map task_id -> list of chat_ids (поддержка нескольким пользователей, если нужно)
task_to_chats: Dict[str, List[int]] = {}
# Сервисы состояния для удобства (task info)
task_meta: Dict[str, Dict[str, Any]] = {}  # task_id -> info like {'type':..., 'created_by': chat_id}

# ---------- Утилиты ----------
def _safe_truncate(text: str, limit: int = 3500) -> str:
    if len(text) <= limit:
        return text
    return text[:limit-200] + "\n\n... (truncated)"

async def create_task_on_wrapper(
    task_type: str,
    input_data: Dict[str, Any],
    parameters: Optional[Dict[str, Any]] = None,
    service_chain: Optional[List[str]] = None,
    timeout: int = 30,
    client_callback_url: Optional[str] = None
) -> Dict[str, Any]:
    """Создаёт задачу в wrapper; отдаёт JSON-ответ wrapper'а."""
    payload = {
        "task_type": task_type,
        "input_data": input_data or {},
        "parameters": parameters or {},
        "timeout": timeout
    }
    if service_chain:
        payload["service_chain"] = service_chain
    # Указываем callback_url, чтобы wrapper звонил нам напрямую (если указан)
    if client_callback_url:
        payload["callback_url"] = client_callback_url

    url = f"{WRAPPER_URL.rstrip('/')}/api/v1/tasks"
    logger.info("Posting task to wrapper: %s", url)
    resp = await http_client.post(url, json=payload)
    resp.raise_for_status()
    return resp.json()

async def poll_task_result(task_id: str, timeout: int) -> Dict[str, Any]:
    url = f"{WRAPPER_URL.rstrip('/')}/api/v1/tasks/{task_id}"
    start = asyncio.get_event_loop().time()
    while True:
        try:
            resp = await http_client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                status = data.get("status")
                if status in ("completed", "error", "timeout"):
                    return data
            else:
                logger.warning("Wrapper returned %s for task %s", resp.status_code, task_id)
        except Exception as e:
            logger.exception("Error while polling wrapper for task %s: %s", task_id, e)
        if asyncio.get_event_loop().time() - start > timeout:
            return {"task_id": task_id, "status": "timeout", "error": "local_poll_timeout", "result": None}
        await asyncio.sleep(POLL_INTERVAL)

# ---------- FastAPI endpoint для callback'ов от wrapper (к нам) ----------
@app.post("/client/webhook")
async def client_webhook(request: Request):
    """
    Wrapper вызовет этот endpoint (client_callback_url), передавая JSON:
    {
      "task_id": "...",
      "status": "completed" | "error" | ...,
      "result": {...},
      "error": "..."
    }
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

    # Найдём чаты, ожидающие этот task_id
    chats = task_to_chats.get(task_id, [])
    if not chats:
        logger.warning("No chat mapping for task %s (webhook ignored)", task_id)
        return {"status": "no_mapping"}

    # Отправляем каждому пользователю результат
    text = f"📬 Результат задачи {task_id} (push from wrapper):\n\nStatus: {status}\n"
    if error:
        text += f"Error: {error}\n"
    if result is not None:
        pretty = json.dumps(result, ensure_ascii=False, indent=2)
        pretty = _safe_truncate(pretty, 3500)
        text += f"\nResult:\n<pre>{pretty}</pre>"

    # Отправка (не блокируем основной поток — создаём задачи)
    for chat_id in chats:
        asyncio.create_task(bot.send_message(chat_id, text, parse_mode="HTML"))

    # Можно пометить мета-инфо
    task_meta.setdefault(task_id, {})["last_webhook"] = payload
    return {"status": "delivered"}

# ---------- Telegram handlers ----------
# Кнопочная клавиатура с двумя тестовыми задачами и быстрыми командами
def make_main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("/test1"), KeyboardButton("/test2"))
    kb.add(KeyboardButton("/task"), KeyboardButton("/mytasks"))
    kb.add(KeyboardButton("/help"))
    return kb

@bot.message_handler(commands=["start", "help"])
async def handle_start(message: Message):
    txt = (
        "Привет! Я бот-интерфейс к Task API Wrapper.\n\n"
        "Можешь отправить задачу в ручном формате:\n"
        "/task <task_type> <json_input_data> [<json_parameters>]\n\n"
        "Или воспользуйся тестовыми кнопками ниже.\n\n"
        "Пример ручной команды:\n"
        "/task analyze_text {\"text\":\"Привет мир\"} {\"detailed_analysis\":true}\n"
    )
    await bot.send_message(message.chat.id, txt, reply_markup=make_main_keyboard())

@bot.message_handler(commands=["test1"])
async def handle_test1(message: Message):
    """
    Дружелюбная тестовая задача 1 — анализ текста (использует service_chain как пример).
    """
    chat_id = message.chat.id
    task_type = "analyze_text"
    input_data = {"text": "Это тест от Telegram-бота: проверьте работу анализа текста.", "language": "ru"}
    parameters = {"detailed_analysis": True}
    service_chain = ["llm-service"]

    info_msg = await bot.send_message(chat_id, "Запускаю тестовую задачу 1 (анализ текста)...")
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
        await bot.send_message(chat_id, f"Ошибка при создании задачи: {e}")
        return

    task_id = resp.get("task_id")
    if not task_id:
        await bot.send_message(chat_id, f"Wrapper ответил без task_id: {resp}")
        return

    # Сохраняем mapping task->chat
    task_to_chats.setdefault(task_id, []).append(chat_id)
    task_meta.setdefault(task_id, {}).update({"type": task_type, "created_by": chat_id})

    await bot.edit_message_text("Тестовая задача отправлена. Ожидаю результат (вы получите push, когда wrapper пришлёт callback).", chat_id, info_msg.message_id)

@bot.message_handler(commands=["test2"])
async def handle_test2(message: Message):
    """
    Дружелюбная тестовая задача 2 — генерация ответа (пример с service_chain).
    """
    chat_id = message.chat.id
    task_type = "generate_response"
    input_data = {"prompt": "Придумай смешной твит про программистов."}
    parameters = {"max_tokens": 80}
    service_chain = ["gigachat-service"]

    info_msg = await bot.send_message(chat_id, "Запускаю тестовую задачу 2 (генерация ответа)...")
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
        await bot.send_message(chat_id, f"Ошибка при создании задачи: {e}")
        return

    task_id = resp.get("task_id")
    if not task_id:
        await bot.send_message(chat_id, f"Wrapper ответил без task_id: {resp}")
        return

    task_to_chats.setdefault(task_id, []).append(chat_id)
    task_meta.setdefault(task_id, {}).update({"type": task_type, "created_by": chat_id})

    await bot.edit_message_text("Тестовая задача отправлена. Ожидаю результат (push будет отправлен при callback от wrapper).", chat_id, info_msg.message_id)

@bot.message_handler(commands=["task"])
async def handle_task(message: Message):
    """
    Ожидаемый формат:
    /task <task_type> <input_data_json> [parameters_json]
    (Если не хотите polling — передаём callback_url явно, иначе бот будет poll'ить по-старому.)
    """
    chat_id = message.chat.id
    text = message.text or ""
    # простой парсинг: как в предыдущей версии — гибкий экстракт JSON'ов
    try:
        rest = text[len("/task"):].strip()
        first_space = rest.find(" ")
        if first_space == -1:
            await bot.send_message(chat_id, "Нужно указать task_type и input_data JSON. Смотрите /help.")
            return
        task_type = rest[:first_space].strip()
        remainder = rest[first_space+1:].strip()

        def extract_json_prefix(s: str):
            s = s.lstrip()
            if not s:
                return None, s
            if s[0] not in ('{','['):
                return None, s
            open_ch = s[0]
            close_ch = '}' if open_ch == '{' else ']'
            depth = 0
            for i, ch in enumerate(s):
                if ch == open_ch:
                    depth += 1
                elif ch == close_ch:
                    depth -= 1
                    if depth == 0:
                        return s[:i+1], s[i+1:].strip()
            return None, s

        json1_str, tail = extract_json_prefix(remainder)
        if not json1_str:
            await bot.send_message(chat_id, "Не получилось распознать JSON input_data.")
            return
        input_data = json.loads(json1_str)
        parameters = {}
        if tail:
            j2, _ = extract_json_prefix(tail)
            if j2:
                parameters = json.loads(j2)
    except json.JSONDecodeError as e:
        await bot.send_message(chat_id, f"JSON parse error: {e}")
        return
    except Exception as e:
        logger.exception("Error parsing /task: %s", e)
        await bot.send_message(chat_id, f"Ошибка парсинга команды: {e}")
        return

    # опционально берем service_chain из input_data или parameters
    service_chain = input_data.get("service_chain") or parameters.get("service_chain")
    timeout = int(parameters.get("timeout", GLOBAL_TIMEOUT))

    status_msg = await bot.send_message(chat_id, f"Отправляю задачу '{task_type}' в wrapper...")
    try:
        # Передаём client_callback_url — чтобы wrapper звонил напрямую; если не хотите, можно убрать
        wrapper_resp = await create_task_on_wrapper(
            task_type=task_type,
            input_data=input_data,
            parameters=parameters,
            service_chain=service_chain,
            timeout=timeout,
            client_callback_url=CLIENT_CALLBACK_URL_FOR_WRAPPER
        )
    except httpx.HTTPStatusError as e:
        logger.exception("Wrapper returned error: %s", e)
        await bot.send_message(chat_id, f"Ошибка от wrapper: {e.response.status_code} {e.response.text}")
        return
    except Exception as e:
        logger.exception("Failed to send task to wrapper: %s", e)
        await bot.send_message(chat_id, f"Не удалось создать задачу: {e}")
        return

    task_id = wrapper_resp.get("task_id")
    if not task_id:
        await bot.send_message(chat_id, f"Wrapper ответил без task_id: {wrapper_resp}")
        return

    # Сохраняем mapping и мета
    task_to_chats.setdefault(task_id, []).append(chat_id)
    task_meta.setdefault(task_id, {}).update({"type": task_type, "created_by": chat_id})

    # По умолчанию — отправляем пользователю сообщение и говорим ждать push (callback).
    await bot.edit_message_text(f"Задача отправлена, task_id: {task_id}. Результат придёт сюда при callback от wrapper (push).", chat_id, status_msg.message_id)

    # Также запустим опциональный polling в фоне как fallback — если wrapper не пришлёт callback.
    async def poll_fallback():
        try:
            status_obj = await poll_task_result(task_id=task_id, timeout=timeout)
            st = status_obj.get("status")
            if st == "completed":
                result = status_obj.get("result") or {}
                pretty = _safe_truncate(json.dumps(result, ensure_ascii=False, indent=2), 3500)
                await bot.send_message(chat_id, f"✅ (poll) Задача {task_id} выполнена:\n<pre>{pretty}</pre>", parse_mode="HTML")
            elif st == "error":
                err = status_obj.get("error") or "unknown"
                await bot.send_message(chat_id, f"❌ (poll) Задача {task_id} завершилась с ошибкой: {err}")
            elif st == "timeout":
                await bot.send_message(chat_id, f"⏰ (poll) Таймаут ожидания результата для {task_id}.")
            else:
                await bot.send_message(chat_id, f"(poll) Статус задачи {task_id}: {st}.")
        except Exception as e:
            logger.exception("Error in poll fallback for %s: %s", task_id, e)
            await bot.send_message(chat_id, f"Ошибка при polling для {task_id}: {e}")

    # Запускаем polling fallback, не блокируя
    asyncio.create_task(poll_fallback())

@bot.message_handler(commands=["mytasks"])
async def handle_mytasks(message: Message):
    chat_id = message.chat.id
    tasks = [tid for tid, chats in task_to_chats.items() if chat_id in chats]
    if not tasks:
        await bot.send_message(chat_id, "У вас нет запущенных задач (в текущей сессии).")
        return
    out_lines = []
    for tid in tasks:
        meta = task_meta.get(tid, {})
        out_lines.append(f"{tid} — type={meta.get('type','?')}")
    await bot.send_message(chat_id, "Ваши задачи (локальная привязка):\n" + "\n".join(out_lines))

# ---------- Запуск: запускаем и FastAPI и Telegram polling в одном процессе ----------
async def run_uvicorn():
    """Запускает uvicorn server программно (awaitable)."""
    config = uvicorn.Config(app, host=BOT_CALLBACK_HOST, port=BOT_CALLBACK_PORT, log_level="info")
    server = uvicorn.Server(config)
    # serve() завершится только при остановке сервера
    await server.serve()

async def main():
    logger.info("Starting combined FastAPI + Telegram bot...")
    # Запускаем uvicorn сервер в фоне и polling бота
    server_task = asyncio.create_task(run_uvicorn())
    try:
        # Запускаем бота polling (blocking but awaitable)
        await bot.polling(non_stop=True)
    finally:
        # При выходе закрываем http клиент и останавливаем сервер
        await http_client.aclose()
        # Остановим uvicorn (если ещё жив)
        if not server_task.done():
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by KeyboardInterrupt")
