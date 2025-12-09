"""Обработчики Telegram бота."""
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext

from config import config, TextCommands
from models import TaskCreationState, ServiceSelectionState
from dependencies import ServiceContainer, get_task_manager
from keyboards import (
    get_main_keyboard, get_llm_service_keyboard, 
    get_audio_service_keyboard, get_service_chain_keyboard,
    get_cancel_keyboard
)
from utils import (
    get_task_type_by_name, validate_json, 
    validate_text_length, format_task_status
)
from logger import get_logger

# Инициализируем логгер для модуля
logger = get_logger(__name__)

router = Router()


async def _send_welcome_message(message: Message) -> None:
    """Отправляет приветственное сообщение."""
    welcome_text = (
        "🤖 <b>Добро пожаловать в AI Assistant!</b>\n\n"
        "Я могу обрабатывать текст и аудио через различные AI сервисы.\n"
        "Вы можете выбрать готовую задачу или создать свою цепочку обработки!\n\n"
        "<b>Доступные возможности:</b>\n"
        "• 📝 Анализ текста (разные модели на выбор)\n"
        "• 🎤 Транскрибация аудио\n" 
        "• 🔊 Озвучка текста\n"
        "• 💬 Голосовой чат (аудио → текст → ИИ → аудио)\n"
        "• ⚙️ Свои цепочки сервисов\n\n"
        "Выберите действие ниже 👇"
    )
    
    await message.answer(welcome_text, reply_markup=get_main_keyboard())


async def _send_help_message(message: Message) -> None:
    """Отправляет сообщение помощи."""
    help_text = (
        "🆘 <b>Помощь по использованию бота</b>\n\n"
        "<b>Основные команды:</b>\n"
        "/start - Главное меню\n"
        "/help - Эта справка\n"
        "/tasks - Мои задачи\n"
        "/cancel - Отмена текущей операции\n\n"
        "<b>Как использовать:</b>\n"
        "1. Выберите тип задачи из меню\n"
        "2. При необходимости выберите сервис\n"
        "3. Отправьте текст или аудио\n"
        "4. Получите результат!\n\n"
        "⏱ Время обработки зависит от выбранных сервисов."
    )
    
    await message.answer(help_text)


@router.message(CommandStart())
async def handle_start(message: Message, state: FSMContext) -> None:
    """Обработчик команды /start."""
    await state.clear()
    await _send_welcome_message(message)


@router.message(Command("help"))
@router.message(F.text == TextCommands.HELP)
async def handle_help(message: Message) -> None:
    """Обработчик команды /help."""
    await _send_help_message(message)


@router.message(Command("tasks"))
@router.message(F.text == TextCommands.MY_TASK)
async def handle_tasks(message: Message) -> None:
    """Показывает задачи пользователя."""
    try:
        task_manager = get_task_manager()
        tasks = task_manager.get_user_tasks(message.from_user.id)
        
        if not tasks:
            await message.answer("📭 У вас нет активных задач.")
            return
        
        text = "📋 <b>Ваши задачи:</b>\n\n"
        for task in tasks[-5:]:  # Последние 5 задач
            text += format_task_status(task) + "\n\n"
        
        await message.answer(text)
    except RuntimeError as error:
        await message.answer(f"❌ Сервис временно недоступен: {error}")


@router.message(Command("cancel"))
@router.message(F.text == TextCommands.CANCEL)
async def handle_cancel(message: Message, state: FSMContext) -> None:
    """Отмена текущей операции."""
    current_state = await state.get_state()
    if current_state is None:
        await message.answer(
            "ℹ️ Нет активных операций для отмены.", 
            reply_markup=get_main_keyboard()
        )
        return
    
    await state.clear()
    await message.answer("❌ Операция отменена.", reply_markup=get_main_keyboard())


@router.message(F.text.in_([task["name"] for task in config.TASK_TYPES.values()]))
async def handle_task_selection(message: Message, state: FSMContext) -> None:
    """Обработчик выбора типа задачи."""
    task_name = message.text
    task_type = get_task_type_by_name(task_name)
    task_config = config.TASK_TYPES[task_type]
    
    await state.update_data(
        task_type=task_type,
        task_config=task_config
    )
    
    # Если нужен выбор сервиса
    if task_config.get("needs_service_selection"):
        service_type = task_config.get("service_type")
        
        if service_type == "llm":
            await state.set_state(ServiceSelectionState.selecting_llm)
            await message.answer(
                "🤖 <b>Выберите AI модель:</b>",
                reply_markup=get_llm_service_keyboard()
            )
        elif service_type == "custom":
            await state.set_state(ServiceSelectionState.building_chain)
            await message.answer(
                "⚙️ <b>Построение цепочки сервисов</b>\n\n"
                "Выберите сервисы по порядку обработки:",
                reply_markup=get_service_chain_keyboard()
            )
    
    else:
        # Используем сервис по умолчанию
        default_service = task_config["default_service"]
        await state.update_data(selected_service=[default_service])
        await state.set_state(TaskCreationState.waiting_for_input)
        
        input_type = task_config["input_type"]
        input_description = "текст" if input_type == "text" else "аудио сообщение"
        
        await message.answer(
            f"📥 <b>Отправьте {input_description}</b>\n\n"
            f"Сервис: {config.AUDIO_SERVICES.get(default_service, default_service)}",
            reply_markup=get_cancel_keyboard()
        )


@router.callback_query(ServiceSelectionState.selecting_llm, F.data.startswith("service_llm:"))
async def handle_llm_selection(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработчик выбора LLM сервиса."""
    service_id = callback.data.split(":")[1]
    service_name = config.LLM_SERVICES[service_id]

    # Если мы в цепочки чтобы сменить в ней LLM
    user_data = await state.get_data()
    task_type = user_data["task_type"]
    service_chain = None

    if "service_chain" in user_data:
        service_chain = [user_data["service_chain"]]

    elif len(config.SERVICE_CHAINS.get(task_type, [])) > 1:
        service_chain = config.SERVICE_CHAINS.get(task_type, [])

    if service_chain:
        i = 0
        # Выбираем llm если он есть в цепочки на тот что выбрали в inlaine
        for service in service_chain:
            for llm_service in config.LLM_SERVICES:
                if service == llm_service:
                    service_chain[i] = service_id
                    await state.update_data(service_chain=service_chain)
            i+=1

    await state.update_data(selected_service=[service_id])
    await state.set_state(TaskCreationState.waiting_for_input)
    
    await callback.message.edit_text(
        f"✅ <b>Выбрана модель:</b> {service_name}\n\n"
        f"📥 Теперь отправьте текст для обработки:",
        reply_markup=None
    )
    await callback.answer()


@router.callback_query(ServiceSelectionState.building_chain, F.data.startswith("chain_"))
async def handle_chain_building(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработчик построения цепочки сервисов."""
    action = callback.data.split(":")[0]
    
    if action == "chain_add":
        service_id = callback.data.split(":")[1]
        user_data = await state.get_data()
        selected_services = user_data.get("selected_services", [])
        
        if service_id not in selected_services:
            selected_services.append(service_id)
            await state.update_data(selected_services=selected_services)
        
        await callback.message.edit_reply_markup(
            reply_markup=get_service_chain_keyboard(selected_services)
        )
        await callback.answer(f"Добавлен: {service_id}")
    
    elif action == "chain_run":
        user_data = await state.get_data()
        selected_services = user_data.get("selected_services", [])
        
        if not selected_services:
            await callback.answer("❌ Выберите хотя бы один сервис")
            return
        
        await state.update_data(service_chain=selected_services)
        await state.set_state(TaskCreationState.waiting_for_input)
        
        chain_text = " → ".join(selected_services)
        await callback.message.edit_text(
            f"✅ <b>Цепочка создана:</b>\n{chain_text}\n\n"
            f"📥 Теперь отправьте текст для обработки:",
            reply_markup=None
        )
        await callback.answer()
    
    elif action == "chain_reset":
        await state.update_data(selected_services=[])
        await callback.message.edit_reply_markup(
            reply_markup=get_service_chain_keyboard()
        )
        await callback.answer("Цепочка сброшена")
    
    elif action == "chain_cancel":
        await state.clear()
        await callback.message.delete()
        await callback.message.answer("❌ Создание цепочки отменено", reply_markup=get_main_keyboard())
        await callback.answer()


@router.message(TaskCreationState.waiting_for_input, F.text)
async def handle_text_input(message: Message, state: FSMContext) -> None:
    """Обработка текстового ввода."""
    try:
        task_manager = get_task_manager()
    except RuntimeError as error:
        await message.answer(f"❌ Сервис временно недоступен: {error}")
        await state.clear()
        return
    
    if not validate_text_length(message.text):
        await message.answer("❌ Текст слишком длинный. Максимум 4000 символов.")
        return
    
    user_data = await state.get_data()
    task_type = user_data["task_type"]
    task_config = user_data["task_config"]
    
    # Определяем цепочку сервисов
    if task_config.get("is_chain"):
        service_chain = config.SERVICE_CHAINS.get(task_type, [])
        logger.info(f"1 {service_chain}")
    elif "service_chain" in user_data:
        service_chain = user_data["service_chain"]
        logger.info(f"2 {service_chain}")
    else:
        service_chain = user_data["selected_service"]
        logger.info(f"3 {service_chain}")

    # Создаем задачу
    try:
        user_task = await task_manager.create_task(
            user_id=message.from_user.id,
            chat_id=message.chat.id,
            task_type=task_type,
            input_data={"text": message.text},
            service_chain=service_chain
        )
        
        status_text = format_task_status(user_task)
        
        await message.answer(
            status_text,
            reply_markup=get_main_keyboard()
        )
    
    except Exception as error:
        await message.answer(
            f"❌ <b>Ошибка при создании задачи:</b>\n{str(error)}",
            reply_markup=get_main_keyboard()
        )
    
    await state.clear()


@router.message(TaskCreationState.waiting_for_input, F.voice)
async def handle_voice_input(message: Message, state: FSMContext) -> None:
    """Обработка голосового сообщения."""
    try:
        task_manager = get_task_manager()
    except RuntimeError as error:
        await message.answer(f"❌ Сервис временно недоступен: {error}")
        await state.clear()
        return
    
    await message.answer(
        "🎤 <b>Голосовое сообщение получено</b>\n\n"
        "Обработка аудио в разработке...",
        reply_markup=get_main_keyboard()
    )

    user_data = await state.get_data()
    task_type = user_data["task_type"]
    task_config = user_data["task_config"]

    if task_config.get("is_chain"):
        service_chain = config.SERVICE_CHAINS.get(task_type, [])
    elif "service_chain" in user_data:
        service_chain = user_data["service_chain"]
    elif user_data:
        service_chain = user_data["selected_service"]
    else:
        service = config.TASK_TYPES.get("voice_transcription").get("default_service")
        service_chain = [service]
        task_type = service

    file_id = message.voice.file_id

    # Получаем сервисы через контейнер
    container = ServiceContainer.get_instance()
    if container.task_manager is None:
        logger.error("❌ Task manager not available") 
           
    file = await container.bot.get_file(file_id)
    file_path = file.file_path
    
    # Формируем URL для скачивания файла
    file_url = f"https://api.telegram.org/file/bot{config.TELEGRAM_TOKEN}/{file_path}"

    # Создаем задачу
    try:
        user_task = await task_manager.create_task(
            user_id=message.from_user.id,
            chat_id=message.chat.id,
            task_type=task_type,
            input_data={"audio_url": file_url},
            service_chain=service_chain
        )
        
        status_text = format_task_status(user_task)
        
        await message.answer(
            status_text,
            reply_markup=get_main_keyboard()
        )
        
    except Exception as error:
        await message.answer(
            f"❌ <b>Ошибка при создании задачи:</b>\n{str(error)}",
            reply_markup=get_main_keyboard()
        )
    await state.clear()

#* ОБРАБОТКА ФАЙЛОВ ДЛЯ QDRANT
@router.message(F.document)
async def handle_pdf_document(message: Message, state: FSMContext) -> None:
    """Обработчик присланных PDF-файлов — сразу создаёт задачу index_document для Qdrant."""
    try:
        task_manager = get_task_manager()
    except RuntimeError as error:
        await message.answer(f"❌ Сервис временно недоступен: {error}")
        await state.clear()
        return

    # Проверяем, что есть document и имя файла
    doc = message.document
    if not doc or not getattr(doc, "file_name", None):
        await message.answer("❌ Файл не распознан.")
        return

    filename = doc.file_name.lower()
    # Обрабатываем только pdf
    if not filename.endswith(".pdf"):
        await message.answer("ℹ️ Поддерживаются только PDF-файлы. Отправьте .pdf для индексирования.")
        return

    await message.answer("📥 Получен PDF. Отправляю на индексирование...", reply_markup=get_main_keyboard())

    # Получаем file_path от Telegram и формируем ссылку для скачивания
    container = ServiceContainer.get_instance()
    try:
        file_obj = await container.bot.get_file(doc.file_id)
        file_path = file_obj.file_path
        file_url = f"https://api.telegram.org/file/bot{config.TELEGRAM_TOKEN}/{file_path}"
    except Exception as e:
        logger.exception("Ошибка при получении файла из Telegram: %s", e)
        await message.answer("❌ Не удалось получить файл из Telegram.")
        return

    # Формируем service_chain — направляем задачу прямо в qdrant (можно править по конфигу)
    service_chain = ["qdrant-service"]

    # Создаём задачу index_document
    try:
        user_task = await task_manager.create_task(
            user_id=message.from_user.id,
            chat_id=message.chat.id,
            task_type="index_document",
            input_data={"file_url": file_url, "owner": str(message.from_user.id)},
            service_chain=service_chain
        )

        status_text = format_task_status(user_task)
        await message.answer(status_text, reply_markup=get_main_keyboard())

    except Exception as error:
        logger.exception("Ошибка при создании задачи index_document: %s", error)
        await message.answer(f"❌ Ошибка при создании задачи: {error}", reply_markup=get_main_keyboard())

    # Чистим состояние (если было)
    await state.clear()



@router.message()
async def handle_unknown(message: Message) -> None:
    """Обработчик неизвестных сообщений."""
    await message.answer(
        "🤔 <b>Не понял ваше сообщение</b>\n\n"
        "Используйте кнопки меню или команды:\n"
        "/start - Главное меню\n"
        "/help - Помощь",
        reply_markup=get_main_keyboard()
    )