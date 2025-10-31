from typing import List
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton
)
from config import config


def get_main_keyboard():
    """Главная клавиатура."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📝 Анализ текста"),
                KeyboardButton(text="🎤 Транскрибация"),
            ],
            [
                KeyboardButton(text="🔊 Текст в речь"), 
                KeyboardButton(text="💬 Голосовой чат"),
            ],
            [
                KeyboardButton(text="⚙️ Кастомная цепочка"),
                KeyboardButton(text="📊 Мои задачи"),
            ],
            [
                KeyboardButton(text="ℹ️ Помощь"),
            ]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )


def get_llm_service_keyboard():
    """Клавиатура выбора LLM сервиса."""
    buttons = []
    for service_id, service_name in config.LLM_SERVICES.items():
        buttons.append([InlineKeyboardButton(
            text=service_name,
            callback_data=f"service_llm:{service_id}"
        )])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_audio_service_keyboard():
    """Клавиатура выбора аудио сервисов."""
    buttons = []
    for service_id, service_name in config.AUDIO_SERVICES.items():
        buttons.append([InlineKeyboardButton(
            text=service_name,
            callback_data=f"service_audio:{service_id}"
        )])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_service_chain_keyboard(selected_services: List[str] = None):
    """Клавиатура для построения цепочки сервисов."""
    selected_services = selected_services or []
    
    buttons = []
    
    # Доступные сервисы
    all_services = {**config.LLM_SERVICES, **config.AUDIO_SERVICES}
    
    for service_id, service_name in all_services.items():
        prefix = "✅" if service_id in selected_services else "◻️"
        buttons.append([InlineKeyboardButton(
            text=f"{prefix} {service_name}",
            callback_data=f"chain_add:{service_id}"
        )])
    
    # Кнопки управления
    control_buttons = []
    if selected_services:
        control_buttons.append(
            InlineKeyboardButton(text="🚀 Запустить", callback_data="chain_run")
        )
        control_buttons.append(
            InlineKeyboardButton(text="🔄 Сбросить", callback_data="chain_reset")
        )
    else:
        control_buttons.append(
            InlineKeyboardButton(text="❌ Отмена", callback_data="chain_cancel")
        )
    
    buttons.append(control_buttons)
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_cancel_keyboard():
    """Клавиатура отмены."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )