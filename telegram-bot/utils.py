import json
from typing import Dict, Any, Tuple
from config import config


def validate_json(text: str) -> Tuple[bool, Any]:
    """Проверяет валидность JSON."""
    try:
        data = json.loads(text)
        return True, data
    except json.JSONDecodeError as e:
        return False, str(e)


def validate_text_length(text: str, max_length: int = 4000) -> bool:
    """Проверяет длину текста."""
    return len(text) <= max_length


def format_task_status(task) -> str:
    """Форматирует статус задачи для пользователя."""
    status_icons = {
        "pending": "⏳",
        "processing": "🔄", 
        "completed": "✅",
        "error": "❌",
        "timeout": "⏰"
    }
    
    icon = status_icons.get(task.status, "📋")
    
    service_chain = " → ".join(task.service_chain)
    
    return (
        f"{icon} <b>Задача #{task.task_id[:8]}</b>\n"
        f"📊 Тип: {task.task_type}\n"
        f"🔗 Цепочка: {service_chain}\n" 
        f"📈 Статус: {task.status}\n"
        f"🕐 Создана: {task.created_at.strftime('%H:%M:%S')}\n"
    )


def format_task_result(task_id: str, status_data: Dict[str, Any]) -> str:
    """Форматирует результат задачи для пользователя."""
    status = status_data.get("status", "unknown")
    result = status_data.get("result")
    error = status_data.get("error")
    
    if status == "completed":
        if isinstance(result, dict) and "text" in result:
            text = result["text"]
            # Обрезаем длинный текст
            if len(text) > 3500:
                text = text[:3500] + "...\n\n(текст обрезан)"
            return f"✅ <b>Результат задачи #{task_id[:8]}</b>\n\n{text}"
        else:
            return f"✅ <b>Задача #{task_id[:8]} завершена</b>\n\nРезультат: {result}"
    
    elif status == "error":
        return f"❌ <b>Ошибка в задаче #{task_id[:8]}</b>\n\n{error}"
    
    elif status == "timeout":
        return f"⏰ <b>Таймаут задачи #{task_id[:8]}</b>\n\nЗадача не была завершена в срок"
    
    return f"📋 <b>Статус задачи #{task_id[:8]}</b>\n\n{status}"


def get_task_type_by_name(name: str) -> str:
    """Возвращает тип задачи по имени."""
    for task_id, task_info in config.TASK_TYPES.items():
        if task_info["name"] == name:
            return task_id
    return name  # fallback