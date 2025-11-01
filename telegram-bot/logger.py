"""Централизованная конфигурация логгера."""
import logging
import sys
from pathlib import Path
from typing import Optional

from config import config


def setup_logger(
    name: str = "app",
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Настраивает и возвращает логгер.
    
    Args:
        name: Имя логгера
        log_level: Уровень логирования
        log_file: Путь к файлу для записи логов
        format_string: Формат строки лога
    
    Returns:
        Настроенный логгер
    """
    # Преобразуем строковый уровень в числовой
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Формат по умолчанию
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Создаем логгер
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Очищаем существующие обработчики (на случай повторной инициализации)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Создаем форматтер
    formatter = logging.Formatter(format_string)
    
    # Обработчик для stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Обработчик для файла (если указан)
    if log_file:
        # Создаем директорию если нужно
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Предотвращаем передачу логов корневому логгеру
    logger.propagate = False
    
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Возвращает настроенный логгер для указанного имени.
    
    Args:
        name: Имя логгера (обычно __name__)
    
    Returns:
        Настроенный логгер
    """
    if name is None:
        name = "app"
    
    # Если логгер уже существует, возвращаем его
    logger = logging.getLogger(name)
    
    # Если у логгера нет обработчиков, настраиваем его
    if not logger.handlers:
        setup_logger(name)
    
    return logger


# Глобальный логгер приложения
app_logger = get_logger("bot")