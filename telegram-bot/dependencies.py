"""Dependency Injection контейнер для сервисов."""
from typing import Optional
from aiogram import Bot

from services.wrapper import WrapperService
from services.task_manager import TaskManager

from services.file_sender import FileSender 


class ServiceContainer:
    """Контейнер сервисов с ленивой инициализацией."""
    
    _instance = None
    
    def __init__(self, bot: Bot = None):
        self.bot = bot
        self._wrapper_service: Optional[WrapperService] = None
        self._task_manager   : Optional[TaskManager]= None
        self._file_sender    : Optional[FileSender] = None
    
    @property
    def wrapper_service(self) -> WrapperService:
        """Ленивая инициализация WrapperService."""
        if self._wrapper_service is None:
            self._wrapper_service = WrapperService()
        return self._wrapper_service
    
    @property
    def task_manager(self) -> TaskManager:
        """Ленивая инициализация TaskManager."""
        if self._task_manager is None and self.bot:
            self._task_manager = TaskManager(self.bot)
        return self._task_manager
    
    @property
    def file_sender(self) -> FileSender:
        """Ленивая инициализация FileSender."""
        if self._file_sender is None and self.bot:
            self._file_sender = FileSender(self.bot)
        return self._file_sender
    
    @classmethod
    def get_instance(cls) -> 'ServiceContainer':
        """Получить синглтон экземпляр контейнера."""
        if cls._instance is None:
            cls._instance = ServiceContainer()
        return cls._instance


def get_task_manager() -> TaskManager:
    """Получить менеджер задач из контейнера."""
    container = ServiceContainer.get_instance()
    task_manager = container.task_manager
    
    if task_manager is None:
        raise RuntimeError("TaskManager not initialized. Bot might not be set.")
    
    return task_manager

def get_file_sender() -> FileSender:
    """Получить FileSender из контейнера."""
    container = ServiceContainer.get_instance()
    file_sender = container.file_sender
    
    if file_sender is None:
        raise RuntimeError("FileSender not initialized. Bot might not be set.")
    
    return file_sender