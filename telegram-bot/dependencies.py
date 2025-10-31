"""Dependency Injection контейнер для сервисов."""
from aiogram import Bot
from services import TaskManager, WrapperService


class ServiceContainer:
    """Контейнер сервисов с ленивой инициализацией."""
    
    _instance = None
    
    def __init__(self, bot: Bot = None):
        self.bot = bot
        self._wrapper_service = None
        self._task_manager = None
    
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