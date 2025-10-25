# AI Service Orchestration Platform

[![Python](https://img.shields.io/badge/Python-3.13%2B-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.118%2B-green)](https://fastapi.tiangolo.com)
[![Pydantic](https://img.shields.io/badge/Pydantic-2.11%2B-brightgreen)](https://pydantic-docs.helpmanual.io/)
[![aio-pika](https://img.shields.io/badge/aio--pika-9.5%2B-yellow)](https://aio-pika.readthedocs.io/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13%2B-orange)](https://www.rabbitmq.com)
[![Uvicorn](https://img.shields.io/badge/Uvicorn-0.37%2B-purple)](https://www.uvicorn.org/)
[![Docker](https://img.shields.io/badge/Docker-28%2B-blue)](https://docker.com)

Микросервисная платформа для оркестрации разнородных AI-сервисов, предназначенная для построения сложных, отказоустойчивых и легко масштабируемых AI-пайплайнов.

## 🚀 Возможности

- **Гибридная архитектура**: Event-Driven Architecture + Service Chaining Pattern
- **Строгая типизация**: Валидация сообщений через Pydantic
- **Отказоустойчивость**: Retry-механизмы, health-checks, Circuit Breaker
- **Гарантии доставки**: Семантика exactly-once через вебхук-подтверждения
- **Горизонтальное масштабирование**: Независимое масштабирование компонентов
- **Унифицированный API**: Стандартизированные эндпоинты для всех AI-сервисов
- **Простота развертывания**: Полная контейнеризация с Docker

## 🏗️ Архитектура системы

### Общая схема

![Архитектура платформы](docs/scheme.svg)

### Ключевые компоненты

#### 🔌 Wrapper (Внешний шлюз)
- Прием HTTP-запросов от клиентов
- Генерация ID задач и определение пайплайнов
- Абстракция внутренней сложности системы

#### 🐇 Message Broker (RabbitMQ)
- Центральный коммуникационный хаб
- Retry-очереди с TTL и Dead Letter Exchange
- Обеспечение надежной доставки сообщений

#### 🎛️ Worker-Orchestrator
- Оркестрация выполнения пайплайнов
- Health-checks зависимых сервисов
- Retry-логика и Circuit Breaker
- Отслеживание состояния задач через TaskManager

#### 🤖 AI-сервисы
- Специализированные сервисы на единой платформе
- Стандартизированные эндпоинты и API
- Независимое развертывание и масштабирование

## 💾 Модели данных

```python
class PayloadType(str, Enum):
    TEXT = "text"
    URL = "url" 
    AUDIO = "audio"
    VIDEO = "video"
    FILE = "file"
    ERROR = "error"

class MessageType(str, Enum):
    TASK = "task"
    RESULT = "result"
    ERROR = "error"
    STATUS = "status"

class Data(BaseModel):
    """Универсальный контейнер данных для задач и результатов"""
    task_type: Optional[str] = None
    payload_type: PayloadType = PayloadType.TEXT

    payload: Dict[str, Any] = Field(default_factory=dict)

    wrapper_callback_url: Optional[str] = None
    callback_url: Optional[str] = None

    original_message_id: Optional[UUID] = None

    parameters: Dict[str, Any] = Field(default_factory=dict)
    execution_metadata: Dict[str, Any] = Field(default_factory=dict)

class BaseMessage(BaseModel):
    """Базовая модель всех сообщений в системе"""
    message_id: UUID = Field(default_factory=uuid4)
    message_type: MessageType
    source_service: str

    target_services: List[str] = Field(default_factory=list)

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: str = "2.0"

    data: Data

class TaskMessage(BaseMessage):
    """Сообщение-задача для выполнения работы"""
    message_type: MessageType = MessageType.TASK

class ResultMessage(BaseMessage):
    """Сообщение-результат выполнения задачи"""
    message_type: MessageType = MessageType.RESULT

    data: Optional[Data] = None  # Может не быть при ошибке

    success: bool = True
    error_message: Optional[str] = None
```

## 📦 Быстрый старт

### Предварительные требования

- Docker
- Docker Compose

### Установка и запуск

1. **Клонирование репозитория**
```bash
$ git clone https://github.com/duudee0/transcription-bot.git
$ cd transcription-bot
```

2. **Настройка окружения**
```bash
$ cp .env.example .env
# Отредактируйте .env при необходимости
```

3. **Запуск приложения**
```bash
$ docker-compose up
```

