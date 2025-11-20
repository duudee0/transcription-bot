FROM python:3.13.7-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

COPY common/models.py /app/common/models.py
COPY common/publisher.py /app/common/publisher.py

COPY services/common /app/common

# Копируем общие модели
COPY services/qdrant-service /app

RUN pip install -r requirements.txt


EXPOSE 8000

# Переменные окружения по умолчанию
# ENV QDRANT_HOST=qdrant
# ENV QDRANT_PORT=6333
# ENV QDRANT_COLLECTION=documents
# ENV EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
# ENV EMBEDDING_BATCH_SIZE=32
# ENV CHUNK_SIZE_CHARS=3000
# ENV CHUNK_OVERLAP_CHARS=500
# ENV DOWNLOAD_TIMEOUT=60

# Запускаем uvicorn в режиме, корректно обрабатывающем сигналы
CMD ["python", "main.py"]
