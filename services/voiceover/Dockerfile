FROM python:3.11.14-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    espeak-ng \
    ffmpeg \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

COPY common/models.py /app/common/models.py
COPY common/publisher.py /app/common/publisher.py

COPY services/common /app/common

# Копируем общие модели
COPY services/voiceover /app

RUN pip install --no-cache-dir -r requirements.txt

# Директория для статических файлов и кэша моделей
ENV OUTPUT_DIR /app/audio_outputs
RUN mkdir -p ${OUTPUT_DIR}

EXPOSE 8000

# Переменные окружения по умолчанию
ENV WHISPER_MODEL=base
ENV WHISPER_TIMEOUT=300
ENV DOWNLOAD_TIMEOUT=60

# Запускаем uvicorn в режиме, корректно обрабатывающем сигналы
CMD ["python", "main.py"]
