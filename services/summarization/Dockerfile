FROM python:3.13.7-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY common/models.py /app/common/models.py
COPY common/publisher.py /app/common/publisher.py

COPY services/common /app/common

# Копируем общие модели
COPY services/summarization /app

RUN pip install -r requirements.txt


EXPOSE 8000

# Запускаем uvicorn в режиме, корректно обрабатывающем сигналы
CMD ["python", "main.py"]
