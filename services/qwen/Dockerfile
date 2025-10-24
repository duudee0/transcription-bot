FROM python:3.13.7-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Копируем общие модели
COPY common/models.py /app/common/models.py
COPY common/publisher.py /app/common/publisher.py

COPY services/common /app/common

# Копируем код сервиса
COPY services/qwen /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["python", "main.py"]