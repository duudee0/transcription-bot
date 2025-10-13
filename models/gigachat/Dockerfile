FROM python:3.13.7-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Копируем общие модели
COPY common /app/common

COPY models/common /app/common

# Копируем код сервиса
COPY models/gigachat /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["python", "main.py"]