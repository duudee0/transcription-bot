FROM python:3.13.7-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY /models/testing/ .

# Копируем общие модели
COPY common /app/common

RUN pip install --no-cache-dir -r requirements.txt


EXPOSE 8000

# Запускаем uvicorn в режиме, корректно обрабатывающем сигналы
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
