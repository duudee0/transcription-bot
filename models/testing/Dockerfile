FROM python:3.13.7-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY common/models.py /app/common/models.py

COPY models/common /app/common

# Копируем общие модели
COPY models/testing /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

# Запускаем uvicorn в режиме, корректно обрабатывающем сигналы
CMD ["python", "main.py"]
