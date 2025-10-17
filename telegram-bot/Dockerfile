FROM python:3.13.7-slim
WORKDIR /app

# Копируем зависимости воркера
COPY telegram-bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код воркера
COPY telegram-bot/ .

CMD ["python", "main.py"]