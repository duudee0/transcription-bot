FROM python:3.13.7-slim
WORKDIR /app

# Копируем общие модели
COPY common /app/common

# Копируем зависимости воркера
COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код воркера
COPY worker/ .

CMD ["python", "main.py"]