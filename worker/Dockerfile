FROM python:3.13.7-slim
WORKDIR /app

# Копируем общие модули
COPY common/models.py /app/common/models.py
COPY common/publisher.py /app/common/publisher.py

# Копируем зависимости воркера
COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код воркера
COPY worker/ .

CMD ["python", "main.py"]