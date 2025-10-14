FROM python:3.13.7-slim
WORKDIR /app

# Копируем общие модули
COPY common/models.py /app/common/models.py
COPY common/publisher.py /app/common/publisher.py

# Копируем зависимости воркера
COPY wrapper/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY wrapper/ .

CMD ["python", "main.py"]