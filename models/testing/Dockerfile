FROM python:3.13.7-slim

WORKDIR /models/testing
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

# Запускаем uvicorn в режиме, корректно обрабатывающем сигналы
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
