FROM python:3.13.7-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Копируем common файлы из корня
COPY common/ /app/common/

# Копируем base_service из models/common
COPY models/common/ /app/common/

# Копируем код сервиса
COPY models/embedding/ /app/

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8004

CMD ["python", "main.py"]