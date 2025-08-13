# Базовый образ Python 3.12
FROM python:3.12.3-slim-bookworm

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Создаем непривилегированного пользователя
RUN useradd -m appuser && \
    mkdir -p /app && \
    chown appuser:appuser /app

WORKDIR /app

# Копируем только requirements.txt сначала для кеширования
COPY --chown=appuser:appuser requirements.txt .

# Устанавливаем Python-зависимости
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем остальные файлы
COPY --chown=appuser:appuser . .

# Переключаем пользователя
USER appuser

# Переменные окружения
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DB_HOST=postgres \
    DB_PORT=5432 \
    DB_NAME=BD_Dobroteka \
    DB_USER=postgres \
    DB_PASSWORD=1234 \
    SFTP_HOST=dobroteka.tomsk.digital \
    SFTP_PORT=22 \
    SFTP_USERNAME=roman

EXPOSE 8000

# Команда запуска
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]