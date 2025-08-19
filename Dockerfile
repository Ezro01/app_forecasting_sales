# Базовый образ Python 3.12
FROM python:3.12.3-slim-bookworm

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    openssh-client \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Создаем непривилегированного пользователя и настраиваем окружение
RUN useradd -m appuser && \
    mkdir -p /app && \
    mkdir -p /home/appuser/.ssh && \
    chown -R appuser:appuser /app /home/appuser/.ssh && \
    chmod 700 /home/appuser/.ssh && \
    chmod 755 /home/appuser

WORKDIR /app

# Копируем только requirements.txt сначала для кеширования и устанавливаем зависимости и Устанавливаем Python-зависимости
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем остальные файлы (исключая ненужные через .dockerignore)
COPY --chown=appuser:appuser . .

# Переключаем пользователя
USER appuser

# Несекретные переменные окружения
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app 

# Порт приложения
EXPOSE 8000

# Healthcheck для мониторинга
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/main/ || exit 1

# Команда запуска
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]