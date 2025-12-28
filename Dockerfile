# Многостадийная сборка для оптимизации размера образа
FROM python:3.12-slim-bookworm as builder

# Установка системных зависимостей для сборки
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Создание виртуального окружения
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Копирование и установка зависимостей Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Финальный образ
FROM python:3.12-slim-bookworm

# Метаданные образа
LABEL maintainer="Sales Forecasting Team"
LABEL description="Sales Forecasting API application"

# Установка только runtime зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    openssh-client \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Копирование виртуального окружения из builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Создание непривилегированного пользователя
RUN useradd -m -u 1000 appuser && \
    mkdir -p /app /tmp/ssh_keys && \
    chown -R appuser:appuser /app /tmp/ssh_keys

# Установка рабочей директории
WORKDIR /app

# Копирование файлов приложения
COPY --chown=appuser:appuser . .

# Переключение на непривилегированного пользователя
USER appuser

# Переменные окружения
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1

# Открытие порта приложения
EXPOSE 8000

# Healthcheck для мониторинга состояния приложения
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/main/ || exit 1

# Команда запуска приложения
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
