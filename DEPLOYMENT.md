# Инструкция по развертыванию

## Развертывание в CI/CD

Проект настроен для работы с внешними сервисами (база данных и SFTP), которые подключаются через переменные окружения CI/CD.

### Переменные окружения для CI/CD

В CI/CD pipeline необходимо установить следующие переменные окружения:

#### База данных
- `DB_HOST` - хост внешней базы данных PostgreSQL
- `DB_PORT` - порт базы данных (обычно 5432)
- `DB_NAME` - имя базы данных
- `DB_USER` - пользователь базы данных
- `DB_PASSWORD` - пароль базы данных (хранить в secrets!)

#### SFTP
- `SFTP_HOST` - адрес SFTP сервера
- `SFTP_PORT` - порт SFTP сервера (обычно 22)
- `SFTP_USERNAME` - имя пользователя SFTP

#### SSH ключи
- `SSH_KEY_STAGE` - полный приватный SSH ключ для staging окружения (хранить в secrets!)
- `SSH_KEY_PROD` - полный приватный SSH ключ для production окружения (хранить в secrets!)

**Важно**: SSH ключ должен включать строки `-----BEGIN ... KEY-----` и `-----END ... KEY-----`. В GitLab CI/CD используйте многострочные переменные с разделителем `\n`.

#### Окружение
- `ENV_TYPE` - тип окружения: `stage` или `prod`

#### Приложение
- `APP_HOST` - хост приложения (обычно 0.0.0.0)
- `APP_PORT` - порт приложения (обычно 8000)

### Пример GitLab CI/CD

```yaml
variables:
  DOCKER_IMAGE_NAME: sales-forecasting-api
  DOCKER_REGISTRY: registry.example.com

stages:
  - build
  - deploy

build:
  stage: build
  script:
    - docker build -t $DOCKER_REGISTRY/$DOCKER_IMAGE_NAME:$CI_COMMIT_SHORT_SHA .
    - docker push $DOCKER_REGISTRY/$DOCKER_IMAGE_NAME:$CI_COMMIT_SHORT_SHA

deploy:stage:
  stage: deploy
  environment:
    name: staging
  script:
    - |
      docker run -d \
        --name sales-forecasting-api-staging \
        -p 8000:8000 \
        -e DB_HOST="$DB_HOST_STAGE" \
        -e DB_PORT="$DB_PORT_STAGE" \
        -e DB_NAME="$DB_NAME_STAGE" \
        -e DB_USER="$DB_USER_STAGE" \
        -e DB_PASSWORD="$DB_PASSWORD_STAGE" \
        -e SFTP_HOST="$SFTP_HOST_STAGE" \
        -e SFTP_PORT="$SFTP_PORT_STAGE" \
        -e SFTP_USERNAME="$SFTP_USERNAME_STAGE" \
        -e SSH_KEY_STAGE="$SSH_KEY_STAGE" \
        -e ENV_TYPE=stage \
        -e APP_HOST=0.0.0.0 \
        -e APP_PORT=8000 \
        $DOCKER_REGISTRY/$DOCKER_IMAGE_NAME:$CI_COMMIT_SHORT_SHA
  only:
    - develop

deploy:prod:
  stage: deploy
  environment:
    name: production
  script:
    - |
      docker run -d \
        --name sales-forecasting-api-prod \
        -p 8000:8000 \
        -e DB_HOST="$DB_HOST_PROD" \
        -e DB_PORT="$DB_PORT_PROD" \
        -e DB_NAME="$DB_NAME_PROD" \
        -e DB_USER="$DB_USER_PROD" \
        -e DB_PASSWORD="$DB_PASSWORD_PROD" \
        -e SFTP_HOST="$SFTP_HOST_PROD" \
        -e SFTP_PORT="$SFTP_PORT_PROD" \
        -e SFTP_USERNAME="$SFTP_USERNAME_PROD" \
        -e SSH_KEY_PROD="$SSH_KEY_PROD" \
        -e ENV_TYPE=prod \
        -e APP_HOST=0.0.0.0 \
        -e APP_PORT=8000 \
        $DOCKER_REGISTRY/$DOCKER_IMAGE_NAME:$CI_COMMIT_SHORT_SHA
  only:
    - main
  when: manual
```

### Использование docker-compose в CI/CD

Альтернативно можно использовать docker-compose:

```yaml
deploy:stage:
  script:
    - |
      DB_HOST="$DB_HOST_STAGE" \
      DB_PORT="$DB_PORT_STAGE" \
      DB_NAME="$DB_NAME_STAGE" \
      DB_USER="$DB_USER_STAGE" \
      DB_PASSWORD="$DB_PASSWORD_STAGE" \
      SFTP_HOST="$SFTP_HOST_STAGE" \
      SFTP_PORT="$SFTP_PORT_STAGE" \
      SFTP_USERNAME="$SFTP_USERNAME_STAGE" \
      SSH_KEY_STAGE="$SSH_KEY_STAGE" \
      ENV_TYPE=stage \
      docker-compose up -d
```

### Локальная разработка с Docker

Для локальной разработки с локальной БД используйте `docker-compose.local.yml`:

```bash
docker-compose -f docker-compose.local.yml up -d
```

Этот файл включает локальный PostgreSQL контейнер.

### Без Docker (только для разработки)

Для разработки без Docker:

```bash
# Создайте виртуальное окружение
python -m venv venv
source venv/bin/activate  # На Windows: venv\Scripts\activate

# Установите зависимости
pip install -r requirements.txt

# Создайте .env файл
cp .env.example .env
# Отредактируйте .env с вашими настройками

# Запустите приложение
python main.py
```

### Проверка работоспособности

После развертывания проверьте:

1. Healthcheck endpoint:
```bash
curl http://localhost:8000/main/
```

2. API документация:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

3. Логи контейнера:
```bash
docker logs sales_forecasting_api
```

### Мониторинг

Приложение включает healthcheck для мониторинга. Проверка выполняется каждые 30 секунд через endpoint `/main/`.

### Безопасность

⚠️ **ВАЖНО**:
- Никогда не коммитьте секреты в код
- Используйте CI/CD secrets для хранения паролей и SSH ключей
- В production используйте внешние системы управления секретами (Vault, AWS Secrets Manager и т.д.)
- Ограничьте доступ к переменным окружения в CI/CD
- Регулярно ротируйте пароли и ключи

