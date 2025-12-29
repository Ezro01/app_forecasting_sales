"""
Конфигурационный модуль для управления настройками приложения.
Все секреты и конфигурации загружаются из переменных окружения.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any
import logging

# Настройка логирования для config модуля
logger = logging.getLogger(__name__)

# Загружаем переменные окружения из .env файла
load_dotenv('.env')


def get_required_env(name: str) -> str:
    """
    Получает обязательную переменную окружения.
    
    Args:
        name: Имя переменной окружения
        
    Returns:
        Значение переменной
        
    Raises:
        ValueError: Если переменная не найдена
    """
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"Environment variable {name} is required but not set")
    return value


def get_optional_env(name: str, default: Any = None) -> Any:
    """
    Получает необязательную переменную окружения.
    
    Args:
        name: Имя переменной окружения
        default: Значение по умолчанию
        
    Returns:
        Значение переменной или значение по умолчанию
    """
    return os.getenv(name, default)


def get_ssh_key_path() -> str:
    """
    Автовыбор SSH ключа по окружению (CI/CD или локальное).
    
    Returns:
        Путь к SSH ключу
        
    Raises:
        FileNotFoundError: Если ключ не найден
    """
    env_type = get_optional_env('ENV_TYPE', 'local').lower()
    
    # Проверяем допустимые значения env_type
    if env_type not in ('local', 'stage', 'prod'):
        raise ValueError(f"Invalid ENV_TYPE: {env_type}. Must be 'local', 'stage' or 'prod'")
    
    # Вариант 1: Ключ из переменной окружения (для .env файла, GitLab CI/CD и Docker)
    key_env_value = os.getenv(f"SSH_KEY_{env_type.upper()}")
    if key_env_value:
        key_env_value = key_env_value.strip()
        project_root = Path(__file__).parent
        
        # Проверяем, является ли значение путем к файлу (абсолютным или относительным)
        # Сначала пробуем как абсолютный путь
        key_file_path = Path(key_env_value)
        if key_file_path.is_absolute() and key_file_path.exists() and key_file_path.is_file():
            logger.info(f"Обнаружен абсолютный путь к файлу ключа: {key_file_path}")
            return str(key_file_path)
        
        # Пробуем как относительный путь от корня проекта
        relative_key_path = project_root / key_env_value
        if relative_key_path.exists() and relative_key_path.is_file():
            logger.info(f"Обнаружен относительный путь к файлу ключа: {relative_key_path}")
            return str(relative_key_path)
        
        # Пробуем как имя файла в корне проекта
        project_key_path = project_root / key_env_value
        if project_key_path.exists() and project_key_path.is_file():
            logger.info(f"Найден ключ в проекте по имени из переменной окружения: {project_key_path}")
            return str(project_key_path)
        
        # Если это содержимое ключа (начинается с BEGIN или PuTTY), создаем временный файл
        if key_env_value.startswith('-----BEGIN') or key_env_value.startswith('PuTTY-User-Key-File'):
            temp_key_path = f"/tmp/ssh_keys/ssh_key_{env_type}"
            try:
                # Создаем директорию если её нет
                os.makedirs(os.path.dirname(temp_key_path), exist_ok=True)
                with open(temp_key_path, 'w', encoding='utf-8') as f:
                    f.write(key_env_value)
                os.chmod(temp_key_path, 0o600)
                logger.info(f"Создан временный файл ключа из содержимого переменной окружения: {temp_key_path}")
                return temp_key_path
            except IOError as e:
                raise IOError(f"Failed to create temporary SSH key file: {e}")
        else:
            # Если ничего не подошло, выдаем предупреждение и продолжаем поиск
            logger.warning(
                f"SSH_KEY_{env_type.upper()}='{key_env_value}' не является:\n"
                f"  - абсолютным путем к файлу\n"
                f"  - относительным путем от корня проекта\n"
                f"  - именем файла в корне проекта\n"
                f"  - содержимым ключа (BEGIN... или PuTTY-User-Key-File)\n"
                f"Продолжаем поиск ключа в других местах..."
            )
    
    # Вариант 2: Локальные файлы в проекте (только для local)
    if env_type == 'local':
        # Сначала проверяем ключи в корне проекта
        project_root = Path(__file__).parent
        project_keys = [
            'id_ed25519',      # OpenSSH формат Ed25519
            'id_ed25519.ppk',  # PPK формат Ed25519
            'id_rsa',          # OpenSSH формат RSA
            'id_rsa.ppk',      # PPK формат RSA
            'id_ecdsa',        # OpenSSH формат ECDSA
            'id_ecdsa.ppk'     # PPK формат ECDSA
        ]
        
        for key_name in project_keys:
            key_path = project_root / key_name
            if key_path.exists():
                logger.info(f"Найден ключ в проекте: {key_path}")
                return str(key_path)
        
        # Проверяем стандартный путь для локальной машины
        local_key_path = Path('~/.ssh/id_ed25519').expanduser()
        if local_key_path.exists():
            return str(local_key_path)
        
        # Также проверяем другие возможные ключи в ~/.ssh
        for key_name in ['id_rsa', 'id_ed25519', 'id_ecdsa']:
            key_path = Path(f'~/.ssh/{key_name}').expanduser()
            if key_path.exists():
                return str(key_path)
        
        # Вариант 3: Локальные файлы для Docker (только для local-docker)
        docker_key_path = Path('/home/appuser/.ssh/id_ed25519')
        if docker_key_path.exists():
            return str(docker_key_path)
        
        # Вариант 4: Проверяем /tmp/ssh_keys для Docker (если ключ был смонтирован)
        tmp_key_path = Path('/tmp/ssh_keys/id_ed25519')
        if tmp_key_path.exists():
            return str(tmp_key_path)
    
    raise FileNotFoundError(f"No SSH key found for {env_type} environment")


# Конфигурация подключения к базе данных
DB_CONFIG: Dict[str, Any] = {
    'db_host': get_required_env('DB_HOST'),
    'db_port': int(get_required_env('DB_PORT')),
    'db_name': get_required_env('DB_NAME'),
    'db_user': get_required_env('DB_USER'),
    'db_password': get_required_env('DB_PASSWORD')
}

# Конфигурация SFTP
try:
    SFTP_CONFIG: Dict[str, Any] = {
        'host': get_required_env('SFTP_HOST'),
        'port': int(get_optional_env('SFTP_PORT', '22')),
        'username': get_required_env('SFTP_USERNAME'),
        'key_filename': get_ssh_key_path()
    }
except (FileNotFoundError, ValueError) as e:
    # SFTP конфигурация может быть опциональной
    SFTP_CONFIG: Dict[str, Any] = {
        'host': get_optional_env('SFTP_HOST', ''),
        'port': int(get_optional_env('SFTP_PORT', '22')),
        'username': get_optional_env('SFTP_USERNAME', ''),
        'key_filename': None
    }

# Конфигурация приложения
APP_CONFIG: Dict[str, Any] = {
    'host': get_optional_env('APP_HOST', '0.0.0.0'),
    'port': int(get_optional_env('APP_PORT', '8000'))
}

# Конфигурация путей к локальным файлам (опционально, только для локальной разработки)
DATA_CONFIG: Dict[str, Any] = {
    'train_data_path': get_optional_env('TRAIN_DATA_PATH', 'data/train_df.csv'),
    'test_data_path': get_optional_env('TEST_DATA_PATH', 'data/test_df.csv')
}

# Конфигурация логирования
LOG_LEVEL = get_optional_env('LOG_LEVEL', 'INFO').upper()

