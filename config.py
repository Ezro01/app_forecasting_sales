"""
Конфигурационный модуль для управления настройками приложения.
Все секреты и конфигурации загружаются из переменных окружения.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any

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
    
    # Вариант 1: Ключ из переменной (для GitLab CI/CD)
    key_content = os.getenv(f"SSH_KEY_{env_type.upper()}")
    if key_content:
        key_path = f"/tmp/ssh_key_{env_type}"
        try:
            with open(key_path, 'w') as f:
                f.write(key_content)
            os.chmod(key_path, 0o600)
            return key_path
        except IOError as e:
            raise IOError(f"Failed to create temporary SSH key file: {e}")
    
    # Вариант 2: Локальные файлы для macOS/Linux (только для local)
    if env_type == 'local':
        # Проверяем стандартный путь для локальной машины
        local_key_path = Path('~/.ssh/id_ed25519').expanduser()
        if local_key_path.exists():
            return str(local_key_path)
        
        # Также проверяем другие возможные ключи
        for key_name in ['id_rsa', 'id_ed25519', 'id_ecdsa']:
            key_path = Path(f'~/.ssh/{key_name}').expanduser()
            if key_path.exists():
                return str(key_path)
        
        # Вариант 3: Локальные файлы для Docker (только для local-docker)
        docker_key_path = Path('/home/appuser/.ssh/id_ed25519')
        if docker_key_path.exists():
            return str(docker_key_path)
    
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

