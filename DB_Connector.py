"""
Модуль для подключения к базе данных PostgreSQL.
Предоставляет классы и функции для работы с базой данных.
"""
import psycopg2
from sqlalchemy import create_engine
import logging
from contextlib import contextmanager
from typing import Generator

# Настройка логирования
logger = logging.getLogger(__name__)


class DBConnector:
    """
    Класс для управления подключениями к базе данных PostgreSQL.
    
    Предоставляет контекстный менеджер для безопасной работы с соединениями
    и создание SQLAlchemy engine для работы с pandas.
    """
    
    def __init__(self, db_host: str, db_port: int, db_name: str, db_user: str, db_password: str):
        """
        Инициализация коннектора к базе данных.
        
        Args:
            db_host: Хост базы данных
            db_port: Порт базы данных
            db_name: Имя базы данных
            db_user: Имя пользователя
            db_password: Пароль пользователя
        """
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

    @contextmanager
    def get_connection(self) -> Generator:
        """
        Контекстный менеджер для получения соединения с базой данных.
        
        Yields:
            psycopg2.connection: Соединение с базой данных
            
        Raises:
            psycopg2.Error: При ошибке подключения к базе данных
        """
        try:
            logger.debug(f"Подключение к БД {self.db_name} на {self.db_host}:{self.db_port}")
            with psycopg2.connect(
                    host=self.db_host,
                    port=self.db_port,
                    dbname=self.db_name,
                    user=self.db_user,
                    password=self.db_password
            ) as conn:
                logger.info(f"Успешное подключение к БД {self.db_name}")
                yield conn

        except psycopg2.Error as e:
            logger.error(f"Ошибка подключения к БД {self.db_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при подключении к БД: {e}")
            raise

    def get_sqlalchemy_engine(self) -> create_engine:
        """
        Возвращает SQLAlchemy engine для работы с pandas.
        
        Returns:
            sqlalchemy.engine.Engine: SQLAlchemy engine
            
        Raises:
            Exception: При ошибке создания engine
        """
        try:
            # Безопасное формирование строки подключения
            connection_string = (
                f"postgresql://{self.db_user}:{self.db_password}@"
                f"{self.db_host}:{self.db_port}/{self.db_name}"
            )
            engine = create_engine(connection_string, pool_pre_ping=True)
            logger.info(f"SQLAlchemy engine создан для БД {self.db_name}")
            return engine
        except Exception as e:
            logger.error(f"Ошибка создания SQLAlchemy engine: {e}")
            raise