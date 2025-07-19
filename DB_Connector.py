import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import logging
from contextlib import contextmanager

class DBConnector:
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для получения соединения с локальной БД"""
        try:
            # Устанавливаем прямое соединение с локальной БД
            with psycopg2.connect(
                    host=self.db_host,
                    port=self.db_port,
                    dbname=self.db_name,
                    user=self.db_user,
                    password=self.db_password
            ) as conn:
                print(f"Подключение к локальной БД {self.db_name} на {self.db_host}:{self.db_port}")
                yield conn

        except Exception as e:
            print(f"Ошибка подключения к локальной БД: {e}")
            raise

    def get_sqlalchemy_engine(self):
        """Возвращает SQLAlchemy engine для работы с pandas"""
        try:
            connection_string = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            engine = create_engine(connection_string)
            return engine
        except Exception as e:
            print(f"Ошибка создания SQLAlchemy engine: {e}")
            raise


