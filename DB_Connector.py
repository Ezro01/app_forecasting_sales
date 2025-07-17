import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import logging
from contextlib import contextmanager

class DBConnector:
    def __init__(self, ssh_host, ssh_user, ssh_pkey,
                 db_host, db_port, db_name, db_user, db_password):
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_pkey = ssh_pkey
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.tunnel = None

    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для получения соединения с БД"""
        try:
            # Создаем SSH-туннель
            with SSHTunnelForwarder(
                    (self.ssh_host, 22),
                    ssh_username=self.ssh_user,
                    ssh_pkey=self.ssh_pkey,
                    remote_bind_address=(self.db_host, self.db_port),
                    local_bind_address=('0.0.0.0', 6543)
            ) as tunnel:
                self.tunnel = tunnel
                print(f"SSH-туннель открыт на localhost:{tunnel.local_bind_port}")

                # Устанавливаем соединение с БД
                with psycopg2.connect(
                        host="127.0.0.1",
                        port=tunnel.local_bind_port,
                        dbname=self.db_name,
                        user=self.db_user,
                        password=self.db_password,
                        sslmode="require"
                ) as conn:
                    yield conn

        except Exception as e:
            print(f"Ошибка подключения: {e}")
            raise


