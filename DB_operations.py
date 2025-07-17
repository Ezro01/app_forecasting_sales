import pandas as pd
from DB_Connector import DBConnector

def create_products_table(db_connector):
    """Создает таблицу products если она не существует"""
    with db_connector.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    product_id INTEGER NOT NULL,
                    store_id INTEGER NOT NULL,
                    date DATE NOT NULL,
                    sales NUMERIC(10, 2),
                    price NUMERIC(10, 2),
                    UNIQUE (product_id, store_id, date)
                )
            """)
            conn.commit()
            print("Таблица products создана или уже существовала")

def get_db_connection(config):
    """Инициализирует и возвращает DBConnector"""
    return DBConnector(
        ssh_host=config['ssh_host'],
        ssh_user=config['ssh_user'],
        ssh_pkey=config['ssh_pkey'],
        db_host=config['db_host'],
        db_port=config['db_port'],
        db_name=config['db_name'],
        db_user=config['db_user'],
        db_password=config['db_password']
    )