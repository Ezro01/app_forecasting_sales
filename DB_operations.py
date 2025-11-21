from DB_Connector import DBConnector
import pickle
import gzip
import pandas as pd
import time
import datetime
from psycopg2 import sql


class Create_tables:
    def create_origin_data_table(self, db_connector):
        """Создает таблицу Исходные_данные_продаж если она не существует"""
        table_name = "Исходные_данные_продаж"

        try:
            with db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Проверка существования таблицы
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                            AND table_schema = current_schema()
                        );
                    """, (table_name,))

                    table_exists = cursor.fetchone()[0]

                    if not table_exists:
                        # Создание таблицы
                        cursor.execute(f"""
                            CREATE TABLE "{table_name}" (
                                "Дата" date NOT NULL,
                                "Магазин" varchar(50) NOT NULL,
                                "Товар" varchar(50) NOT NULL,
                                "Цена" float4 NOT NULL,
                                "Акция" bool NOT NULL,
                                "Выходной" bool NOT NULL,
                                
                                "Категория" varchar(50) NULL,
                                "ПотребГруппа" varchar(50) NULL,
                                "МНН" varchar(50) NULL,
                                
                                "Продано_шт" float4 NOT NULL,
                                "Остаток_шт" float4 NOT NULL,
                                "Поступило_шт" int4 NOT NULL,
                                "Заказ_шт" float4 NOT NULL,
                                "КоличествоЧеков" float4 NOT NULL,
                                
                                "ПроданоСеть_шт" float4 NOT NULL,
                                "ОстатокСеть_шт" float4 NOT NULL,
                                "ПоступилоСеть_шт" int4 NOT NULL,
                                "КоличествоЧековСеть_шт" float4 NOT NULL,
                                
                                CONSTRAINT data_pk_origin PRIMARY KEY ("Дата", "Магазин", "Товар")
                            )
                        """)
                        print(f"Таблица {table_name} успешно создана")

                    # Проверка существования индексов
                    indexes_to_create = {
                        'date_idx_origin': '"Дата"',
                        'store_idx_origin': '"Магазин"',
                        'product_idx_origin': '"Товар"'
                    }

                    created_indexes = 0

                    for index_name, column in indexes_to_create.items():
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM pg_indexes 
                                WHERE indexname = %s 
                                AND tablename = %s
                            );
                        """, (index_name, table_name))

                        if not cursor.fetchone()[0]:
                            cursor.execute(f"""
                                CREATE INDEX {index_name} 
                                ON "{table_name}" ({column});
                            """)
                            created_indexes += 1

                    if created_indexes > 0:
                        print(f"Создано {created_indexes} новых индекса")
                    else:
                        if table_exists:
                            print(f"Таблица {table_name} и все индексы уже существуют")

                    conn.commit()

        except Exception as e:
            print(f"Ошибка при работе с таблицей {table_name}: {e}")
            raise

    def create_enriched_data_table(self, db_connector):
        """Создает таблицу Обогащённые_данные_продаж если она не существует"""
        table_name = "Обогащённые_данные_продаж"

        try:
            with db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Проверка существования таблицы
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                            AND table_schema = current_schema()
                        );
                    """, (table_name,))

                    table_exists = cursor.fetchone()[0]

                    if not table_exists:
                        # Создание таблицы
                        cursor.execute(f"""
                            CREATE TABLE "{table_name}" (
                                "Дата" date NOT NULL,
                                "Магазин" varchar(50) NOT NULL,
                                "Товар" varchar(50) NOT NULL,
                                "Цена" float4 NOT NULL,
                                "Акция" bool NOT NULL,
                                "Выходной" bool NOT NULL,
                                
                                "Категория" varchar(50) NULL,
                                "ПотребГруппа" varchar(50) NULL,
                                "МНН" varchar(50) NULL,
                                
                                "Продано_шт" int4 NOT NULL,
                                "Остаток_шт" int4 NOT NULL,
                                "Поступило_шт" int4 NOT NULL,
                                "Заказ_шт" int4 NOT NULL,
                                "КоличествоЧеков" int4 NOT NULL,
                                
                                "ПроданоСеть_шт" int4 NOT NULL,
                                "ОстатокСеть_шт" int4 NOT NULL,
                                "ПоступилоСеть_шт" int4 NOT NULL,
                                "КоличествоЧековСеть_шт" int4 NOT NULL,
                                
                                "ДеньНедели" int4 NOT NULL,
                                "День" int4 NOT NULL,
                                "Месяц" int4 NOT NULL,
                                "Год" int4 NOT NULL,
                                
                                "Сезонность" varchar(50) NOT NULL,
                                "Сезонность_точн" bool NOT NULL,
                                "Температура (°C)" float4 NOT NULL,
                                "Давление (мм рт. ст.)" float4 NOT NULL,
                                
                                CONSTRAINT data_pk_enriched PRIMARY KEY ("Дата", "Магазин", "Товар")
                            )
                        """)
                        print(f"Таблица {table_name} успешно создана")

                    # Проверка существования индексов
                    indexes_to_create = {
                        'date_idx_enriched': '"Дата"',
                        'store_idx_enriched': '"Магазин"',
                        'product_idx_enriched': '"Товар"'
                    }

                    created_indexes = 0

                    for index_name, column in indexes_to_create.items():
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM pg_indexes 
                                WHERE indexname = %s 
                                AND tablename = %s
                            );
                        """, (index_name, table_name))

                        if not cursor.fetchone()[0]:
                            cursor.execute(f"""
                                CREATE INDEX {index_name} 
                                ON "{table_name}" ({column});
                            """)
                            created_indexes += 1

                    if created_indexes > 0:
                        print(f"Создано {created_indexes} новых индекса")
                    else:
                        if table_exists:
                            print(f"Таблица {table_name} и все индексы уже существуют")

                    conn.commit()

        except Exception as e:
            print(f"Ошибка при работе с таблицей {table_name}: {e}")
            raise

    def create_recovery_data_table(self, db_connector):
        """Создает таблицу Обогащённые_данные_продаж если она не существует"""
        table_name = "Восстановленные_данные_продаж"

        try:
            with db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Проверка существования таблицы
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                            AND table_schema = current_schema()
                        );
                    """, (table_name,))

                    table_exists = cursor.fetchone()[0]

                    if not table_exists:
                        # Создание таблицы
                        cursor.execute(f"""
                            CREATE TABLE "{table_name}" (
                                "Дата" date NOT NULL,
                                "Магазин" varchar(50) NOT NULL,
                                "Товар" varchar(50) NOT NULL,
                                "Цена" float4 NOT NULL,
                                "Акция" bool NOT NULL,
                                "Выходной" bool NOT NULL,
                                
                                "Категория" varchar(50) NULL,
                                "ПотребГруппа" varchar(50) NULL,
                                "МНН" varchar(50) NULL,
                                
                                "Продано_шт" int4 NOT NULL,
                                "Остаток_шт" int4 NOT NULL,
                                "Поступило_шт" int4 NOT NULL,
                                "Заказ_шт" int4 NOT NULL,
                                "КоличествоЧеков" int4 NOT NULL,
                                
                                "ПроданоСеть_шт" int4 NOT NULL,
                                "ОстатокСеть_шт" int4 NOT NULL,
                                "ПоступилоСеть_шт" int4 NOT NULL,
                                "КоличествоЧековСеть_шт" int4 NOT NULL,
                                
                                "ДеньНедели" int4 NOT NULL,
                                "День" int4 NOT NULL,
                                "Месяц" int4 NOT NULL,
                                "Год" int4 NOT NULL,
                                
                                "Пуассон_распр" bool NOT NULL,
                                "Сезонность" varchar(50) NOT NULL,
                                "Сезонность_точн" bool NOT NULL,
                                "Температура (°C)" float4 NOT NULL,
                                "Давление (мм рт. ст.)" float4 NOT NULL,
                                
                                "Медианный_лаг_в_днях" float4 NOT NULL,
                                "Продано_правка" int4 NOT NULL,
                                "Заказы_правка" int4 NOT NULL,
                                "Поступило_правка" int4 NOT NULL,
                                "Остаток_правка" int4 NOT NULL,
                                
                                CONSTRAINT data_pk_recovery PRIMARY KEY ("Дата", "Магазин", "Товар")
                            )
                        """)
                        print(f"Таблица {table_name} успешно создана")

                    # Проверка существования индексов
                    indexes_to_create = {
                        'date_idx_recovery': '"Дата"',
                        'store_idx_recovery': '"Магазин"',
                        'product_idx_recovery': '"Товар"'
                    }

                    created_indexes = 0

                    for index_name, column in indexes_to_create.items():
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM pg_indexes 
                                WHERE indexname = %s 
                                AND tablename = %s
                            );
                        """, (index_name, table_name))

                        if not cursor.fetchone()[0]:
                            cursor.execute(f"""
                                CREATE INDEX {index_name} 
                                ON "{table_name}" ({column});
                            """)
                            created_indexes += 1

                    if created_indexes > 0:
                        print(f"Создано {created_indexes} новых индекса")
                    else:
                        if table_exists:
                            print(f"Таблица {table_name} и все индексы уже существуют")

                    conn.commit()

        except Exception as e:
            print(f"Ошибка при работе с таблицей {table_name}: {e}")
            raise

    def saved_ml_data_table(self, db_connector):
        """Создает таблицу Обогащённые_данные_продаж если она не существует"""
        table_name = "ML_данные_для_работы_модели"

        try:
            with db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Проверка существования таблицы
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                            AND table_schema = current_schema()
                        );
                    """, (table_name,))

                    table_exists = cursor.fetchone()[0]

                    if not table_exists:
                        # Создание таблицы
                        cursor.execute(f"""
                            CREATE TABLE "{table_name}" (
                                load_id SERIAL PRIMARY KEY,
                                label_encoder_product BYTEA NOT NULL,
                                label_encoder_shop BYTEA NOT NULL,
                                label_encoder_category BYTEA NOT NULL,
                                label_encoder_potreb_group BYTEA NOT NULL,
                                label_encoder_mnn BYTEA NOT NULL,
                                minmax_scaler BYTEA NOT NULL,
                                catboost_model BYTEA NOT NULL,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                comment TEXT
                            )
                        """)
                        print(f"Таблица {table_name} успешно создана")

                    # Проверка существования индексов
                    indexes_to_create = {
                        'load_id_idx': '"load_id"',
                    }

                    created_indexes = 0

                    for index_name, column in indexes_to_create.items():
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM pg_indexes 
                                WHERE indexname = %s 
                                AND tablename = %s
                            );
                        """, (index_name, table_name))

                        if not cursor.fetchone()[0]:
                            cursor.execute(f"""
                                CREATE INDEX {index_name} 
                                ON "{table_name}" ({column});
                            """)
                            created_indexes += 1

                    if created_indexes > 0:
                        print(f"Создано {created_indexes} новых индекса")
                    else:
                        if table_exists:
                            print(f"Таблица {table_name} и все индексы уже существуют")

                    conn.commit()

        except Exception as e:
            print(f"Ошибка при работе с таблицей {table_name}: {e}")
            raise

    def create_forecast_table(self, db_connector):
        """Создает таблицу Прогноз если она не существует"""
        table_name = "Прогноз"

        try:
            with db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Проверка существования таблицы
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                            AND table_schema = current_schema()
                        );
                    """, (table_name,))

                    table_exists = cursor.fetchone()[0]

                    if not table_exists:
                        # Создание таблицы
                        cursor.execute(f"""
                            CREATE TABLE "{table_name}" (
                                "Дата" date NOT NULL,
                                "Магазин" varchar(50) NOT NULL,
                                "Товар" varchar(50) NOT NULL,
                                "Прогноз" int4 NOT NULL,
                                "Время_загрузки" timestamp DEFAULT CURRENT_TIMESTAMP,
                                
                                CONSTRAINT forecast_pk PRIMARY KEY ("Дата", "Магазин", "Товар")
                            )
                        """)
                        print(f"Таблица {table_name} успешно создана")

                    # Проверка существования индексов
                    indexes_to_create = {
                        'date_idx_forecast': '"Дата"',
                        'store_idx_forecast': '"Магазин"',
                        'product_idx_forecast': '"Товар"',
                    }

                    created_indexes = 0

                    for index_name, column in indexes_to_create.items():
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM pg_indexes 
                                WHERE indexname = %s 
                                AND tablename = %s
                            );
                        """, (index_name, table_name))

                        if not cursor.fetchone()[0]:
                            cursor.execute(f"""
                                CREATE INDEX {index_name} 
                                ON "{table_name}" ({column});
                            """)
                            created_indexes += 1

                    if created_indexes > 0:
                        print(f"Создано {created_indexes} новых индекса")
                    else:
                        if table_exists:
                            print(f"Таблица {table_name} и все индексы уже существуют")

                    conn.commit()

        except Exception as e:
            print(f"Ошибка при работе с таблицей {table_name}: {e}")
            raise

class DataLoader:
    def __init__(self, db_connector):
        self.db = db_connector
        self.table_configs = {
            "Исходные_данные_продаж": {
                "pk_columns": ["Дата", "Магазин", "Товар"],
                "column_mapping": {
                    # DataFrame column: DB column
                    'Дата': 'Дата',
                    'Магазин': 'Магазин',
                    'Товар': 'Товар',
                    'Цена': 'Цена',
                    'Акция': 'Акция',
                    'Выходной': 'Выходной',

                    'Категория': 'Категория',
                    'ПотребГруппа': 'ПотребГруппа',
                    'МНН': 'МНН',

                    'Продано': 'Продано_шт',
                    'Остаток': 'Остаток_шт',
                    'Поступило': 'Поступило_шт',
                    'Заказ': 'Заказ_шт',
                    'КоличествоЧеков': 'КоличествоЧеков',

                    'ПроданоСеть': 'ПроданоСеть_шт',
                    'ОстатокСеть': 'ОстатокСеть_шт',
                    'ПоступилоСеть':  'ПоступилоСеть_шт',
                    'КоличествоЧековСеть': 'КоличествоЧековСеть_шт',
                },
                "type_mapping": {
                    'Дата': 'date',
                    'Магазин': 'str',
                    'Товар': 'str',
                    'Цена': 'float32',
                    'Акция': 'bool',
                    'Выходной': 'bool',

                    'Категория': 'str',
                    'ПотребГруппа': 'str',
                    'МНН': 'str',

                    'Продано': 'float32',
                    'Остаток': 'float32',
                    'Поступило':  'int32',
                    'Заказ': 'int32',
                    'КоличествоЧеков': 'int32',

                    'ПроданоСеть': 'float32',
                    'ОстатокСеть': 'float32',
                    'ПоступилоСеть':  'int32',
                    'КоличествоЧековСеть': 'int32',
                }
            },
            "Обогащённые_данные_продаж": {
                "pk_columns": ["Дата", "Магазин", "Товар"],
                "column_mapping": {
                    # DataFrame column: DB column
                    'Дата': 'Дата',
                    'Магазин': 'Магазин',
                    'Товар': 'Товар',
                    'Цена': 'Цена',
                    'Акция': 'Акция',
                    'Выходной': 'Выходной',

                    'Категория': 'Категория',
                    'ПотребГруппа': 'ПотребГруппа',
                    'МНН': 'МНН',

                    'Продано': 'Продано_шт',
                    'Остаток': 'Остаток_шт',
                    'Поступило': 'Поступило_шт',
                    'Заказ': 'Заказ_шт',
                    'КоличествоЧеков': 'КоличествоЧеков',

                    'ПроданоСеть': 'ПроданоСеть_шт',
                    'ОстатокСеть': 'ОстатокСеть_шт',
                    'ПоступилоСеть': 'ПоступилоСеть_шт',
                    'КоличествоЧековСеть': 'КоличествоЧековСеть_шт',

                    'ДеньНедели': 'ДеньНедели',
                    'День': 'День',
                    'Месяц': 'Месяц',
                    'Год': 'Год',

                    'Сезонность': 'Сезонность',
                    'Сезонность_точн': 'Сезонность_точн',
                    'Температура (°C)': 'Температура (°C)',
                    'Давление (мм рт. ст.)': 'Давление (мм рт. ст.)'
                },
                "type_mapping": {
                    'Дата': 'date',
                    'Магазин': 'str',
                    'Товар': 'str',
                    'Цена': 'float32',
                    'Акция': 'bool',
                    'Выходной': 'bool',

                    'Категория': 'str',
                    'ПотребГруппа': 'str',
                    'МНН': 'str',

                    'Продано': 'int32',
                    'Остаток': 'int32',
                    'Поступило':  'int32',
                    'Заказ': 'int32',
                    'КоличествоЧеков': 'int32',

                    'ПроданоСеть': 'int32',
                    'ОстатокСеть': 'int32',
                    'ПоступилоСеть':  'int32',
                    'КоличествоЧековСеть': 'int32',

                    'ДеньНедели': 'int32',
                    'День': 'int32',
                    'Месяц': 'int32',
                    'Год': 'int32',

                    'Сезонность': 'str',
                    'Сезонность_точн': 'bool',
                    'Температура (°C)': 'float32',
                    'Давление (мм рт. ст.)': 'float32'
                }
            },
            "Восстановленные_данные_продаж": {
                "pk_columns": ["Дата", "Магазин", "Товар"],
                "column_mapping": {
                    # DataFrame column: DB column
                    'Дата': 'Дата',
                    'Магазин': 'Магазин',
                    'Товар': 'Товар',
                    'Цена': 'Цена',
                    'Акция': 'Акция',
                    'Выходной': 'Выходной',

                    'Категория': 'Категория',
                    'ПотребГруппа': 'ПотребГруппа',
                    'МНН': 'МНН',

                    'Продано': 'Продано_шт',
                    'Остаток': 'Остаток_шт',
                    'Поступило': 'Поступило_шт',
                    'Заказ': 'Заказ_шт',
                    'КоличествоЧеков': 'КоличествоЧеков',

                    'ПроданоСеть': 'ПроданоСеть_шт',
                    'ОстатокСеть': 'ОстатокСеть_шт',
                    'ПоступилоСеть': 'ПоступилоСеть_шт',
                    'КоличествоЧековСеть': 'КоличествоЧековСеть_шт',

                    'ДеньНедели': 'ДеньНедели',
                    'День': 'День',
                    'Месяц': 'Месяц',
                    'Год': 'Год',

                    'Пуассон_распр': 'Пуассон_распр',
                    'Сезонность': 'Сезонность',
                    'Сезонность_точн': 'Сезонность_точн',
                    'Температура (°C)': 'Температура (°C)',
                    'Давление (мм рт. ст.)': 'Давление (мм рт. ст.)',

                    "Медианный_лаг_в_днях" : 'Медианный_лаг_в_днях',
                    "Продано_правка" : 'Продано_правка',
                    "Смоделированные_заказы" : 'Заказы_правка',
                    "Поступило_правка" : 'Поступило_правка',
                    "Остаток_правка" : 'Остаток_правка'
                },
                "type_mapping": {
                    'Дата': 'date',
                    'Магазин': 'str',
                    'Товар': 'str',
                    'Цена': 'float32',
                    'Акция': 'bool',
                    'Выходной': 'bool',

                    'Категория': 'str',
                    'ПотребГруппа': 'str',
                    'МНН': 'str',

                    'Продано': 'int32',
                    'Остаток': 'int32',
                    'Поступило':  'int32',
                    'Заказ': 'int32',
                    'КоличествоЧеков': 'int32',

                    'ПроданоСеть': 'int32',
                    'ОстатокСеть': 'int32',
                    'ПоступилоСеть':  'int32',
                    'КоличествоЧековСеть': 'int32',

                    'ДеньНедели': 'int32',
                    'День': 'int32',
                    'Месяц': 'int32',
                    'Год': 'int32',

                    'Пуассон_распр': 'bool',
                    'Сезонность': 'str',
                    'Сезонность_точн': 'bool',
                    'Температура (°C)': 'float32',
                    'Давление (мм рт. ст.)': 'float32',

                    "Медианный_лаг_в_днях" : 'float32',
                    "Продано_правка" : 'int32',
                    "Смоделированные_заказы" : 'int32',
                    "Поступило_правка" : 'int32',
                    "Остаток_правка" : 'int32'
                }
            },
            "Прогноз": {
                "pk_columns": ["Дата", "Магазин", "Товар"],
                "column_mapping": {
                    # DataFrame column: DB column
                    'Дата': 'Дата',
                    'Магазин': 'Магазин',
                    'Товар': 'Товар',
                    'Предсказанные значения': 'Прогноз'
                },
                "type_mapping": {
                    'Дата': 'date',
                    'Магазин': 'str',
                    'Товар': 'str',
                    'Предсказанные значения': 'int32'
                }
            }
        }

    def _prepare_data(self, df, table_name):
        """Подготовка данных перед загрузкой"""
        if table_name not in self.table_configs:
            raise ValueError(f"Неизвестная таблица: {table_name}")

        config = self.table_configs[table_name]

        # Определяем необязательные столбцы (которые могут отсутствовать)
        optional_columns = {'Температура (°C)', 'Давление (мм рт. ст.)'}
        
        # Проверяем наличие обязательных столбцов в DataFrame
        required_columns = set(config["column_mapping"].keys()) - optional_columns
        missing_cols = required_columns - set(df.columns)
        if missing_cols:
            raise ValueError(f"Отсутствуют обязательные столбцы в DataFrame: {missing_cols}")

        # Добавляем недостающие необязательные столбцы с значениями по умолчанию
        for col in optional_columns:
            if col in config["column_mapping"] and col not in df.columns:
                if col == 'Температура (°C)':
                    df[col] = 0.0
                elif col == 'Давление (мм рт. ст.)':
                    df[col] = 0.0
                print(f"📝 Добавлен столбец {col} со значением по умолчанию")

        # Переименовываем столбцы согласно маппингу
        df = df.rename(columns=config["column_mapping"])

        # Обрабатываем дату отдельно - приводим к формату date
        if 'Дата' in df.columns:
            df['Дата'] = pd.to_datetime(df['Дата']).dt.date

        # Приводим типы данных
        type_mapping = {db_col: dtype
                        for df_col, db_col in config["column_mapping"].items()
                        for dtype in [config["type_mapping"].get(df_col)]
                        if dtype}

        # Применяем типы данных, исключая дату (она уже обработана)
        for col, dtype in type_mapping.items():
            if col in df.columns and col != 'Дата':
                try:
                    if dtype == 'str':
                        df[col] = df[col].astype(str)
                    elif dtype == 'int32':
                        df[col] = df[col].astype('int32')
                    elif dtype == 'float32':
                        df[col] = df[col].astype('float32')
                    elif dtype == 'bool':
                        df[col] = df[col].astype(bool)
                except Exception as e:
                    print(f"⚠️ Ошибка при приведении типа для столбца {col}: {e}")
                    # Продолжаем с исходным типом

        return df

    def _check_existing_data(self, df, table_name):
        """
        Проверяет последние даты в БД и загружает только записи новее последней даты
        
        :param df: DataFrame с данными для проверки
        :param table_name: Название таблицы в БД
        :return: DataFrame только с записями новее последней даты в БД
        """
        if table_name not in self.table_configs:
            raise ValueError(f"Таблица {table_name} не поддерживается")
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    if 'Дата' in df.columns:
                        print(f"🔍 Проверяем последние даты в таблице {table_name}...")
                        print(f"📊 Исходный датасет содержит {len(df)} записей")
                        
                        # Получаем максимальную дату из исходного датасета
                        max_date_df = pd.to_datetime(df['Дата']).max()
                        print(f"📅 Максимальная дата в исходном датасете: {max_date_df.strftime('%Y-%m-%d')}")
                        
                        # Получаем максимальную дату из БД
                        cursor.execute(f"""
                            SELECT MAX("Дата") 
                            FROM "{table_name}"
                        """)
                        result = cursor.fetchone()
                        
                        if result[0] is None:
                            print("📥 Таблица пуста, загружаем все данные")
                            return df
                        
                        max_date_db = result[0]
                        print(f"📅 Максимальная дата в БД: {max_date_db.strftime('%Y-%m-%d')}")
                        
                        # Приводим даты к одному типу для корректного сравнения
                        max_date_df_date = max_date_df.date()
                        max_date_db_date = max_date_db if isinstance(max_date_db, datetime.date) else max_date_db.date()
                        
                        # Сравниваем даты
                        if max_date_df_date <= max_date_db_date:
                            print(f"✅ Все данные уже загружены (последняя дата в БД: {max_date_db.strftime('%Y-%m-%d')})")
                            return pd.DataFrame()  # Возвращаем пустой DataFrame
                        
                        # Фильтруем записи новее последней даты в БД
                        newer_records = df[pd.to_datetime(df['Дата']).dt.date > max_date_db_date].copy()
                        
                        if len(newer_records) > 0:
                            print(f"📥 Найдено {len(newer_records)} записей новее {max_date_db.strftime('%Y-%m-%d')}")
                            print(f"📅 Диапазон новых дат: {newer_records['Дата'].min()} - {newer_records['Дата'].max()}")
                            return newer_records
                        else:
                            print("✅ Нет новых записей для загрузки")
                            return pd.DataFrame()
                    else:
                        print("⚠️ Столбец 'Дата' не найден, загружаем без проверки существующих данных")
                        return df
                        
        except Exception as e:
            print(f"⚠️ Ошибка при проверке последних дат: {str(e)}")
            print("🔄 Продолжаем загрузку без проверки существующих данных...")
            return df
        except RecursionError:
            print("⚠️ Превышена максимальная глубина рекурсии при проверке данных")
            print("🔄 Продолжаем загрузку без проверки существующих данных...")
            return df

    def load_data(self, df, table_name, batch_size=100000, on_conflict_update=True, check_existing=True):
        """
        Универсальный метод для загрузки данных в указанную таблицу

        :param df: DataFrame с данными (с исходными названиями столбцов)
        :param table_name: Название таблицы в БД
        :param batch_size: Размер пакета для вставки
        :param on_conflict_update: Обновлять существующие записи при конфликте
        :param check_existing: Проверять существующие данные перед загрузкой
        """
        try:
            if table_name not in self.table_configs:
                raise ValueError(f"Таблица {table_name} не поддерживается")

            # Проверяем существующие данные, если включено
            if check_existing and len(df) > 0:
                df_original = df.copy()  # Сохраняем оригинальный DataFrame для сравнения
                print(f"🔍 Проверяем существующие данные в таблице {table_name}...")
                df = self._check_existing_data(df, table_name)
                
                # Если нет новых данных для загрузки
                if len(df) == 0:
                    print(f"✅ Все данные уже существуют в таблице {table_name}")
                    return
                elif len(df) < len(df_original):
                    print(f"📥 Загружаем только недостающие записи: {len(df)} из {len(df_original)}")

            # Подготавливаем данные (переименование + приведение типов)
            df = self._prepare_data(df, table_name)
            config = self.table_configs[table_name]

            # Получаем список столбцов в БД после переименования
            db_columns = list(config["column_mapping"].values())

            # Формируем SQL запрос
            insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, db_columns)),
                sql.SQL(', ').join([sql.Placeholder()] * len(db_columns))
            )

            # Добавляем обработку конфликтов если нужно
            if on_conflict_update and config["pk_columns"]:
                conflict_cols = ', '.join([f'"{col}"' for col in config["pk_columns"]])
                update_cols = []

                for df_col, db_col in config["column_mapping"].items():
                    if db_col not in config["pk_columns"]:
                        update_cols.append(f'"{db_col}" = EXCLUDED."{db_col}"')

                if update_cols:
                    insert_sql = sql.SQL("{} ON CONFLICT ({}) DO UPDATE SET {}").format(
                        insert_sql,
                        sql.SQL(conflict_cols),
                        sql.SQL(', ').join(map(sql.SQL, update_cols))
                    )

            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Пакетная вставка
                    def to_scalar(val):
                        if isinstance(val, pd.Series):
                            return val.iloc[0]
                        return val
                    for i in range(0, len(df), batch_size):
                        batch = df.iloc[i:i + batch_size]

                        # Формируем записи с правильным порядком столбцов
                        records = [tuple(to_scalar(row[db_col]) for db_col in db_columns)
                                   for _, row in batch.iterrows()]

                        cursor.executemany(insert_sql, records)
                        conn.commit()
                        print(f"Загружено {min(i + batch_size, len(df))}/{len(df)} записей в {table_name}")

                    print(f"✅ Успешно загружено {len(df)} записей в {table_name}")

        except Exception as e:
            print(f"❌ Ошибка при загрузке данных в {table_name}: {str(e)}")
            raise

    # Специализированные методы для удобства
    def load_to_origin_table(self, df, batch_size=100000, check_existing=True):
        """Загрузка в Исходные_данные_продаж"""
        self.load_data(df, "Исходные_данные_продаж", batch_size, check_existing=check_existing)

    def load_to_enriched_table(self, df, batch_size=100000, check_existing=True):
        """Загрузка в Обогащённые_данные_продаж"""
        self.load_data(df, "Обогащённые_данные_продаж", batch_size, check_existing=check_existing)

    def load_to_recovery_table(self, df, batch_size=100000, check_existing=True):
        """Загрузка в Восстановленные_данные_продаж"""
        self.load_data(df, "Восстановленные_данные_продаж", batch_size, check_existing=check_existing)

    def force_load_to_origin_table(self, df, batch_size=100000):
        """Принудительная загрузка в Исходные_данные_продаж (без проверки существующих)"""
        self.load_data(df, "Исходные_данные_продаж", batch_size, check_existing=False)

    def force_load_to_enriched_table(self, df, batch_size=100000):
        """Принудительная загрузка в Обогащённые_данные_продаж (без проверки существующих)"""
        self.load_data(df, "Обогащённые_данные_продаж", batch_size, check_existing=False)

    def force_load_to_recovery_table(self, df, batch_size=100000):
        """Принудительная загрузка в Восстановленные_данные_продаж (без проверки существующих)"""
        self.load_data(df, "Восстановленные_данные_продаж", batch_size, check_existing=False)

    def load_to_forecast_table(self, df, batch_size=100000, check_existing=True):
        """Загрузка в таблицу Прогноз"""
        self.load_data(df, "Прогноз", batch_size, check_existing=check_existing)

    def force_load_to_forecast_table(self, df, batch_size=100000):
        """Принудительная загрузка в таблицу Прогноз (без проверки существующих)"""
        self.load_data(df, "Прогноз", batch_size, check_existing=False)

class ModelStorage:
    def __init__(self, db_connector):
        self.db = db_connector

    def _compress(self, data):
        """Сжимает данные с помощью gzip"""
        
        return gzip.compress(pickle.dumps(data))

    def _decompress(self, data):
        """Распаковывает данные, сжатые gzip"""
        return pickle.loads(gzip.decompress(data))

    def save_models(self, label_encoder_product, label_encoder_shop, label_encoder_category, label_encoder_potreb_group, label_encoder_mnn, scaler, catboost_model, comment=None, compress=False):
        """
        Сохраняет все энкодеры, скалер и модель CatBoost в таблицу ML_данные_для_работы_модели
        """

        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Сериализация и (опционально) сжатие моделей
                    def serializer(obj):
                        return self._compress(obj) if compress else pickle.dumps(obj)

                    product_bytes = serializer(label_encoder_product)
                    shop_bytes = serializer(label_encoder_shop)
                    category_bytes = serializer(label_encoder_category)
                    potreb_group_bytes = serializer(label_encoder_potreb_group)
                    mnn_bytes = serializer(label_encoder_mnn)
                    scaler_bytes = serializer(scaler)
                    catboost_bytes = serializer(catboost_model)

                    cursor.execute("""
                        INSERT INTO "ML_данные_для_работы_модели" 
                        (label_encoder_product, label_encoder_shop, label_encoder_category, label_encoder_potreb_group, label_encoder_mnn, minmax_scaler, catboost_model, comment)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING load_id
                    """, (product_bytes, shop_bytes, category_bytes, potreb_group_bytes, mnn_bytes, scaler_bytes, catboost_bytes, comment))

                    load_id = cursor.fetchone()[0]
                    conn.commit()
                    print(f"Модели и энкодеры сохранены в таблицу ML_данные_для_работы_модели под ID: {load_id}")
                    return load_id
        except Exception as e:
            print(f"Ошибка сохранения: {str(e)}")
            raise

    def load_latest_models(self, compressed=False):
        """
        Загружает последний набор моделей и энкодеров из таблицы
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT label_encoder_product, label_encoder_shop, label_encoder_category, label_encoder_potreb_group, label_encoder_mnn, minmax_scaler, catboost_model
                        FROM "ML_данные_для_работы_модели"
                        ORDER BY load_id DESC
                        LIMIT 1
                    """)

                    result = cursor.fetchone()
                    if result:
                        def deserializer(data):
                            return self._decompress(data) if compressed else pickle.loads(data)
                        label_encoder_product = deserializer(result[0])
                        label_encoder_shop = deserializer(result[1])
                        label_encoder_category = deserializer(result[2])
                        label_encoder_potreb_group = deserializer(result[3])
                        label_encoder_mnn = deserializer(result[4])
                        scaler = deserializer(result[5])
                        catboost_model = deserializer(result[6])
                        return (label_encoder_product, label_encoder_shop, label_encoder_category, label_encoder_potreb_group, label_encoder_mnn, scaler, catboost_model)
                    raise ValueError("В таблице ML_данные_для_работы_модели нет сохраненных моделей")
        except Exception as e:
            print(f"Ошибка загрузки: {str(e)}")
            raise


    def delete_models(self, load_id):
        """Удаляет набор моделей по ID из таблицы"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        DELETE FROM "ML_данные_для_работы_модели"
                        WHERE load_id = %s
                        RETURNING load_id
                    """, (load_id,))

                    deleted = cursor.fetchone()
                    conn.commit()
                    if deleted:
                        print(f"Набор моделей ID {deleted[0]} удален из ML_данные_для_работы_модели")
                        return True
                    return False
        except Exception as e:
            print(f"Ошибка удаления: {str(e)}")
            raise

    def table_exists(self):
        """Проверяет существование таблицы"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = 'ML_данные_для_работы_модели'
                        )
                    """)
                    return cursor.fetchone()[0]
        except Exception as e:
            print(f"Ошибка проверки таблицы: {str(e)}")
            return False

    def load_models_by_id(self, load_id, compressed=False):
        """
        Загружает модели и энкодеры по конкретному ID из таблицы
        """
        import pickle
        import gzip
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT label_encoder_product, label_encoder_shop, label_encoder_category, label_encoder_potreb_group, label_encoder_mnn, minmax_scaler, catboost_model
                        FROM "ML_данные_для_работы_модели"
                        WHERE load_id = %s
                    """, (load_id,))

                    result = cursor.fetchone()
                    if result:
                        def deserializer(data):
                            return self._decompress(data) if compressed else pickle.loads(data)
                        label_encoder_product = deserializer(result[0])
                        label_encoder_shop = deserializer(result[1])
                        label_encoder_category = deserializer(result[2])
                        label_encoder_potreb_group = deserializer(result[3])
                        label_encoder_mnn = deserializer(result[4])
                        scaler = deserializer(result[5])
                        catboost_model = deserializer(result[6])
                        return (label_encoder_product, label_encoder_shop, label_encoder_category, label_encoder_potreb_group, label_encoder_mnn, scaler, catboost_model)
                    raise ValueError(f"Модели с ID {load_id} не найдены в таблице ML_данные_для_работы_модели")
        except Exception as e:
            print(f"Ошибка загрузки: {str(e)}")
            raise

class DataExtractor:
    def __init__(self, db_connector):
        self.db = db_connector

    def fetch_table(self, table_name, columns=None, where=None, limit=None):
        """
        Универсальный метод для выгрузки данных из таблицы в DataFrame.
        :param table_name: Название таблицы
        :param columns: Список столбцов (по умолчанию все)
        :param where: SQL-условие (строка, например: 'Магазин = %s AND Дата >= %s')
        :param limit: Ограничение по количеству строк
        :return: DataFrame с данными
        """
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cols = '*'
                    if columns:
                        cols = ', '.join([f'"{col}"' for col in columns])
                    query = f'SELECT {cols} FROM "{table_name}"'
                    params = []
                    if where:
                        query += f' WHERE {where[0]}'
                        params = where[1]
                    if limit:
                        query += f' LIMIT {limit}'
                    cursor.execute(query, params)
                    data = cursor.fetchall()
                    colnames = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(data, columns=colnames)
            return df
        except Exception as e:
            print(f"Ошибка при выгрузке из {table_name}: {e}")
            raise

    def fetch_origin_data(self, columns=None, where=None, limit=None):
        return self.fetch_table("Исходные_данные_продаж", columns, where, limit)

    def fetch_enriched_data(self, columns=None, where=None, limit=None):
        return self.fetch_table("Обогащённые_данные_продаж", columns, where, limit)

    def fetch_recovery_data(self, columns=None, where=None, limit=None):
        return self.fetch_table("Восстановленные_данные_продаж", columns, where, limit)


class Last30DaysExtractor:
    def __init__(self, db_connector):
        self.db = db_connector

    def fetch_last_30_days_origin(self):
        """Выгрузка последних 30 дней из таблицы Исходные_данные_продаж"""
        return self._fetch_last_30_days_by_table("Исходные_данные_продаж")

    def fetch_last_30_days_enriched(self):
        """Выгрузка последних 30 дней из таблицы Обогащённые_данные_продаж"""
        return self._fetch_last_30_days_by_table("Обогащённые_данные_продаж")

    def fetch_last_30_days_recovery(self):
        """Выгрузка последних 30 дней из таблицы Восстановленные_данные_продаж"""
        return self._fetch_last_30_days_by_table("Восстановленные_данные_продаж")

    def _fetch_last_30_days_by_table(self, table_name):
        """Внутренняя функция для выгрузки последних 30 дней по дате"""
        import pandas as pd
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f'SELECT DISTINCT "Дата" FROM "{table_name}" ORDER BY "Дата" DESC LIMIT 30'
                    )
                    last_dates = [row[0] for row in cursor.fetchall()]
                    if not last_dates:
                        print(f"Нет данных в таблице {table_name}")
                        return pd.DataFrame()
                    min_date = min(last_dates)
                    cursor.execute(
                        f'SELECT * FROM "{table_name}" WHERE "Дата" >= %s',
                        (min_date,)
                    )
                    data = cursor.fetchall()
                    colnames = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(data, columns=colnames)
            return df
        except Exception as e:
            print(f"Ошибка при выгрузке из {table_name}: {e}")
            raise



def get_db_connection(config):
    """Инициализирует и возвращает DBConnector для локальной БД"""
    return DBConnector(
        db_host=config['db_host'],
        db_port=config['db_port'],
        db_name=config['db_name'],
        db_user=config['db_user'],
        db_password=config['db_password']
    )