from DB_Connector import DBConnector
import pickle
import gzip
import pandas as pd
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
                                
                                CONSTRAINT data_pk PRIMARY KEY ("Дата", "Магазин", "Товар")
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
                                
                                CONSTRAINT data_pk PRIMARY KEY ("Дата", "Магазин", "Товар")
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
                                
                                "Сезонность" varchar(50) NOT NULL,
                                "Сезонность_точн" bool NOT NULL,
                                "Температура (°C)" float4 NOT NULL,
                                "Давление (мм рт. ст.)" float4 NOT NULL,
                                
                                "Медианный_лаг_в_днях" float4 NOT NULL,
                                "Продано_правка" int4 NOT NULL,
                                "Заказы_правка" int4 NOT NULL,
                                "Поступило_правка" int4 NOT NULL,
                                "Остаток_правка" int4 NOT NULL,
                                
                                CONSTRAINT data_pk PRIMARY KEY ("Дата", "Магазин", "Товар")
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

    def create_saved_ml_data_table(self, db_connector):
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
                    'ПоступилоСеть': 'ПоступилоСеть_шт',
                    'КоличествоЧековСеть': 'КоличествоЧековСеть_шт'
                },
                "type_mapping": {
                    'Дата': 'datetime64[ns]',
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
                    'Заказ': 'Заказ_шт',
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
                    'Дата': 'datetime64[ns]',
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
                    'Дата': 'datetime64[ns]',
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
                    'Давление (мм рт. ст.)': 'float32',

                    "Медианный_лаг_в_днях" : 'float32',
                    "Продано_правка" : 'int32',
                    "Смоделированные_заказы" : 'int32',
                    "Поступило_правка" : 'int32',
                    "Остаток_правка" : 'int32'
                }
            }
        }

    def _prepare_data(self, df, table_name):
        """Подготовка данных перед загрузкой"""
        if table_name not in self.table_configs:
            raise ValueError(f"Неизвестная таблица: {table_name}")

        config = self.table_configs[table_name]

        # Проверяем наличие всех необходимых столбцов в DataFrame
        missing_cols = set(config["column_mapping"].keys()) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Отсутствуют обязательные столбцы в DataFrame: {missing_cols}")

        # Переименовываем столбцы согласно маппингу
        df = df.rename(columns=config["column_mapping"])

        # Приводим типы данных
        type_mapping = {db_col: dtype
                        for df_col, db_col in config["column_mapping"].items()
                        for dtype in [config["type_mapping"].get(df_col)]
                        if dtype}

        return df.astype({col: dtype for col, dtype in type_mapping.items() if col in df.columns})

    def load_data(self, df, table_name, batch_size=1000, on_conflict_update=True):
        """
        Универсальный метод для загрузки данных в указанную таблицу

        :param df: DataFrame с данными (с исходными названиями столбцов)
        :param table_name: Название таблицы в БД
        :param batch_size: Размер пакета для вставки
        :param on_conflict_update: Обновлять существующие записи при конфликте
        """
        try:
            if table_name not in self.table_configs:
                raise ValueError(f"Таблица {table_name} не поддерживается")

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
                    for i in range(0, len(df), batch_size):
                        batch = df.iloc[i:i + batch_size]

                        # Формируем записи с правильным порядком столбцов
                        records = [tuple(row[db_col] for db_col in db_columns)
                                   for _, row in batch.iterrows()]

                        cursor.executemany(insert_sql, records)
                        conn.commit()
                        print(f"Загружено {min(i + batch_size, len(df))}/{len(df)} записей в {table_name}")

                    print(f"✅ Успешно загружено {len(df)} записей в {table_name}")

        except Exception as e:
            print(f"❌ Ошибка при загрузке данных в {table_name}: {str(e)}")
            raise

    # Специализированные методы для удобства
    def load_to_origin_table(self, df, batch_size=1000):
        """Загрузка в Исходные_данные_продаж"""
        self.load_data(df, "Исходные_данные_продаж", batch_size)

    def load_to_enriched_table(self, df, batch_size=1000):
        """Загрузка в Обогащённые_данные_продаж"""
        self.load_data(df, "Обогащённые_данные_продаж", batch_size)

    def load_to_recovery_table(self, df, batch_size=1000):
        """Загрузка в Восстановленные_данные_продаж"""
        self.load_data(df, "Восстановленные_данные_продаж", batch_size)

class ModelStorage:
    def __init__(self, db_connector):
        self.db = db_connector

    def _compress(self, data):
        """Сжимает данные с помощью gzip"""
        return gzip.compress(pickle.dumps(data))

    def _decompress(self, data):
        """Распаковывает данные, сжатые gzip"""
        return pickle.loads(gzip.decompress(data))

    def save_models(self, product_encoder, shop_encoder, catboost_model, comment=None, compress=False):
        """Сохраняет все модели в таблицу ML_данные_для_работы_модели"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Сериализация и (опционально) сжатие моделей
                    serializer = self._compress if compress else pickle.dumps

                    product_bytes = serializer(product_encoder)
                    shop_bytes = serializer(shop_encoder)
                    catboost_bytes = serializer(catboost_model)

                    cursor.execute("""
                        INSERT INTO "ML_данные_для_работы_модели" 
                        (label_encoder_product, label_encoder_shop, catboost_model, comment)
                        VALUES (%s, %s, %s, %s)
                        RETURNING load_id
                    """, (product_bytes, shop_bytes, catboost_bytes, comment))

                    load_id = cursor.fetchone()[0]
                    conn.commit()
                    print(f"Модели сохранены в таблицу ML_данные_для_работы_модели под ID: {load_id}")
                    return load_id
        except Exception as e:
            print(f"Ошибка сохранения: {str(e)}")
            raise

    def load_latest_models(self, compressed=False):
        """Загружает последний набор моделей из таблицы"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT label_encoder_product, label_encoder_shop, catboost_model
                        FROM "ML_данные_для_работы_модели"
                        ORDER BY load_id DESC
                        LIMIT 1
                    """)

                    result = cursor.fetchone()
                    if result:
                        deserializer = self._decompress if compressed else pickle.loads
                        return (
                            deserializer(result[0]),
                            deserializer(result[1]),
                            deserializer(result[2])
                        )
                    raise ValueError("В таблице ML_данные_для_работы_модели нет сохраненных моделей")
        except Exception as e:
            print(f"Ошибка загрузки: {str(e)}")
            raise

    def load_models_by_id(self, load_id, compressed=False):
        """Загружает модели по конкретному ID из таблицы"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT label_encoder_product, label_encoder_shop, catboost_model
                        FROM "ML_данные_для_работы_модели"
                        WHERE load_id = %s
                    """, (load_id,))

                    result = cursor.fetchone()
                    if result:
                        deserializer = self._decompress if compressed else pickle.loads
                        return (
                            deserializer(result[0]),
                            deserializer(result[1]),
                            deserializer(result[2])
                        )
                    raise ValueError(f"Модели с ID {load_id} не найдены в таблице ML_данные_для_работы_модели")
        except Exception as e:
            print(f"Ошибка загрузки: {str(e)}")
            raise

    def get_model_history(self, limit=5):
        """Возвращает историю сохранений из таблицы"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT load_id, created_at, comment
                        FROM "ML_данные_для_работы_модели"
                        ORDER BY load_id DESC
                        LIMIT %s
                    """, (limit,))
                    return [
                        {
                            'id': row[0],
                            'date': row[1],
                            'comment': row[2]
                        }
                        for row in cursor.fetchall()
                    ]
        except Exception as e:
            print(f"Ошибка получения истории: {str(e)}")
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