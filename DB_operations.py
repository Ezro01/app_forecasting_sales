from DB_Connector import DBConnector
import pickle
import gzip

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