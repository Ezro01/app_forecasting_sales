"""
Локальный скрипт для обучения и тестирования модели.
Использует конфигурацию из переменных окружения.
"""
import pandas as pd
import numpy as np
import logging
from Preprocessing import Preprocessing_data
from Sales_recovery import Recovery_sales
from First_model_learning import First_learning_model
from Next_model_predict import Use_model_predict
from DB_Connector import DBConnector
from DB_operations import Create_tables
from DB_operations import DataLoader
from DB_operations import get_db_connection
from DB_operations import ModelStorage
from DB_operations import Last30DaysExtractor
from config import DB_CONFIG, DATA_CONFIG

# Настройка логирования
from config import LOG_LEVEL
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_tables(db):
    """Создает все необходимые таблицы в базе данных."""
    create_tables_obj = Create_tables()
    logger.info("Создание таблиц в локальной БД...")
    create_tables_obj.create_origin_data_table(db)
    create_tables_obj.create_enriched_data_table(db)
    create_tables_obj.create_recovery_data_table(db)
    create_tables_obj.saved_ml_data_table(db)
    create_tables_obj.create_forecast_table(db)
    logger.info("Все таблицы успешно созданы")

def first_model_learn(df_first, db):
    """Обучает модель на исходных данных."""
    df_first_copy = df_first.copy()
    df_first_copy = df_first_copy.sort_values(by=['Дата', 'Магазин', 'Товар'])

    data_loader = DataLoader(db)
    processor = Preprocessing_data()
    sales_recovery = Recovery_sales()
    first_model_learn_obj = First_learning_model()

    # Загрузка данных в локальную БД
    logger.info("Загрузка исходных данных в локальную БД...")
    data_loader.load_to_origin_table(df_first_copy, batch_size=100000)
    logger.info("Исходные данные успешно загружены в локальную БД!")

    # Предобработка данных
    logger.info("Предобработка данных...")
    df_clean = processor.first_preprocess_data(df_first_copy)

    # Восстановление продаж
    logger.info("Восстановление продаж...")
    df_recovery = sales_recovery.first_full_sales_recovery(df_clean)

    logger.info("Загрузка восстановленных данных в локальную БД...")
    data_loader.load_to_recovery_table(df_recovery, batch_size=100000)
    logger.info("Восстановленные данные успешно загружены в локальную БД!")

    # Обучение модели
    logger.info("Обучение модели...")
    df_preduction = first_model_learn_obj.first_learning_model(df_recovery, db)
    logger.info("Обучение модели завершено")

def use_model_predict(df_first, df_next, df_season_sales, db):
    """Использует обученную модель для предсказания."""
    df_first_copy = df_first.copy()
    df_next_copy = df_next.copy()
    df_season_sales_copy = df_season_sales.copy()

    processor = Preprocessing_data()
    sales_recovery = Recovery_sales()
    use_model_prediction = Use_model_predict()
    data_loader = DataLoader(db)

    # Загрузка данных в локальную БД
    logger.info("Загрузка новых данных в локальную БД...")
    data_loader.load_to_origin_table(df_next_copy, batch_size=100000)
    logger.info("Новые данные успешно загружены в локальную БД!")

    # Предобработка данных
    logger.info("Предобработка новых данных...")
    df_clean = processor.next_preprocess_data(df_first_copy, df_next_copy, df_season_sales_copy)

    # Восстановление продаж
    logger.info("Восстановление продаж для новых данных...")
    df_recovery = sales_recovery.next_full_sales_recovery(df_first_copy, df_clean, df_season_sales_copy)

    logger.info("Загрузка восстановленных данных в локальную БД...")
    data_loader.load_to_recovery_table(df_recovery, batch_size=100000)
    logger.info("Восстановленные данные успешно загружены в локальную БД!")

    # Предсказание
    logger.info("Выполнение предсказания...")
    df_preduction = use_model_prediction.use_model_predict(df_first_copy, df_recovery, df_season_sales_copy, db)

    # Загрузка прогноза в БД
    logger.info("Загрузка прогноза в локальную БД...")
    data_loader.load_to_forecast_table(df_preduction, batch_size=100000)
    logger.info("Прогноз успешно загружен в локальную БД!")


def main():
    """Основная функция для запуска обучения и предсказания модели."""
    try:
        # Инициализация подключения к локальной БД
        logger.info("Подключение к базе данных...")
        db = get_db_connection(DB_CONFIG)
        
        # Создание таблиц в локальной БД
        create_tables(db)

        # Загрузка данных
        logger.info("Загрузка данных из CSV файлов...")
        train_path = DATA_CONFIG['train_data_path']
        test_path = DATA_CONFIG['test_data_path']
        logger.info(f"Загрузка обучающих данных из: {train_path}")
        logger.info(f"Загрузка тестовых данных из: {test_path}")
        df_first = pd.read_csv(train_path, parse_dates=["Дата"])
        df_next = pd.read_csv(test_path, parse_dates=["Дата"])
        logger.info(f"Загружено {len(df_first)} строк для обучения и {len(df_next)} строк для тестирования")

        # Обучение модели
        logger.info("Начало обучения модели...")
        first_model_learn(df_first, db)
        
        # Получение последних 30 дней для предсказания
        logger.info("Получение данных за последние 30 дней...")
        last_30_days_extractor = Last30DaysExtractor(db)
        df_last_30_days_origin = last_30_days_extractor.fetch_last_30_days_origin()
        df_last_30_days_recovery = last_30_days_extractor.fetch_last_30_days_recovery()

        # Предсказание
        logger.info("Начало предсказания...")
        use_model_predict(df_last_30_days_origin, df_next, df_last_30_days_recovery, db)
        logger.info("Предсказание завершено")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()