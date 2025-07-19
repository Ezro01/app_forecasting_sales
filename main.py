import pandas as pd
import numpy as np
import time
from Preprocessing import Preprocessing_data
from Sales_recovery import Recovery_sales
from First_model_learning import First_learning_model
from Next_model_predict import Use_model_predict
from DB_Connector import DBConnector
from DB_operations import Create_tables
from DB_operations import DataLoader
from DB_operations import get_db_connection

def first_model_learn(df_first):
    df_first_copy = df_first.copy()

    processor = Preprocessing_data()
    sales_recovery = Recovery_sales()
    first_model_learn = First_learning_model()
    create_tables = Create_tables()


    df_clean = processor.first_preprocess_data(df_first_copy)
    df_recovery = sales_recovery.first_full_sales_recovery(df_clean)
    df_preduction = first_model_learn.first_learning_model(df_recovery)

    return df_preduction

def use_model_predict(df_first, df_next):
    df_first_copy = df_first.copy()
    df_next_copy = df_next.copy()

    processor = Preprocessing_data()
    sales_recovery = Recovery_sales()
    use_model_prediction = Use_model_predict()

    df_clean = processor.next_preprocess_data(df_first_copy, df_next_copy)
    df_recovery = sales_recovery.next_full_sales_recovery(df_first_copy, df_clean)
    df_preduction = use_model_prediction.use_model_predict(df_first_copy, df_recovery)

    return df_preduction

# Конфигурация подключения к локальной БД
DB_CONFIG = {
    'db_host': "localhost",  # или "127.0.0.1"
    'db_port': 5432,         # стандартный порт PostgreSQL
    'db_name': "BD_Dobroteka",  # имя вашей локальной БД
    'db_user': "postgres",   # пользователь БД
    'db_password': "1234"  # пароль от БД
}

def main():
    # Инициализация подключения к локальной БД
    db = get_db_connection(DB_CONFIG)
    create_tables = Create_tables()
    data_loader = DataLoader(db)

    # Создание таблиц в локальной БД
    print("Создание таблиц в локальной БД...")
    create_tables.create_origin_data_table(db)
    create_tables.create_enriched_data_table(db)
    create_tables.create_recovery_data_table(db)
    create_tables.saved_ml_data_table(db)

    df_first = pd.read_csv("Dataframe_500_tovars_magazins.csv", parse_dates=["Дата"])
    df_next = pd.read_csv("test_df.csv", parse_dates=["Дата"])

    # Загрузка данных в локальную БД
    print("Загрузка данных в локальную БД...")
    data_loader.load_data(df_first, "Исходные_данные_продаж")
    print("Данные успешно загружены в локальную БД!")
    
    # first_learning_model = first_model_learn(df_first)


if __name__ == "__main__":
    main()





