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
from DB_operations import ModelStorage
from DB_operations import Last30DaysExtractor
from DB_operations import DataExtractor
from fastapi import FastAPI, APIRouter
from main import create_tables
import uvicorn

def connection_to_db():
    db = get_db_connection(DB_CONFIG)
    return db

# Конфигурация подключения к локальной БД
DB_CONFIG = {
    'db_host': "localhost",  # или "127.0.0.1"
    'db_port': 5432,         # стандартный порт PostgreSQL
    'db_name': "BD_Dobroteka",  # имя вашей локальной БД
    'db_user': "postgres",   # пользователь БД
    'db_password': "1234"  # пароль от БД
}

app = FastAPI()
router = APIRouter(prefix="/model-train", tags=["Model Training"])

@app.get("/")
def root():
    """
    Главная страница с информацией о доступных эндпоинтах.
    """
    return {
        "message": "API для прогнозирования продаж",
        "endpoints": {
            "create_tables": "/create-tables",
            "clean_data": "/model-train/clean-data",
            "recover_data": "/model-train/recover-data", 
            "train_model": "/model-train/train-model"
        }
    }

@app.post("/create-tables")
def create_tables_route():
    """
    Эндпоинт для создания таблиц в базе данных.
    """
    try:
        db = get_db_connection(DB_CONFIG)
        create_tables(db)
        return {"message": "Таблицы успешно созданы в базе данных!"}
    except Exception as e:
        return {"error": f"Ошибка при создании таблиц: {str(e)}"}

@router.post("/load-origin-data")
def load_data():
    """
    Эндпоинт для загрузки данных в базу данных.
    """
    try:
        db = get_db_connection(DB_CONFIG)
        data_loader = DataLoader(db)
        df_first = pd.read_csv("train_df.csv", parse_dates=["Дата"])
        data_loader.load_to_origin_table(df_first, batch_size=100000)
        return {"message": "Исходные данные успешно загружены в локальную БД!"}
    except Exception as e:
        return {"error": f"Ошибка при загрузке данных: {str(e)}"}

@router.post("/clean-data")
def clean_data():
    """
    Эндпоинт для отчистки данных (первичная обработка).
    """
    try:
        # Получение полных данных из локальной БД
        db = get_db_connection(DB_CONFIG)
        data_extractor = DataExtractor(db)
        df_first = data_extractor.fetch_origin_data()

        # Очистка данных
        processor = Preprocessing_data()
        df_clean = processor.first_preprocess_data(df_first)

        # Загрузка очищенных данных в локальную БД
        print("Загрузка очищенных данных в локальную БД...")
        data_loader = DataLoader(db)
        data_loader.load_to_enriched_table(df_clean, batch_size=100000)
        print("Очищенные данные успешно загружены в локальную БД!")

        return {
            "message": "Данные успешно очищены и загружены в БД!", 
            "rows": len(df_clean),
            "database": "Данные сохранены в таблицу enriched_data"
        }
    except Exception as e:
        return {"error": f"Ошибка при очистке данных: {str(e)}"}

@router.post("/recover-data")
def recover_data():
    """
    Эндпоинт для восстановления данных.
    """
    try:
        # Получение полных данных из локальной БД
        db = get_db_connection(DB_CONFIG)
        data_extractor = DataExtractor(db)
        df_clean = data_extractor.fetch_enriched_data()
        print(df_clean.info())

        # Восстановление данных
        sales_recovery = Recovery_sales()
        df_recovery = sales_recovery.first_full_sales_recovery(df_clean)

        # Загрузка данных в локальную БД
        print("Загрузка восстановленных данных в локальную БД...")
        data_loader = DataLoader(db)
        data_loader.load_to_recovery_table(df_recovery, batch_size=100000)
        print("Восстановленные данные успешно загружены в локальную БД!")

        return {
            "message": "Данные успешно восстановлены и загружены в БД!", 
            "rows": len(df_recovery),
            "database": "Данные сохранены в таблицу recovery_data"
        }
    except Exception as e:
        return {"error": f"Ошибка при восстановлении данных: {str(e)}"}

@router.post("/train-model")
def train_model():
    """
    Эндпоинт для обучения модели.
    """
    try:
        # Получение полных данных из локальной БД
        db = get_db_connection(DB_CONFIG)
        data_extractor = DataExtractor(db)
        df_recovery = data_extractor.fetch_recovery_data()

        # Обучение модели
        first_model_learn = First_learning_model()
        df_preduction = first_model_learn.first_learning_model(df_recovery, db)

        return {
            "message": "Модель успешно обучена и данные загружены в БД!", 
            "rows": len(df_preduction),
            "database": "Данные сохранены в таблицу ml_data"
        }
    except Exception as e:
        return {"error": f"Ошибка при обучении модели: {str(e)}"}

app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)