import pandas as pd
import numpy as np
import time
import os
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
from SFTP_Connector import SFTPDataLoader
from fastapi import FastAPI, APIRouter, UploadFile, File, HTTPException
from main_local import create_tables
import uvicorn
from dotenv import load_dotenv
from pathlib import Path

def connection_to_db():
    db = get_db_connection(DB_CONFIG)
    return db

load_dotenv('.env')
def get_required_env(name):
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"Environment variable {name} is required")
    return value

# Конфигурация подключения к локальной БД
DB_CONFIG = {
    'db_host': get_required_env('DB_HOST'),
    'db_port': int(get_required_env('DB_PORT')),
    'db_name': get_required_env('DB_NAME'),
    'db_user': get_required_env('DB_USER'),
    'db_password': get_required_env('DB_PASSWORD')
}

def get_ssh_key():
    """Автовыбор ключа по окружению (CI/CD или локальное)"""
    env_type = os.getenv('ENV_TYPE', 'local').lower()  # приводим к нижнему регистру
    
    # Проверяем допустимые значения env_type
    if env_type not in ('local', 'stage', 'prod'):
        raise ValueError(f"Invalid ENV_TYPE: {env_type}. Must be 'local', 'stage' or 'prod'")
    
    # Вариант 1: Ключ из переменной (для GitLab CI)
    if key_content := os.getenv(f"SSH_KEY_{env_type.upper()}"):
        key_path = f"/tmp/ssh_key_{env_type}"
        try:
            with open(key_path, 'w') as f:
                f.write(key_content)
            os.chmod(key_path, 0o600)
            return key_path
        except IOError as e:
            raise IOError(f"Failed to create temporary SSH key file: {e}")
    
    # Вариант 2: Локальные файлы для macOS/Linux (только для local)
    if env_type == 'local':
        # Сначала проверяем стандартный путь для локальной машины
        local_key_path = Path('~/.ssh/id_ed25519').expanduser()
        if local_key_path.exists():
            return str(local_key_path)
        # Также проверяем другие возможные ключи
        for key_name in ['id_rsa', 'id_ed25519', 'id_ecdsa']:
            key_path = Path(f'~/.ssh/{key_name}').expanduser()
            if key_path.exists():
                return str(key_path)
        # Вариант 3: Локальные файлы для Docker (только для local-docker)
        docker_key_path = Path('/home/appuser/.ssh/id_ed25519')
        if docker_key_path.exists():
            return str(docker_key_path)
    
    raise FileNotFoundError(f"No SSH key found for {env_type} environment")


SFTP_CONFIG = {
    'host': get_required_env('SFTP_HOST'),
    'port': int(get_required_env('SFTP_PORT')),
    'username': get_required_env('SFTP_USERNAME'),
    'key_filename': get_ssh_key()
}

app = FastAPI()
router_main = APIRouter(prefix="/main", tags=["Main"])
router_train = APIRouter(prefix="/model-train", tags=["Model Training"])
router_predict = APIRouter(prefix="/model-predict", tags=["Model Prediction"])

@router_main.get("/")
def root():
    """
    Главная страница с информацией о доступных эндпоинтах.
    """
    return {
        "message": "API для прогнозирования продаж - Расширенная версия",
        "endpoints": {
            "create_tables": "/main/create-tables",
            "sftp_list_files": "/main/list-files",
            "load_origin_data": "/model-train/load-origin-data",
            "clean_data": "/model-train/clean-data",
            "recover_data": "/model-train/recover-data", 
            "train_model": "/model-train/train-model",
            "predict_new_data": "/model-predict/predict-new-data (поддерживает SFTP)",
        }
    }

@router_main.post("/create-tables")
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

@router_main.get("/list-files")
def list_sftp_files(remote_directory: str = "/"):
    """
    Получение списка файлов на SFTP сервере
    """
    try:
        sftp_loader = SFTPDataLoader(SFTP_CONFIG)
        
        if not sftp_loader.connect():
            raise HTTPException(status_code=500, detail="Не удалось подключиться к SFTP серверу")
        
        try:
            files = sftp_loader.list_available_files(remote_directory)
            return {
                "message": "Список файлов получен успешно",
                "directory": remote_directory,
                "files": files,
                "total_files": len(files)
            }
        finally:
            sftp_loader.disconnect()
            
    except Exception as e:
        return {"error": f"Ошибка при получении списка файлов: {str(e)}"}

@router_train.post("/load-origin-data")
def load_data_train(remote_file_path: str):
    """
    Эндпоинт для загрузки данных с SFTP сервера в базу данных.
    """
    try:
        # 1. Подключаемся к SFTP и загружаем данные
        sftp_loader = SFTPDataLoader(SFTP_CONFIG)
        
        if not sftp_loader.connect():
            raise HTTPException(status_code=500, detail="Не удалось подключиться к SFTP серверу")
        
        try:
            # Загружаем данные с SFTP
            df_first = sftp_loader.load_new_data_from_sftp(remote_file_path)
            
            if df_first is None:
                raise HTTPException(status_code=404, detail=f"Не удалось загрузить файл {remote_file_path} с SFTP сервера")
            
            # 2. Подключаемся к БД и сохраняем данные
            db = get_db_connection(DB_CONFIG)
            data_loader = DataLoader(db)
            
            # 3. Загружаем данные в БД
            data_loader.load_to_origin_table(df_first, batch_size=100000)
            
            return {
                "message": "Исходные данные успешно загружены с SFTP в локальную БД!",
                "source_file": remote_file_path,
                "rows_loaded": len(df_first),
                "database": "Данные сохранены в таблицу origin_data"
            }
            
        finally:
            sftp_loader.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        return {"error": f"Ошибка при загрузке данных: {str(e)}"}

@router_train.post("/clean-data")
def clean_data_train():
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

@router_train.post("/recover-data")
def recover_data_train():
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

@router_train.post("/train-model")
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


@router_predict.post("/predict-new-data")
def data_predict(remote_file_path: str = None, upload_to_sftp: bool = False, sftp_output_path: str = None):
    """
    Эндпоинт для прогнозирования с данными с SFTP сервера.
    
    Args:
        remote_file_path: Путь к файлу на SFTP сервере (если None, используется локальный test_df.csv)
        upload_to_sftp: Загружать ли результат на SFTP сервер
        sftp_output_path: Путь для сохранения результата на SFTP сервере
    """
    try:
        db = get_db_connection(DB_CONFIG)
        processor = Preprocessing_data()
        sales_recovery = Recovery_sales()
        use_model_prediction = Use_model_predict()
        data_loader = DataLoader(db)
        
        # Загружаем данные (с SFTP или локально)
        if remote_file_path:
            # Загружаем с SFTP сервера
            sftp_loader = SFTPDataLoader(SFTP_CONFIG)
            if not sftp_loader.connect():
                return {"error": "Не удалось подключиться к SFTP серверу"}
            
            df_next = sftp_loader.load_new_data_from_sftp(remote_file_path)
            if df_next is None:
                return {"error": f"Не удалось загрузить файл {remote_file_path} с SFTP сервера"}
            
            sftp_loader.disconnect()
        else:
            return {"error": f"Не удалось загрузить файл {remote_file_path} с SFTP сервера"}
        
        # Загружаем в таблицу origin_data
        try:
            data_loader.load_to_origin_table(df_next, batch_size=100000)
        except Exception as e:
            return {"error": f"Ошибка при загрузке данных в origin_data: {str(e)}"}
        
        # Получаем данные за последние 30 дней
        df_last_30_days_origin, df_last_30_days_recovery = last_30_days_extractor()
        
        # Очищаем данные
        df_clean = processor.next_preprocess_data(df_last_30_days_origin, df_next, df_last_30_days_recovery)
        
        # Загружаем в таблицу enriched_data
        try:
            data_loader.load_to_enriched_table(df_clean, batch_size=100000)
        except Exception as e:
            return {"error": f"Ошибка при загрузке данных в enriched_data: {str(e)}"}
        
        # Восстанавливаем продажи
        df_recovery = sales_recovery.next_full_sales_recovery(df_last_30_days_origin, df_clean, df_last_30_days_recovery)
        
        # Загружаем в таблицу recovery_data
        try:
            data_loader.load_to_recovery_table(df_recovery, batch_size=100000)
        except Exception as e:
            return {"error": f"Ошибка при загрузке данных в recovery_data: {str(e)}"}
        
        # Делаем прогноз
        df_preduction = use_model_prediction.use_model_predict(df_last_30_days_origin, df_recovery, df_last_30_days_recovery, db)
        
        # Загружаем в таблицу forecast_data
        try:
            data_loader.load_to_forecast_table(df_preduction, batch_size=100000)
        except Exception as e:
            return {"error": f"Ошибка при загрузке данных в forecast_data: {str(e)}"}
        
        # Загружаем результат на SFTP сервер (если требуется)
        if upload_to_sftp and sftp_output_path:
            try:
                sftp_loader = SFTPDataLoader(SFTP_CONFIG)
                if sftp_loader.connect():
                    success = sftp_loader.upload_predictions_to_sftp(df_preduction, sftp_output_path)
                    sftp_loader.disconnect()
                    
                    if success:
                        return {
                            "message": "Прогноз успешно сделан, данные загружены в БД и на SFTP сервер",
                            "rows": len(df_clean),
                            "database": "Данные сохранены в таблицу forecast_data",
                            "sftp": f"Результат загружен на SFTP: {sftp_output_path}"
                        }
                    else:
                        return {
                            "message": "Прогноз успешно сделан и данные загружены в БД, но не удалось загрузить на SFTP",
                            "rows": len(df_clean),
                            "database": "Данные сохранены в таблицу forecast_data",
                            "sftp_error": "Не удалось загрузить на SFTP сервер"
                        }
                else:
                    return {
                        "message": "Прогноз успешно сделан и данные загружены в БД, но не удалось подключиться к SFTP",
                        "rows": len(df_clean),
                        "database": "Данные сохранены в таблицу forecast_data",
                        "sftp_error": "Не удалось подключиться к SFTP серверу"
                    }
            except Exception as sftp_error:
                return {
                    "message": "Прогноз успешно сделан и данные загружены в БД, но ошибка при работе с SFTP",
                    "rows": len(df_clean),
                    "database": "Данные сохранены в таблицу forecast_data",
                    "sftp_error": str(sftp_error)
                }
        
        return {
            "message": "Прогноз успешно сделан и данные загружены в БД",
            "rows": len(df_clean),
            "database": "Данные сохранены в таблицу forecast_data"
        }
        
    except Exception as e:
        return {"error": f"Ошибка: {str(e)}"}


def last_30_days_extractor():
    db = get_db_connection(DB_CONFIG)
    last_30_days_extractor = Last30DaysExtractor(db)
    df_last_30_days_origin = last_30_days_extractor.fetch_last_30_days_origin()
    df_last_30_days_recovery = last_30_days_extractor.fetch_last_30_days_recovery()
    return df_last_30_days_origin, df_last_30_days_recovery


app.include_router(router_main)
app.include_router(router_train)
app.include_router(router_predict)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)