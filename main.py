"""
FastAPI приложение для прогнозирования продаж.
Основной модуль с API эндпоинтами для обучения и использования модели.
"""
import logging
from typing import Optional
from fastapi import FastAPI, APIRouter, HTTPException
import uvicorn

from Preprocessing import Preprocessing_data
from Sales_recovery import Recovery_sales
from First_model_learning import First_learning_model
from Next_model_predict import Use_model_predict
from DB_operations import DataLoader, get_db_connection, Last30DaysExtractor, DataExtractor
from SFTP_Connector import SFTPDataLoader
from main_local import create_tables
from config import DB_CONFIG, SFTP_CONFIG, APP_CONFIG, LOG_LEVEL

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация FastAPI приложения
app = FastAPI(
    title="Sales Forecasting API",
    description="API для прогнозирования продаж",
    version="1.0.0"
)

# Создание роутеров
router_main = APIRouter(prefix="/main", tags=["Main"])
router_train = APIRouter(prefix="/model-train", tags=["Model Training"])
router_predict = APIRouter(prefix="/model-predict", tags=["Model Prediction"])


def get_db():
    """Получает подключение к базе данных."""
    return get_db_connection(DB_CONFIG)

@router_main.get("/")
def root():
    """Главная страница с информацией о доступных эндпоинтах."""
    return {
        "message": "API для прогнозирования продаж",
        "version": "1.0.0",
        "endpoints": {
            "create_tables": "/main/create-tables",
            "sftp_list_files": "/main/list-files",
            "load_origin_data": "/model-train/load-origin-data",
            "clean_data": "/model-train/clean-data",
            "recover_data": "/model-train/recover-data",
            "train_model": "/model-train/train-model",
            "predict_new_data": "/model-predict/predict-new-data",
        }
    }

@router_main.post("/create-tables")
def create_tables_route():
    """Эндпоинт для создания таблиц в базе данных."""
    try:
        logger.info("Создание таблиц в базе данных...")
        db = get_db()
        create_tables(db)
        logger.info("Таблицы успешно созданы")
        return {"message": "Таблицы успешно созданы в базе данных!"}
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при создании таблиц: {str(e)}")

@router_main.get("/list-files")
def list_sftp_files(remote_directory: str = "/"):
    """Получение списка файлов на SFTP сервере."""
    try:
        logger.info(f"Получение списка файлов из директории: {remote_directory}")
        sftp_loader = SFTPDataLoader(SFTP_CONFIG)
        
        if not sftp_loader.connect():
            raise HTTPException(status_code=500, detail="Не удалось подключиться к SFTP серверу")
        
        try:
            files = sftp_loader.list_available_files(remote_directory)
            logger.info(f"Найдено {len(files)} файлов")
            return {
                "message": "Список файлов получен успешно",
                "directory": remote_directory,
                "files": files,
                "total_files": len(files)
            }
        finally:
            sftp_loader.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении списка файлов: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при получении списка файлов: {str(e)}")

@router_train.post("/load-origin-data")
def load_data_train(remote_file_path: str):
    """Эндпоинт для загрузки данных с SFTP сервера в базу данных."""
    try:
        logger.info(f"Загрузка данных с SFTP: {remote_file_path}")
        # 1. Подключаемся к SFTP и загружаем данные
        sftp_loader = SFTPDataLoader(SFTP_CONFIG)
        
        if not sftp_loader.connect():
            raise HTTPException(status_code=500, detail="Не удалось подключиться к SFTP серверу")
        
        try:
            # Загружаем данные с SFTP
            df_first = sftp_loader.load_new_data_from_sftp(remote_file_path)
            
            if df_first is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Не удалось загрузить файл {remote_file_path} с SFTP сервера"
                )
            
            # 2. Подключаемся к БД и сохраняем данные
            db = get_db()
            data_loader = DataLoader(db)
            
            # 3. Загружаем данные в БД
            data_loader.load_to_origin_table(df_first, batch_size=100000)
            logger.info(f"Загружено {len(df_first)} строк в базу данных")
            
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
        logger.error(f"Ошибка при загрузке данных: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при загрузке данных: {str(e)}")

@router_train.post("/clean-data")
def clean_data_train():
    """Эндпоинт для очистки данных (первичная обработка)."""
    try:
        logger.info("Начало очистки данных...")
        # Получение полных данных из локальной БД
        db = get_db()
        data_extractor = DataExtractor(db)
        df_first = data_extractor.fetch_origin_data()
        logger.info(f"Загружено {len(df_first)} строк исходных данных")

        # Очистка данных
        processor = Preprocessing_data()
        df_clean = processor.first_preprocess_data(df_first)

        # Загрузка очищенных данных в локальную БД
        logger.info("Загрузка очищенных данных в локальную БД...")
        data_loader = DataLoader(db)
        data_loader.load_to_enriched_table(df_clean, batch_size=100000)
        logger.info(f"Очищенные данные успешно загружены: {len(df_clean)} строк")

        return {
            "message": "Данные успешно очищены и загружены в БД!",
            "rows": len(df_clean),
            "database": "Данные сохранены в таблицу enriched_data"
        }
    except Exception as e:
        logger.error(f"Ошибка при очистке данных: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при очистке данных: {str(e)}")

@router_train.post("/recover-data")
def recover_data_train():
    """Эндпоинт для восстановления данных."""
    try:
        logger.info("Начало восстановления данных...")
        # Получение полных данных из локальной БД
        db = get_db()
        data_extractor = DataExtractor(db)
        df_clean = data_extractor.fetch_enriched_data()
        logger.info(f"Загружено {len(df_clean)} строк обогащенных данных")

        # Восстановление данных
        sales_recovery = Recovery_sales()
        df_recovery = sales_recovery.first_full_sales_recovery(df_clean)

        # Загрузка данных в локальную БД
        logger.info("Загрузка восстановленных данных в локальную БД...")
        data_loader = DataLoader(db)
        data_loader.load_to_recovery_table(df_recovery, batch_size=100000)
        logger.info(f"Восстановленные данные успешно загружены: {len(df_recovery)} строк")

        return {
            "message": "Данные успешно восстановлены и загружены в БД!",
            "rows": len(df_recovery),
            "database": "Данные сохранены в таблицу recovery_data"
        }
    except Exception as e:
        logger.error(f"Ошибка при восстановлении данных: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при восстановлении данных: {str(e)}")

@router_train.post("/train-model")
def train_model():
    """Эндпоинт для обучения модели."""
    try:
        logger.info("Начало обучения модели...")
        # Получение полных данных из локальной БД
        db = get_db()
        data_extractor = DataExtractor(db)
        df_recovery = data_extractor.fetch_recovery_data()
        logger.info(f"Загружено {len(df_recovery)} строк восстановленных данных")

        # Обучение модели
        first_model_learn = First_learning_model()
        df_preduction = first_model_learn.first_learning_model(df_recovery, db)
        logger.info(f"Модель обучена, создано {len(df_preduction)} предсказаний")

        return {
            "message": "Модель успешно обучена и данные загружены в БД!",
            "rows": len(df_preduction),
            "database": "Данные сохранены в таблицу ml_data"
        }
    except Exception as e:
        logger.error(f"Ошибка при обучении модели: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при обучении модели: {str(e)}")


@router_predict.post("/predict-new-data")
def data_predict(
    remote_file_path: str,
    upload_to_sftp: bool = False,
    sftp_output_path: Optional[str] = None
):
    """
    Эндпоинт для прогнозирования с данными с SFTP сервера.
    
    Args:
        remote_file_path: Путь к файлу на SFTP сервере (обязательный)
        upload_to_sftp: Загружать ли результат на SFTP сервер
        sftp_output_path: Путь для сохранения результата на SFTP сервере (обязателен если upload_to_sftp=True)
    """
    try:
        logger.info(f"Начало прогнозирования для файла: {remote_file_path}")
        db = get_db()
        processor = Preprocessing_data()
        sales_recovery = Recovery_sales()
        use_model_prediction = Use_model_predict()
        data_loader = DataLoader(db)
        
        # Загружаем данные с SFTP сервера
        sftp_loader = SFTPDataLoader(SFTP_CONFIG)
        if not sftp_loader.connect():
            raise HTTPException(status_code=500, detail="Не удалось подключиться к SFTP серверу")
        
        try:
            df_next = sftp_loader.load_new_data_from_sftp(remote_file_path)
            if df_next is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Не удалось загрузить файл {remote_file_path} с SFTP сервера"
                )
            logger.info(f"Загружено {len(df_next)} строк с SFTP сервера")
        finally:
            sftp_loader.disconnect()
        
        # Загружаем в таблицу origin_data
        logger.info("Загрузка данных в таблицу origin_data...")
        data_loader.load_to_origin_table(df_next, batch_size=100000)
        
        # Получаем данные за последние 30 дней
        logger.info("Получение данных за последние 30 дней...")
        df_last_30_days_origin, df_last_30_days_recovery = _get_last_30_days_data(db)
        
        # Очищаем данные
        logger.info("Предобработка данных...")
        df_clean = processor.next_preprocess_data(df_last_30_days_origin, df_next, df_last_30_days_recovery)
        
        # Загружаем в таблицу enriched_data
        logger.info("Загрузка данных в таблицу enriched_data...")
        data_loader.load_to_enriched_table(df_clean, batch_size=100000)
        
        # Восстанавливаем продажи
        logger.info("Восстановление продаж...")
        df_recovery = sales_recovery.next_full_sales_recovery(
            df_last_30_days_origin, df_clean, df_last_30_days_recovery
        )
        
        # Загружаем в таблицу recovery_data
        logger.info("Загрузка данных в таблицу recovery_data...")
        data_loader.load_to_recovery_table(df_recovery, batch_size=100000)
        
        # Делаем прогноз
        logger.info("Выполнение прогноза...")
        df_preduction = use_model_prediction.use_model_predict(
            df_last_30_days_origin, df_recovery, df_last_30_days_recovery, db
        )
        
        # Загружаем в таблицу forecast_data
        logger.info("Загрузка прогноза в таблицу forecast_data...")
        data_loader.load_to_forecast_table(df_preduction, batch_size=100000)
        logger.info(f"Прогноз успешно создан для {len(df_preduction)} записей")
        
        # Загружаем результат на SFTP сервер (если требуется)
        result = {
            "message": "Прогноз успешно сделан и данные загружены в БД",
            "rows": len(df_preduction),
            "database": "Данные сохранены в таблицу forecast_data"
        }
        
        if upload_to_sftp:
            if not sftp_output_path:
                raise HTTPException(
                    status_code=400,
                    detail="sftp_output_path обязателен, если upload_to_sftp=True"
                )
            
            try:
                logger.info(f"Загрузка результатов на SFTP: {sftp_output_path}")
                sftp_loader = SFTPDataLoader(SFTP_CONFIG)
                if sftp_loader.connect():
                    try:
                        success = sftp_loader.upload_predictions_to_sftp(df_preduction, sftp_output_path)
                        if success:
                            result["sftp"] = f"Результат загружен на SFTP: {sftp_output_path}"
                            result["message"] = "Прогноз успешно сделан, данные загружены в БД и на SFTP сервер"
                        else:
                            result["sftp_error"] = "Не удалось загрузить на SFTP сервер"
                    finally:
                        sftp_loader.disconnect()
                else:
                    result["sftp_error"] = "Не удалось подключиться к SFTP серверу"
            except Exception as sftp_error:
                logger.error(f"Ошибка при загрузке на SFTP: {sftp_error}", exc_info=True)
                result["sftp_error"] = str(sftp_error)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при прогнозировании: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при прогнозировании: {str(e)}")


def _get_last_30_days_data(db):
    """Получает данные за последние 30 дней из базы данных."""
    extractor = Last30DaysExtractor(db)
    df_last_30_days_origin = extractor.fetch_last_30_days_origin()
    df_last_30_days_recovery = extractor.fetch_last_30_days_recovery()
    return df_last_30_days_origin, df_last_30_days_recovery


# Подключение роутеров к приложению
app.include_router(router_main)
app.include_router(router_train)
app.include_router(router_predict)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=APP_CONFIG['host'],
        port=APP_CONFIG['port']
    )