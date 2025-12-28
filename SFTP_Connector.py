"""
Модуль для подключения к SFTP серверу и загрузки файлов
"""

import pandas as pd
import paramiko
import io
import os
import tempfile
from typing import List, Optional, Dict, Any
import logging

# Настройка логирования
logger = logging.getLogger(__name__)

class SFTPConnector:
    """
    Класс для работы с SFTP сервером
    """
    
    def __init__(self, host: str, port: int = 22, username: str = None, 
                 password: str = None, key_filename: str = None):
        """
        Инициализация подключения к SFTP серверу
        
        Args:
            host: Адрес SFTP сервера
            port: Порт (по умолчанию 22)
            username: Имя пользователя
            password: Пароль (если не используется ключ)
            key_filename: Путь к файлу приватного ключа (если используется)
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self.transport = None
        self.sftp = None
        
    def connect(self) -> bool:
        """
        Подключение к SFTP серверу
        
        Returns:
            bool: True если подключение успешно, False иначе
        """
        try:
            # Создаем транспорт
            self.transport = paramiko.Transport((self.host, self.port))
            
            # Аутентификация
            if self.key_filename:
                # Аутентификация по ключу
                try:
                    # Пробуем разные форматы ключей
                    try:
                        # Стандартный OpenSSH формат
                        private_key = paramiko.RSAKey.from_private_key_file(self.key_filename)
                        logger.info("Используется RSA ключ OpenSSH формата")
                    except:
                        try:
                            # Попробуем Ed25519 ключ
                            private_key = paramiko.Ed25519Key.from_private_key_file(self.key_filename)
                            logger.info("Используется Ed25519 ключ")
                        except:
                            try:
                                # Попробуем DSS ключ
                                private_key = paramiko.DSSKey.from_private_key_file(self.key_filename)
                                logger.info("Используется DSS ключ")
                            except:
                                # Попробуем ECDSA ключ
                                try:
                                    private_key = paramiko.ECDSAKey.from_private_key_file(self.key_filename)
                                    logger.info("Используется ECDSA ключ")
                                except:
                                    # Если все не работает, попробуем загрузить как RSA (для .ppk файлов)
                                    logger.warning("Пробуем загрузить как RSA ключ (возможно .ppk файл)")
                                    private_key = paramiko.RSAKey.from_private_key_file(self.key_filename)
                    
                    self.transport.connect(username=self.username, pkey=private_key)
                    logger.info("Аутентификация по ключу успешна")
                    
                except Exception as key_error:
                    logger.error(f"Ошибка загрузки ключа: {key_error}")
                    # Если не удалось загрузить ключ, пробуем пароль (если указан)
                    if self.password:
                        logger.info("Пробуем аутентификацию по паролю")
                        self.transport.connect(username=self.username, password=self.password)
                    else:
                        raise key_error
            else:
                # Аутентификация по паролю
                if not self.password:
                    raise ValueError("Не указан ни ключ, ни пароль для аутентификации")
                self.transport.connect(username=self.username, password=self.password)
            
            # Создаем SFTP клиент
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            logger.info(f"Успешно подключились к SFTP серверу {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка подключения к SFTP серверу: {e}")
            return False
    
    def disconnect(self):
        """
        Отключение от SFTP сервера
        """
        try:
            if self.sftp:
                self.sftp.close()
            if self.transport:
                self.transport.close()
            logger.info("Отключились от SFTP сервера")
        except Exception as e:
            logger.error(f"Ошибка при отключении от SFTP сервера: {e}")
    
    def list_files(self, remote_path: str = "/") -> List[str]:
        """
        Получение списка файлов в удаленной директории
        
        Args:
            remote_path: Путь к удаленной директории
            
        Returns:
            List[str]: Список файлов
        """
        try:
            if not self.sftp:
                logger.error("Нет подключения к SFTP серверу")
                return []
            
            files = self.sftp.listdir(remote_path)
            logger.info(f"Найдено {len(files)} файлов в {remote_path}")
            return files
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка файлов: {e}")
            return []
    
    def download_file(self, remote_path: str, local_path: str = None) -> Optional[str]:
        """
        Загрузка файла с SFTP сервера
        
        Args:
            remote_path: Путь к файлу на сервере
            local_path: Локальный путь для сохранения (если None, создается временный файл)
            
        Returns:
            Optional[str]: Путь к загруженному файлу или None при ошибке
        """
        try:
            if not self.sftp:
                logger.error("Нет подключения к SFTP серверу")
                return None
            
            if local_path is None:
                # Создаем временный файл
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                local_path = temp_file.name
                temp_file.close()
            
            # Загружаем файл
            self.sftp.get(remote_path, local_path)
            logger.info(f"Файл {remote_path} загружен в {local_path}")
            return local_path
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла {remote_path}: {e}")
            return None
    
    def download_csv_as_dataframe(self, remote_path: str, force_csv: bool = False) -> Optional[pd.DataFrame]:
        """
        Загрузка файла с SFTP сервера как DataFrame
        
        Args:
            remote_path: Путь к файлу на сервере
            force_csv: Принудительно конвертировать в CSV формат
            
        Returns:
            Optional[pd.DataFrame]: DataFrame с данными или None при ошибке
        """
        try:
            if not self.sftp:
                logger.error("Нет подключения к SFTP серверу")
                return None
            
            # Определяем расширение файла
            file_extension = remote_path.lower().split('.')[-1] if '.' in remote_path else 'csv'
            
            # Создаем временный файл с соответствующим расширением
            with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{file_extension}') as temp_file:
                local_path = temp_file.name
            
            # Загружаем файл
            self.sftp.get(remote_path, local_path)
            
            # Читаем файл в зависимости от расширения
            try:
                if file_extension in ['csv', 'txt']:
                    try:
                        df = pd.read_csv(local_path, parse_dates=["Дата"])
                    except:
                        # Если не удалось с parse_dates, пробуем без него
                        df = pd.read_csv(local_path)
                        # Пробуем конвертировать столбец Дата в datetime
                        if 'Дата' in df.columns:
                            df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce')
                elif file_extension in ['xlsx', 'xls']:
                    df = pd.read_excel(local_path)
                    # Пробуем конвертировать столбец Дата в datetime
                    if 'Дата' in df.columns:
                        df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce')
                else:
                    # Пробуем как CSV по умолчанию
                    df = pd.read_csv(local_path)
                    if 'Дата' in df.columns:
                        df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce')
                        
            except Exception as read_error:
                logger.error(f"Ошибка при чтении файла {remote_path}: {read_error}")
                return None
            
            # Удаляем временный файл
            os.unlink(local_path)
            
            logger.info(f"Файл {remote_path} загружен как DataFrame: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла {remote_path}: {e}")
            # Удаляем временный файл в случае ошибки
            if 'local_path' in locals():
                try:
                    os.unlink(local_path)
                except:
                    pass
            return None
    
    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """
        Загрузка файла на SFTP сервер
        
        Args:
            local_path: Локальный путь к файлу
            remote_path: Путь на сервере
            
        Returns:
            bool: True если загрузка успешна, False иначе
        """
        try:
            if not self.sftp:
                logger.error("Нет подключения к SFTP серверу")
                return False
            
            self.sftp.put(local_path, remote_path)
            logger.info(f"Файл {local_path} загружен на сервер как {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла на сервер: {e}")
            return False
    
    def upload_dataframe_as_csv(self, df: pd.DataFrame, remote_path: str) -> bool:
        """
        Загрузка DataFrame как CSV файл на SFTP сервер
        
        Args:
            df: DataFrame для загрузки
            remote_path: Путь к файлу на сервере
            
        Returns:
            bool: True если загрузка успешна, False иначе
        """
        try:
            if not self.sftp:
                logger.error("Нет подключения к SFTP серверу")
                return False
            
            # Создаем временный файл
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
                local_path = temp_file.name
            
            # Сохраняем DataFrame в CSV
            df.to_csv(local_path, index=False)
            
            # Загружаем на сервер
            self.sftp.put(local_path, remote_path)
            
            # Удаляем временный файл
            os.unlink(local_path)
            
            logger.info(f"DataFrame загружен как CSV на сервер: {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке DataFrame на сервер: {e}")
            # Удаляем временный файл в случае ошибки
            if 'local_path' in locals():
                try:
                    os.unlink(local_path)
                except:
                    pass
            return False
    
    def get_file_info(self, remote_path: str) -> Optional[Dict[str, Any]]:
        """
        Получение информации о файле
        
        Args:
            remote_path: Путь к файлу на сервере
            
        Returns:
            Optional[Dict[str, Any]]: Информация о файле или None при ошибке
        """
        try:
            if not self.sftp:
                logger.error("Нет подключения к SFTP серверу")
                return None
            
            stat = self.sftp.stat(remote_path)
            return {
                'size': stat.st_size,
                'modified_time': stat.st_mtime,
                'permissions': stat.st_mode
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации о файле {remote_path}: {e}")
            return None
    
    def file_exists(self, remote_path: str) -> bool:
        """
        Проверка существования файла на сервере
        
        Args:
            remote_path: Путь к файлу на сервере
            
        Returns:
            bool: True если файл существует, False иначе
        """
        try:
            if not self.sftp:
                return False
            
            self.sftp.stat(remote_path)
            return True
            
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.error(f"Ошибка при проверке существования файла {remote_path}: {e}")
            return False

class SFTPDataLoader:
    """
    Класс для загрузки данных с SFTP сервера для системы прогнозирования
    """
    
    def __init__(self, sftp_config: Dict[str, Any]):
        """
        Инициализация загрузчика данных
        
        Args:
            sftp_config: Конфигурация SFTP подключения
        """
        self.sftp_config = sftp_config
        self.sftp_connector = None
        
    def connect(self) -> bool:
        """
        Подключение к SFTP серверу
        
        Returns:
            bool: True если подключение успешно
        """
        try:
            self.sftp_connector = SFTPConnector(
                host=self.sftp_config['host'],
                port=self.sftp_config.get('port', 22),
                username=self.sftp_config['username'],
                password=self.sftp_config.get('password'),
                key_filename=self.sftp_config.get('key_filename')
            )
            
            return self.sftp_connector.connect()
            
        except Exception as e:
            logger.error(f"Ошибка при создании SFTP подключения: {e}")
            return False
    
    def disconnect(self):
        """
        Отключение от SFTP сервера
        """
        if self.sftp_connector:
            self.sftp_connector.disconnect()
    
    def load_new_data_from_sftp(self, remote_file_path: str) -> Optional[pd.DataFrame]:
        """
        Загрузка новых данных с SFTP сервера
        
        Args:
            remote_file_path: Путь к файлу на SFTP сервере
            
        Returns:
            Optional[pd.DataFrame]: DataFrame с данными или None при ошибке
        """
        try:
            if not self.sftp_connector:
                if not self.connect():
                    return None
            
            # Проверяем существование файла
            if not self.sftp_connector.file_exists(remote_file_path):
                logger.error(f"Файл {remote_file_path} не найден на SFTP сервере")
                return None
            
            # Загружаем файл как DataFrame
            df = self.sftp_connector.download_csv_as_dataframe(remote_file_path, force_csv=True)
            
            if df is not None:
                logger.info(f"Успешно загружены данные с SFTP: {df.shape}")
                logger.info(f"Доступные столбцы: {list(df.columns)}")
                
                # Проверяем наличие необходимых столбцов (более гибкая проверка)
                required_columns = ['Дата', 'Магазин', 'Товар']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                # Проверяем столбцы с продажами (может быть разное название)
                sales_columns = [col for col in df.columns if 'прода' in col.lower() or 'sale' in col.lower()]
                if not sales_columns:
                    logger.warning("Не найдены столбцы с продажами. Доступные столбцы: " + str(list(df.columns)))
                
                if missing_columns:
                    logger.error(f"В файле отсутствуют обязательные столбцы: {missing_columns}")
                    logger.error(f"Доступные столбцы: {list(df.columns)}")
                    return None
                
                return df
            else:
                logger.error("Не удалось загрузить данные с SFTP сервера")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных с SFTP: {e}")
            return None
    
    def upload_predictions_to_sftp(self, predictions_df: pd.DataFrame, remote_file_path: str) -> bool:
        """
        Загрузка результатов предсказания на SFTP сервер
        
        Args:
            predictions_df: DataFrame с предсказаниями
            remote_file_path: Путь к файлу на сервере
            
        Returns:
            bool: True если загрузка успешна
        """
        try:
            if not self.sftp_connector:
                if not self.connect():
                    return False
            
            return self.sftp_connector.upload_dataframe_as_csv(predictions_df, remote_file_path)
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке предсказаний на SFTP: {e}")
            return False
    
    def list_available_files(self, remote_directory: str = "/") -> List[str]:
        """
        Получение списка доступных файлов на SFTP сервере
        
        Args:
            remote_directory: Директория на сервере
            
        Returns:
            List[str]: Список файлов
        """
        try:
            if not self.sftp_connector:
                if not self.connect():
                    return []
            
            files = self.sftp_connector.list_files(remote_directory)
            # Фильтруем только CSV файлы
            csv_files = [f for f in files if f.endswith('.csv')]
            return csv_files
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка файлов: {e}")
            return []