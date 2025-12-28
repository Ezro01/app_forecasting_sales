"""
Модуль для предобработки данных продаж.
Включает функции для очистки, обогащения и трансформации данных.
"""
import numpy as np
import requests
import pytz
import pandas as pd
import time
import logging

# Настройка логирования
logger = logging.getLogger(__name__)


class Preprocessing_data:
    def rename_columns(self, df):
        column_rename_map = {
            'Дата': 'Дата',
            'Магазин': 'Магазин',
            'Товар': 'Товар',
            'Цена': 'Цена',
            'Акция': 'Акция',
            'Выходной': 'Выходной',

            'Категория': 'Категория',
            'ПотребГруппа': 'ПотребГруппа',
            'МНН': 'МНН',

            'Продано_шт': 'Продано',
            'Остаток_шт': 'Остаток',
            'Поступило_шт': 'Поступило',
            'Заказ_шт': 'Заказ',
            'КоличествоЧеков_шт': 'КоличествоЧеков',

            'ПроданоСеть_шт': 'ПроданоСеть',
            'ОстатокСеть_шт': 'ОстатокСеть',
            'ПоступилоСеть_шт': 'ПоступилоСеть',
            'КоличествоЧековСеть_шт': 'КоличествоЧековСеть',
        }

        df = df.rename(columns=column_rename_map)

        return df
    
    def fill_zero_prices(self, series):

        series = series.astype(float).replace(0, np.nan).bfill().ffill()

        # Сначала backward fill для нулей в начале
        # Затем forward fill для оставшихся нулей
        return series

    def parse_dates(self, df):
        df['Дата'] = pd.to_datetime(df['Дата'])
        df['ДеньНедели'] = df['Дата'].dt.dayofweek
        df['День'] = df['Дата'].dt.day
        df['Месяц'] = df['Дата'].dt.month
        df['Год'] = df['Дата'].dt.year

        logger.debug('Добавлены столбцы: год, месяц, день недели')

        return df

    def non_negative_values(self, df):
        df_copy = df.copy()
        df_copy['Продано'] = df_copy['Продано'].clip(lower=0)
        df_copy['Остаток'] = df_copy['Остаток'].clip(lower=0)
        df_copy['ПроданоСеть'] = df_copy['ПроданоСеть'].clip(lower=0)
        df_copy['ОстатокСеть'] = df_copy['ОстатокСеть'].clip(lower=0)
        logger.debug('Отрицательные значения продаж и остатков приравнены к нулю')

        df_copy['МНН'] = df_copy['МНН'].astype(object)
        df_copy['Магазин'] = df_copy['Магазин'].astype(object)

        df_copy['МНН'] = df_copy['МНН'].fillna('Не определено')
        df_copy['ПотребГруппа'] = df_copy['ПотребГруппа'].fillna('Не определена')
        logger.debug('NA столбцов МНН и ПотребГруппа заполнены')

        return df_copy

    def clining_data(self, df):
        logger.info(f"Количество строк до фильтрации: {df.shape[0]}")
        # 1. Находим максимальную дату в датафрейме и вычисляем порог (365 дней назад)
        max_date = df['Дата'].max()
        cutoff_date = max_date - pd.Timedelta(days=365)

        # 2. Фильтруем данные за последние 365 дней и оставляем только записи, где были продажи (Продано > 0)
        recent_sales = df[(df['Дата'] >= cutoff_date) & (df['Продано'] > 0)]

        # 3. Получаем уникальные пары "Магазин + Товар", у которых были продажи в этот период
        active_pairs = recent_sales[['Магазин', 'Товар']].drop_duplicates()

        # 4. Фильтруем исходный датафрейм, оставляя только эти пары
        filtered_df = df.merge(active_pairs, on=['Магазин', 'Товар'], how='inner')

        # 5. Перезаписываем исходный df и проверяем количество строк
        df = filtered_df.copy()


        # Сгруппировали и посчитали сумму продаж по магазину и товару
        test_for_0_zero = df.groupby(['Магазин', 'Товар'])['Продано'].sum().reset_index()

        # Отбираем группы, где сумма продаж больше 6
        # good_groups = test_for_0_zero[test_for_0_zero['Продано'] <= 6]
        # print('Меньше или равно 6 продажам', good_groups.count())
        good_groups = test_for_0_zero[test_for_0_zero['Продано'] > 6]
        # print('Больше 6 продаж',good_groups.count())

        # Фильтруем исходный df, оставляя только строки из этих групп
        test_0 = df.merge(good_groups[['Магазин', 'Товар']], on=['Магазин', 'Товар'], how='inner')

        df = test_0.copy()
        logger.info(f"Количество строк после фильтрации: {df.shape[0]}")
        logger.info('Удалены товары вышедшие из ассортимента')

        return df

    # Определение сезонности
    def define_the_season(self, df):

    # Проверяем наличие необходимых столбцов
        required_cols = ['Дата', 'Магазин', 'Товар', 'Продано']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Отсутствует обязательный столбец: {col}")

    # Определяем сезон
        def get_season(month):
            if month in [12, 1, 2]:
                return 'Зима'
            elif month in [3, 4, 5]:
                return 'Весна'
            elif month in [6, 7, 8]:
                return 'Лето'
            else:
                return 'Осень'

        df['Сезон'] = df['Месяц'].apply(get_season)

        # Создаем сводку по сезонам
        season_summary = (
            df.groupby(['Магазин', 'Товар', 'Сезон'])['Продано']
            .sum()
            .unstack(fill_value=0)
            .reset_index()  # Важно: преобразуем индексы в столбцы
        )

        # Рассчитываем доли продаж по сезонам
        season_cols = ['Зима', 'Весна', 'Лето', 'Осень']

        # 1. Суммируем продажи по сезонам (с проверкой на нулевые значения)
        season_summary['Всего'] = season_summary[season_cols].sum(axis=1)

        # 2. Вычисляем доли продаж для каждого сезона
        season_dolya_cols = []
        for season in season_cols:
            col_name = f'{season}_доля'
            season_summary[col_name] = season_summary[season] / season_summary['Всего']
            season_dolya_cols.append(col_name)

        # 3. Определяем максимальную долю и соответствующий сезон
        season_summary['Макс_доля'] = season_summary[season_dolya_cols].max(axis=1)
        season_summary['Сезонность'] = season_summary[season_dolya_cols].idxmax(axis=1)

        # 4. Форматируем названия сезонов и определяем несезонные товары
        season_summary['Сезонность'] = (
            season_summary['Сезонность']
            .str.replace('_доля', '')
            .where(season_summary['Макс_доля'] >= 0.51, 'Несезонный')
        )

        # Объединяем с исходными данными
        df = df.merge(
            season_summary[['Магазин', 'Товар', 'Сезонность']],
            on=['Магазин', 'Товар'],
            how='left'
        )

        # Удаляем временные столбцы
        df = df.drop(['Сезон'], axis=1, errors='ignore')

        # Количество сезонных товаров
        seasonal_count = (season_summary['Сезонность'] != 'Несезонный').sum()

        # Количество несезонных товаров
        nonseasonal_count = (season_summary['Сезонность'] == 'Несезонный').sum()
        logger.info(f"Количество сезонных товаров: {seasonal_count}")
        logger.info(f"Количество несезонных товаров: {nonseasonal_count}")
        logger.info('Сезонные товары определены')

        return df

    # Уточнение сезонных товаров по датам
    def check_season(self, row):
        season = row['Сезонность']
        month = row['Месяц']

        if season == 'Несезонный':
            return 0

        season_months = {
            'Зима': {12, 1, 2},
            'Весна': {3, 4, 5},
            'Лето': {6, 7, 8},
            'Осень': {9, 10, 11},
        }

        return 1 if season in season_months and month in season_months[season] else 0

    def data_type_refactor(self, df):
        type_objects = ['Магазин', 'Товар', 'Категория', 'ПотребГруппа', 'МНН', 'Сезонность']
        type_int = ['Продано', 'Поступило', 'Остаток', 'КоличествоЧеков', 'Заказ',
                    'ПроданоСеть', 'ПоступилоСеть', 'ОстатокСеть', 'КоличествоЧековСеть',
                    'ДеньНедели', 'День', 'Месяц', 'Год']
        type_float = ['Цена']
        type_bool = ['Акция', 'Выходной', 'Сезонность_точн']

        # Проверяем наличие столбцов погоды и добавляем их в соответствующие списки
        if 'Температура (°C)' in df.columns:
            type_float.append('Температура (°C)')
        if 'Давление (мм рт. ст.)' in df.columns:
            type_float.append('Давление (мм рт. ст.)')

        df['Дата'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y')
        
        # Приводим типы только для существующих столбцов
        existing_objects = [col for col in type_objects if col in df.columns]
        existing_int = [col for col in type_int if col in df.columns]
        existing_float = [col for col in type_float if col in df.columns]
        existing_bool = [col for col in type_bool if col in df.columns]
        
        if existing_objects:
            df[existing_objects] = df[existing_objects].astype(str)
        if existing_int:
            df[existing_int] = df[existing_int].astype(int)
        if existing_float:
            df[existing_float] = df[existing_float].astype(float)
        if existing_bool:
            df[existing_bool] = df[existing_bool].astype(bool)

        logger.debug('Типы данных скорректированы')
        return df

    def add_weather_data(self, df):
        """
        Добавляет данные о температуре и атмосферном давлении в DataFrame
        на основе исторических данных для Томска.
        """
        # Проверка наличия колонки с датой
        if 'Дата' not in df.columns:
            raise ValueError("DataFrame должен содержать колонку 'Дата'")

        # Создаем копию DataFrame чтобы не изменять оригинал
        df = df.copy()

        # Координаты Томска
        latitude, longitude = 56.4977, 84.9744

        try:
            # Преобразуем даты в нужный формат
            df['Дата'] = pd.to_datetime(df['Дата']) #, format='%d.%m.%Y'
            start_date = df['Дата'].min().strftime('%Y-%m-%d')
            end_date = df['Дата'].max().strftime('%Y-%m-%d')

            # URL для запроса данных
            url = f'https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,pressure_msl'

            # Выполняем запрос с таймаутом
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'hourly' not in data:
                logger.warning("В ответе API отсутствуют hourly данные")
                return df

            # Часовой пояс Томска
            tomsk_tz = pytz.timezone('Asia/Novosibirsk')

            # Создаем временный DataFrame для погодных данных
            weather_df = pd.DataFrame({
                'time': data['hourly']['time'],
                'temperature': data['hourly']['temperature_2m'],
                'pressure_hpa': data['hourly']['pressure_msl']
            })

            # Конвертируем время и фильтруем по часовому поясу
            weather_df['time'] = pd.to_datetime(weather_df['time']).dt.tz_localize('UTC').dt.tz_convert(tomsk_tz)

            # Фильтруем период 17:00-19:00
            weather_df = weather_df[weather_df['time'].dt.hour.between(17, 19)]

            # Группируем по дате и вычисляем средние значения
            weather_df['date'] = weather_df['time'].dt.date
            daily_weather = weather_df.groupby('date').agg({
                'temperature': 'mean',
                'pressure_hpa': 'mean'
            }).reset_index()

            # Конвертируем давление в мм рт. ст. и округляем
            daily_weather['pressure_mmhg'] = (daily_weather['pressure_hpa'] * 0.750062).round(1)
            daily_weather['temperature'] = daily_weather['temperature'].round(1)

            # Преобразуем дату в datetime64[ns] для слияния
            daily_weather['date'] = pd.to_datetime(daily_weather['date'])

            # Объединяем с исходным DataFrame
            df = df.merge(
                daily_weather[['date', 'temperature', 'pressure_mmhg']],
                left_on=pd.to_datetime(df['Дата']).dt.date,
                right_on=daily_weather['date'].dt.date,
                how='left'
            ).drop('date', axis=1)  # Удаляем временную колонку

            # Переименовываем колонки
            df = df.rename(columns={
                'temperature': 'Температура (°C)',
                'pressure_mmhg': 'Давление (мм рт. ст.)'
            })

        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе к API погоды: {e}")
            logger.warning("Возможные причины: нет подключения к интернету, API недоступен, превышен лимит запросов")
        except Exception as e:
            logger.error(f"Произошла ошибка при получении погодных данных: {e}", exc_info=True)

        # Заполняем NaN значения для столбцов погоды, если они отсутствуют
        if 'Температура (°C)' not in df.columns:
            df['Температура (°C)'] = 0.0
            logger.warning("Добавлен столбец 'Температура (°C)' со значением по умолчанию (0.0)")
        else:
            df['Температура (°C)'] = df['Температура (°C)'].fillna(0.0)
            
        if 'Давление (мм рт. ст.)' not in df.columns:
            df['Давление (мм рт. ст.)'] = 0.0
            logger.warning("Добавлен столбец 'Давление (мм рт. ст.)' со значением по умолчанию (0.0)")
        else:
            df['Давление (мм рт. ст.)'] = df['Давление (мм рт. ст.)'].fillna(0.0)

        logger.info('Погода и атмосферное давление добавлены')

        return df


    def first_preprocess_data(self, df):
        start_time = time.time()

        df_copy = df.copy()

        df_copy = self.rename_columns(df_copy)

        df_copy['Цена'] = ((df_copy.groupby(['Магазин', 'Товар'])['Цена']
                            .transform(self.fill_zero_prices)))
        df_copy = df_copy.dropna(subset=['Цена'])
        logger.info('Нулевые значения цены восстановлены')

        df_non_negative_values = self.non_negative_values(df_copy)

        df_parse_dates = self.parse_dates(df_non_negative_values)

        df_cleaning = self.clining_data(df_parse_dates)

        df_define = self.define_the_season(df_cleaning)

        df_define['Сезонность_точн'] = df_define.apply(self.check_season, axis=1)
        logger.debug('Добавлена "Точная сезонность" в булевом формате для каждого дня')

        df_temp = self.add_weather_data(df_define)
        if 'key_0' in df_temp.columns:
            df_temp = df_temp.drop('key_0', axis=1)

        df_result_cleaning = self.data_type_refactor(df_temp)

        logger.info('Датасет очищен')

        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Время выполнения предобработки: {execution_time // 60:.0f} минут {execution_time % 60:.0f} секунд")

        logger.debug(f"Статистика пропущенных значений:\n{df_temp.isna().sum()}")
        logger.debug(f"Информация о датасете:\n{df_temp.info()}")

        df_result_cleaning = df_result_cleaning.sort_values(by=['Дата', 'Магазин', 'Товар'])

        return df_result_cleaning


    def next_preprocess_data(self, df_first, df_next, df_season_sales):
        start_time = time.time()

        df_next_copy = df_next.copy()
        df_first_copy = df_first.copy()
        df_season_sales_copy = df_season_sales.copy()

        df_next_copy = self.rename_columns(df_next_copy)
        df_first_copy = self.rename_columns(df_first_copy)

        df_next_copy['Цена'] = ((df_next_copy.groupby(['Магазин', 'Товар'])['Цена']
                            .transform(self.fill_zero_prices)))
        df_second_copy = df_next_copy.dropna(subset=['Цена'])
        logger.info('Нулевые значения цены восстановлены')

        df_non_negative_values = self.non_negative_values(df_second_copy)

        df_parse_dates = self.parse_dates(df_non_negative_values)

        logger.debug(f"Информация о данных сезонности:\n{df_season_sales_copy.info()}")

        # Сезонность + фильтрация
        df_first_season = (df_season_sales_copy[['Магазин', 'Товар', 'Сезонность']]
                           .drop_duplicates(subset=['Магазин', 'Товар'])
                           .reset_index(drop=True))


        # Получаем уникальные пары из обоих датафреймов
        df_parse_dates['Магазин'] = df_parse_dates['Магазин'].astype(str)
        df_parse_dates['Товар'] = df_parse_dates['Товар'].astype(str)
        df_first_season['Магазин'] = df_first_season['Магазин'].astype(str)
        df_first_season['Товар'] = df_first_season['Товар'].astype(str)
        
        pairs_in_next = set(zip(df_parse_dates['Магазин'], df_parse_dates['Товар']))
        pairs_in_season = set(zip(df_first_season['Магазин'], df_first_season['Товар']))

        # Находим пересечение
        common_pairs = pairs_in_next & pairs_in_season

        logger.info(f"Всего пар в df_parse_dates: {len(pairs_in_next)}")
        logger.info(f"Всего пар в df_first_season: {len(pairs_in_season)}")
        logger.info(f"Совпадающих пар: {len(common_pairs)}")

        if len(common_pairs) == 0:
            logger.warning("Нет совпадающих пар Магазин+Товар для объединения!")
        else:
            logger.debug("Есть совпадающие пары, можно делать merge.")
            df_next_with_season = df_parse_dates.merge(
            df_first_season,
            on=['Магазин', 'Товар'],
            how='inner'  # Только совпадающие строки
        )
        logger.info('Добавлена сезонность + отфильтрованы данные (как в исходном датасете)')

        df_next_with_season['Сезонность_точн'] = df_next_with_season.apply(self.check_season, axis=1)
        logger.debug('Добавлена "Точная сезонность" в булевом формате для каждого дня')

        df_temp = self.add_weather_data(df_next_with_season)
        
        if 'key_0' in df_temp.columns:
            df_temp = df_temp.drop('key_0', axis=1)

        df_result_cleaning = self.data_type_refactor(df_temp)
        logger.info('Датасет очищен')

        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Время выполнения предобработки: {execution_time // 60:.0f} минут {execution_time % 60:.0f} секунд")

        df_result_cleaning = df_result_cleaning.sort_values(by=['Дата', 'Магазин', 'Товар'])

        return df_result_cleaning






