from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import PoissonRegressor
from collections import deque
import lightgbm as lgb
import numpy as np
import pandas as pd
import time

class Recovery_sales():
    def first_data_type_refactor(self, df):
        df_copy = df.copy()

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

            'ДеньНедели': 'ДеньНедели',
            'День': 'День',
            'Месяц': 'Месяц',
            'Год': 'Год',

            'Сезонность': 'Сезонность',
            'Сезонность_точн': 'Сезонность_точн',
            'Температура (°C)': 'Температура (°C)',
            'Давление (мм рт. ст.)': 'Давление (мм рт. ст.)'
        }

        df_copy = df_copy.rename(columns=column_rename_map)

        return df_copy

    def is_poisson_simple(self, group_data, tolerance=0.20, zero_threshold=0.95):
        """
        Проверка на распределение Пуассона по упрощенным критериям:
        1. Если 95+% значений нулевые - считаем Пуассоном (λ≈0)
        2. Если среднее и дисперсия отличаются не более чем на tolerance*100%
        """
        if len(group_data) == 0:
            return False

        mean = np.mean(group_data)
        var = np.var(group_data)

        # Случай почти нулевых продаж
        if np.mean(group_data == 0) >= zero_threshold:
            # zero_ratio = np.mean(group_data == 0)
            return True # zero_ratio >= zero_threshold

        # Основной критерий
        return abs(mean - var) / mean <= tolerance

    def use_poison_check(self, df):
        df_copy = df.copy()

        poisson_flags = {}

        for (shop, product), group in df_copy.groupby(['Магазин', 'Товар'])['Продано']:
            poisson_flags[(shop, product)] = (self.is_poisson_simple(group))

        # Преобразуем словарь в DataFrame для объединения
        poisson_df = pd.DataFrame(
            [(shop, product, is_poisson) for (shop, product),
            is_poisson in poisson_flags.items()],
            columns=['Магазин', 'Товар', 'Пуассон_распр']
        )

        # Объединяем обратно с основным DataFrame по Магазин + Товар
        df = df_copy.merge(poisson_df, on=['Магазин', 'Товар'], how='left')
        print(f'\nСвязок товар+магазин с Пуассоновским распределением: {df[df['Пуассон_распр'] == 1][['Товар', 'Магазин']].drop_duplicates().shape[0]}')
        print(f'Связок товар+магазин не с Пуассоновским распределением:{df[df['Пуассон_распр'] == 0][['Товар', 'Магазин']].drop_duplicates().shape[0]}')

        print('Проверка распределений закончена\n')

        return df


    def enhance_poison_sales(self, data):
        """
        Обрабатывает данные, заменяя нулевые продажи (при нулевом остатке и отсутствии поступлений)
        на смоделированные значения. Временные признаки обрабатываются как категориальные.
        """
        data = data.copy()

        data['Продано_правка'] = data['Продано']
        combos = data[['Магазин', 'Товар']].drop_duplicates()
        data['Акция'] = data['Акция'].astype(str)
        data['Выходной'] = data['Выходной'].astype(str)
        data['ДеньНедели'] = data['ДеньНедели'].astype(str)
        data['День'] = data['День'].astype(str)
        data['Месяц'] = data['Месяц'].astype(str)
        data['Год'] = data['Год'].astype(str)

        # Все категориальные признаки (включая временные)
        features_categorical = ['Акция', 'Выходной',
                                'ДеньНедели', 'День', 'Месяц', 'Год', 'Сезонность_точн']

        #    # Числовые признаки
        features_numerical = ['Цена', 'КоличествоЧеков', 'Температура (°C)', 'Давление (мм рт. ст.)']

        for _, (shop, product) in combos.iterrows():
            mask = (data['Магазин'] == shop) & (data['Товар'] == product)
            subset = data[mask].copy()

            condition_modify = (subset['Продано'] == 0) & (subset['Остаток'] == 0) & (subset['Поступило'] == 0)
            condition_keep = ~condition_modify

            nonzero_sales = subset[condition_keep].copy()
            zero_sales = subset[condition_modify].copy()

            if len(zero_sales) == 0:
                continue

            try:
                preprocessor = ColumnTransformer(
                    transformers=[
                        ('cat', OneHotEncoder(handle_unknown='ignore'), features_categorical),
                        ('num', StandardScaler(), features_numerical)
                    ]
                )

                model = Pipeline([
                    ('preprocessor', preprocessor),
                    ('regressor', PoissonRegressor(alpha=0.5, max_iter=2000))
                ])

                model.fit(nonzero_sales[features_categorical + features_numerical],
                          nonzero_sales['Продано'].clip(lower=0))

                predicted = model.predict(zero_sales[features_categorical + features_numerical])
                simulated_sales = np.random.poisson(np.maximum(predicted, 0))

                data.loc[zero_sales.index, 'Продано_правка'] = simulated_sales

            except Exception as e:
                print(f"Ошибка для магазин {shop}, товар {product}: {str(e)}")
                continue

        print('Продажи товаров с пуассоновским распределением восстановлены')

        return data

    def enhance_non_poison_sales(self, data):
        """
        Обрабатывает данные, заменяя нулевые продажи на смоделированные значения с помощью LightGBM.
        """
        data = data.copy()
        data['Продано_правка'] = data['Продано']
        combos = data[['Магазин', 'Товар']].drop_duplicates()

        # Преобразование категориальных признаков
        data['Акция'] = data['Акция'].astype(str)
        data['Выходной'] = data['Выходной'].astype(str)
        data['ДеньНедели'] = data['ДеньНедели'].astype(str)
        data['День'] = data['День'].astype(str)
        data['Месяц'] = data['Месяц'].astype(str)
        data['Год'] = data['Год'].astype(str)

        # Признаки
        features_categorical = ['Акция', 'Выходной', 'ДеньНедели', 'День', 'Месяц', 'Год', 'Сезонность_точн']
        features_numerical = ['Цена', 'КоличествоЧеков', 'Температура (°C)', 'Давление (мм рт. ст.)']

        for _, (shop, product) in combos.iterrows():
            mask = (data['Магазин'] == shop) & (data['Товар'] == product)
            subset = data[mask].copy()

            condition_modify = (subset['Продано'] == 0) & (subset['Остаток'] == 0) & (subset['Поступило'] == 0)
            condition_keep = ~condition_modify

            nonzero_sales = subset[condition_keep].copy()
            zero_sales = subset[condition_modify].copy()

            if len(zero_sales) == 0:
                continue

            try:
                # LightGBM параметры
                lgb_params = {
                    'objective': 'poisson',  # Для счетных данных
                    'metric': 'poisson',
                    'num_leaves': 31,
                    'learning_rate': 0.05,
                    'n_estimators': 100,
                    'verbose': -1
                }

                # Препроцессинг
                preprocessor = ColumnTransformer(
                    transformers=[
                        ('cat', OneHotEncoder(handle_unknown='ignore'), features_categorical),
                        ('num', StandardScaler(), features_numerical)
                    ]
                )

                # Пайплайн с LightGBM
                model = Pipeline([
                    ('preprocessor', preprocessor),
                    ('regressor', lgb.LGBMRegressor(**lgb_params))
                ])

                # Обучение на ненулевых данных
                model.fit(
                    nonzero_sales[features_categorical + features_numerical],
                    nonzero_sales['Продано'].clip(lower=0)
                )

                # Предсказание и генерация целых чисел
                predicted = model.predict(zero_sales[features_categorical + features_numerical])
                data.loc[zero_sales.index, 'Продано_правка'] = np.round(np.maximum(predicted, 0)).astype(int)

            except Exception as e:
                print(f"Ошибка для магазина {shop}, товара {product}: {str(e)}")
                continue

        print('Продажи восстановлены с помощью LightGBM')
        print('Восстановление продаж закончено\n')
        return data

    def calculate_delivery_lags(self, df):
        """
        Рассчитывает средний и медианный лаг между заказом и поступлением товара
        для каждой пары Магазин-Товар.

        Параметры:
        ----------
        df : pandas.DataFrame
            Исходный датафрейм с колонками:
            - 'Дата' (дата операции)
            - 'Магазин' (название магазина)
            - 'Товар' (название товара)
            - 'Заказ' (количество заказанного товара)
            - 'Поступило' (количество поступившего товара)

        Возвращает:
        ----------
        pandas.DataFrame
            Датафрейм с колонками:
            - 'Магазин'
            - 'Товар'
            - 'Средний_лаг_в_днях'
            - 'Медианный_лаг_в_днях'
        """
        # Копируем датафрейм, чтобы не менять исходный
        df = df.copy()

        # Преобразуем дату в datetime
        df['Дата'] = pd.to_datetime(df['Дата'])

        # Сортируем по дате
        df = df.sort_values(['Магазин', 'Товар', 'Дата'])

        # Список для результатов
        lags = []

        # Группировка по (Магазин, Товар)
        for (store, product), group in df.groupby(['Магазин', 'Товар'], sort=False):
            queue = deque()
            dates = group['Дата'].values
            orders = group['Заказ'].values
            receipts = group['Поступило'].values

            for i in range(len(group)):
                order_qty = orders[i]
                receipt_qty = receipts[i]
                current_date = dates[i]

                if order_qty > 0:
                    queue.append({'Дата': current_date, 'Осталось': order_qty})

                if receipt_qty > 0:
                    remaining = receipt_qty
                    while queue and remaining > 0:
                        current_order = queue[0]
                        used = min(current_order['Осталось'], remaining)
                        lag_days = (current_date - current_order['Дата']) / np.timedelta64(1, 'D')
                        lags.append((store, product, lag_days))
                        current_order['Осталось'] -= used
                        remaining -= used
                        if current_order['Осталось'] == 0:
                            queue.popleft()

        # Создаем DataFrame с лагами
        if not lags:  # Если нет ни одного лага
            return pd.DataFrame(columns=['Магазин', 'Товар', 'Средний_лаг_в_днях', 'Медианный_лаг_в_днях'])

        lag_df = pd.DataFrame(lags, columns=['Магазин', 'Товар', 'Лаг_в_днях'])

        # Группируем и считаем средний и медианный лаг
        lag_stats = (
            lag_df.groupby(['Магазин', 'Товар'])['Лаг_в_днях']
            .agg(Медианный_лаг_в_днях='median') # Средний_лаг_в_днях='mean'
            .reset_index()
        )

        return lag_stats


    def add_lag_columns_to_data(self, df, default_lag=2):
        """
        Добавляет в исходный датафрейм 2 столбца:
        - Средний_лаг_в_днях
        - Медианный_лаг_в_днях
        на основе анализа заказов и поставок.

        Параметры:
        ----------
        df : pandas.DataFrame
            Исходный датафрейм с колонками:
            - 'Дата' (дата операции)
            - 'Магазин' (название магазина)
            - 'Товар' (название товара)
            - 'Заказ' (количество заказанного товара)
            - 'Поступило' (количество поступившего товара)
        default_lag : int, optional
            Значение лага по умолчанию, если для пары нет данных (по умолчанию 7)

        Возвращает:
        ----------
        pandas.DataFrame
            Исходный датафрейм с добавленными колонками.
        """
        # Рассчитываем лаги
        lag_stats = self.calculate_delivery_lags(df)

        # Объединяем с исходным датафреймом
        df_with_lags = pd.merge(
            df,
            lag_stats,
            on=['Магазин', 'Товар'],
            how='left'
        )

        # Выводим количество уникальных пар Магазин-Товар с NA
        na_pairs_count = df_with_lags[df_with_lags['Медианный_лаг_в_днях'].isna()][['Магазин', 'Товар']].drop_duplicates().shape[0]

        print(f"Количество пар Магазин-Товар с отсутствующими лагами: {na_pairs_count}")

        df_with_lags['Медианный_лаг_в_днях'] = df_with_lags['Медианный_лаг_в_днях'].fillna(default_lag)

        print('Добавлен медианный лаг между заказом и приходом в аптеку\n')

        return df_with_lags

    def simulate_inventory_with_lags(self, df, max_deficit_period=14):
        """
        Моделирует заказы, поступления и остатки товаров с учетом медианного лага поставок.
        После окончания смоделированного периода остаток обнуляется.

        Параметры:
        ----------
        df : pandas.DataFrame
            Исходный датафрейм с колонками:
            - 'Дата' (дата операции)
            - 'Магазин' (название магазина)
            - 'Товар' (название товара)
            - 'Остаток' (текущий остаток)
            - 'Продано' (количество проданного товара)
            - 'Поступило' (количество поступившего товара)
            - 'Заказ' (количество заказанного товара)
            - 'Продано_правка' (скорректированные продажи, если есть)
            - 'Медианный_лаг_в_днях' (медианное время поставки)

        max_deficit_period : int, optional
            Максимальный период дефицита для анализа (по умолчанию 14 дней)

        Возвращает:
        ----------
        pandas.DataFrame
            Датафрейм с добавленными колонками:
            - 'Смоделированные_заказы'
            - 'Поступило_правка'
            - 'Остаток_правка' (обнуляется после смоделированного периода)
        """
        df_copy = df.copy()
        df_copy = df_copy.sort_values(['Магазин', 'Товар', 'Дата'])

        df_copy['Смоделированные_заказы'] = 0
        df_copy['Поступило_правка'] = df_copy['Поступило'].copy()
        df_copy['Остаток_правка'] = df_copy['Остаток'].copy()

        for (store, product), group in df_copy.groupby(['Магазин', 'Товар']):
            group_indices = group.index
            n = len(group)
            median_lag = group['Медианный_лаг_в_днях'].iloc[0] if 'Медианный_лаг_в_днях' in group.columns else 1

            # 1. Формируем заказы для дефицитных периодов
            i = 0
            while i < n:
                current_row = group.iloc[i]

                if current_row['Остаток'] == 0 and current_row['Продано'] == 0:
                    start_idx = i
                    end_idx = min(i + max_deficit_period, n)
                    deficit_period = []

                    for j in range(start_idx, end_idx):
                        if group.iloc[j]['Остаток'] == 0 and group.iloc[j]['Продано'] == 0:
                            deficit_period.append(j)
                        else:
                            break

                    if deficit_period:
                        order_qty = group.iloc[deficit_period]['Продано_правка'].sum()

                        order_date_idx = group_indices[max(0, i - int(median_lag))]
                        df_copy.at[order_date_idx, 'Смоделированные_заказы'] += order_qty

                        for day in deficit_period:
                            receipt_date_idx = group_indices[day]
                            df_copy.at[receipt_date_idx, 'Поступило_правка'] += round(order_qty / len(deficit_period))

                        i = deficit_period[-1] + 1
                    else:
                        i += 1
                else:
                    i += 1

            # 2. Пересчитываем остатки с учётом новых поступлений
            current_balance = None
            last_simulated_day = None

            for i in range(n):
                current_idx = group_indices[i]
                current_row = group.iloc[i]

                if current_balance is None:
                    current_balance = current_row['Остаток']

                # Проверяем, закончился ли смоделированный период
                if current_row['Остаток'] != 0 or current_row['Продано'] != 0:
                    last_simulated_day = i

                current_balance += df_copy.at[current_idx, 'Поступило_правка'] - df_copy.at[current_idx, 'Продано_правка']
                current_balance = max(0, current_balance)

                # Если это день после последнего смоделированного, обнуляем остаток
                if last_simulated_day is not None and i > last_simulated_day:
                    current_balance = 0

                df_copy.at[current_idx, 'Остаток_правка'] = round(current_balance)

        df_copy['Смоделированные_заказы'] += df_copy['Заказ']
        df_copy = df_copy.sort_index()

        print('Добавлены смоделированные поступления, заказы и остатки\n')
        print('Восстановление продаж, остатков, поступлений и заказов закончено')

        return df_copy

    def data_type_refactor(self, df):
        type_objects = ['Магазин', 'Товар', 'Категория', 'ПотребГруппа', 'МНН', 'Сезонность']
        type_int = ['Продано', 'Поступило', 'Остаток', 'КоличествоЧеков', 'Заказ',
                    'ПроданоСеть', 'ПоступилоСеть', 'ОстатокСеть', 'КоличествоЧековСеть',
                    'ДеньНедели', 'День', 'Месяц', 'Год',
                    'Продано_правка', 'Смоделированные_заказы', 'Поступило_правка', 'Остаток_правка']
        type_float = ['Цена', 'Температура (°C)', 'Давление (мм рт. ст.)', 'Медианный_лаг_в_днях']
        type_bool = ['Акция', 'Выходной', 'Сезонность_точн', 'Пуассон_распр']

        df['Дата'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y')
        df[type_objects] = df[type_objects].astype(str)
        df[type_int] = df[type_int].astype(int)
        df[type_float] = df[type_float].astype(float)
        df[type_bool] = df[type_bool].astype(bool)

        print('Типы данных скорректированы')
        return df

    def first_full_sales_recovery(self, df):
        start_time = time.time()
        df_copy = df.copy()

        df_copy = self.first_data_type_refactor(df_copy)

        df_result = self.use_poison_check(df_copy)

        df_poison = df_result[df_result['Пуассон_распр'] == True]
        df_poison_restored_sales = self.enhance_poison_sales(df_poison)

        df_non_poison = df_result[df_result['Пуассон_распр'] == False]
        df_non_poison_restored_sales = self.enhance_non_poison_sales(df_non_poison)

        df_recovery_sales = pd.concat([df_poison_restored_sales, df_non_poison_restored_sales]
                                      ,ignore_index=True)

        df_median_lag = self.add_lag_columns_to_data(df_recovery_sales)

        df_full_recovery = self.simulate_inventory_with_lags(df_median_lag)

        df_full_recovery = self.data_type_refactor(df_full_recovery)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения: {execution_time // 60} минут {execution_time % 60} секунд\n")

        df_full_recovery.sort_values(by=['Дата', 'Магазин', 'Товар'])

        return df_full_recovery

    def next_full_sales_recovery(self, df_first, df_next, df_season_sales):

        df_first_copy = df_first.copy()
        df_next_copy = df_next.copy()
        df_season_sales_copy = df_season_sales.copy()


        df_first_poisson = (
            df_season_sales_copy[['Магазин', 'Товар', 'Пуассон_распр', 'Медианный_лаг_в_днях']]
            .drop_duplicates(subset=['Магазин', 'Товар'])
            .reset_index(drop=True)
        )


        df_next_poison = df_next_copy.merge(
            df_first_poisson,
            on=['Магазин', 'Товар'],
            how='inner'  # Сохраняем все строки из df_next_copy
        )
        print('Добавлено соответствие распределению Пуассона и Медианный лаг в днях')   

        df_next_poison['Продано_правка'] = df_next_poison['Продано'].copy()
        df_next_poison['Смоделированные_заказы'] = df_next_poison['Заказ'].copy()
        df_next_poison['Поступило_правка'] = df_next_poison['Поступило'].copy()
        df_next_poison['Остаток_правка'] = df_next_poison['Остаток'].copy()

        df_full_recovery = self.data_type_refactor(df_next_poison)

        df_full_recovery = df_full_recovery[['Дата', 'Магазин', 'Товар', 'Цена', 'Акция', 'Выходной',
                             'Продано', 'Поступило', 'Остаток',
                             'Категория', 'ПотребГруппа',
                             'ПроданоСеть', 'ПоступилоСеть', 'ОстатокСеть',
                             'МНН', 'КоличествоЧеков', 'КоличествоЧековСеть',
                             'Заказ', 'ДеньНедели', 'День', 'Месяц', 'Год',
                             'Сезонность', 'Сезонность_точн',
                             'Температура (°C)', 'Давление (мм рт. ст.)', 'Пуассон_распр',
                             'Продано_правка', 'Медианный_лаг_в_днях',
                             'Смоделированные_заказы', 'Поступило_правка', 'Остаток_правка']]

        df_full_recovery.sort_values(by=['Дата', 'Магазин', 'Товар'])

        return df_full_recovery


