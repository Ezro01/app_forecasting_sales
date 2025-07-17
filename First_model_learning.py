from sklearn.preprocessing import LabelEncoder, MinMaxScaler
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error
import catboost as cb
import optuna

class First_learning_model:
    def add_lag_values(self, df):
        # Сортируем данные по дате (очень важно!)
        df = df.sort_values(by=['Магазин', 'Товар', 'Дата'])

        # Лаги (значения за предыдущие периоды)
        df['Продано_1д_назад'] = df.groupby(['Магазин', 'Товар'])['Продано_правка'].shift(1)
        df['Поступило_1д_назад'] = df.groupby(['Магазин', 'Товар'])['Поступило_правка'].shift(1)
        df['Остаток_1д_назад'] = df.groupby(['Магазин', 'Товар'])['Остаток_правка'].shift(1)
        df['Заказ_1д_назад'] = df.groupby(['Магазин', 'Товар'])['Смоделированные_заказы'].shift(1)

        df['Продано_частота'] = df.groupby(['Магазин', 'Товар'])['Продано_правка'].transform(lambda x: (x > 0).astype(int).shift(1))
        df['Продано_частота_3д'] = df.groupby(['Магазин', 'Товар'])['Продано_частота'].transform(lambda x: x.rolling(window=3, min_periods=3).sum())
        df['Продано_частота_7д'] = df.groupby(['Магазин', 'Товар'])['Продано_частота'].transform(lambda x: x.rolling(window=7, min_periods=7).sum())
        df['Продано_частота_21д'] = df.groupby(['Магазин', 'Товар'])['Продано_частота'].transform(lambda x: x.rolling(window=21, min_periods=21).sum())

        df['Продано_темп_3д'] = df.groupby(['Магазин', 'Товар'])['Продано_правка'].transform(lambda x: x.rolling(window=3, min_periods=3).mean().shift(1))
        df['Продано_темп_7д'] = df.groupby(['Магазин', 'Товар'])['Продано_правка'].transform(lambda x: x.rolling(window=7, min_periods=7).mean().shift(1))
        df['Продано_темп_21д'] = df.groupby(['Магазин', 'Товар'])['Продано_правка'].transform(lambda x: x.rolling(window=21, min_periods=21).mean().shift(1))


        # Лаги (значения за предыдущие периоды)
        df['ПроданоСеть_1д_назад'] = df.groupby(['Магазин', 'Товар'])['ПроданоСеть'].shift(1)
        df['ПоступилоСеть_1д_назад'] = df.groupby(['Магазин', 'Товар'])['ПоступилоСеть'].shift(1)
        df['ОстатокСеть_1д_назад'] = df.groupby(['Магазин', 'Товар'])['ОстатокСеть'].shift(1)
        df['КоличествоЧековСеть_1д_назад'] = df.groupby(['Магазин', 'Товар'])['КоличествоЧековСеть'].shift(1)


        df['ПроданоСеть_частота'] = df.groupby(['Магазин', 'Товар'])['ПроданоСеть'].transform(lambda x: (x > 0).astype(int).shift(1))
        df['ПроданоСеть_частота_3д'] = df.groupby(['Магазин', 'Товар'])['ПроданоСеть_частота'].transform(lambda x: x.rolling(window=3, min_periods=3).sum())
        df['ПроданоСеть_частота_7д'] = df.groupby(['Магазин', 'Товар'])['ПроданоСеть_частота'].transform(lambda x: x.rolling(window=7, min_periods=7).sum())
        df['ПроданоСеть_частота_21д'] = df.groupby(['Магазин', 'Товар'])['ПроданоСеть_частота'].transform(lambda x: x.rolling(window=21, min_periods=21).sum())

        df['ПроданоСеть_темп_3д'] = df.groupby(['Магазин', 'Товар'])['ПроданоСеть'].transform(lambda x: x.rolling(window=3, min_periods=3).mean().shift(1))
        df['ПроданоСеть_темп_7д'] = df.groupby(['Магазин', 'Товар'])['ПроданоСеть'].transform(lambda x: x.rolling(window=7, min_periods=7).mean().shift(1))
        df['ПроданоСеть_темп_21д'] = df.groupby(['Магазин', 'Товар'])['ПроданоСеть'].transform(lambda x: x.rolling(window=21, min_periods=21).mean().shift(1))


        df = df.drop(['Продано_частота', 'ПроданоСеть_частота',
                      'Продано', 'Поступило', 'Остаток', 'КоличествоЧеков', 'Заказ',
                      'Пуассон_распр', 'Медианный_лаг_в_днях'], axis=1)

        # Таргет
        df['Продажи_7д_вперёд'] = (df.groupby(['Магазин', 'Товар'])['Продано_правка'].shift(-1) + \
                                   df.groupby(['Магазин', 'Товар'])['Продано_правка'].shift(-2) + \
                                   df.groupby(['Магазин', 'Товар'])['Продано_правка'].shift(-3) + \
                                   df.groupby(['Магазин', 'Товар'])['Продано_правка'].shift(-4) + \
                                   df.groupby(['Магазин', 'Товар'])['Продано_правка'].shift(-5) + \
                                   df.groupby(['Магазин', 'Товар'])['Продано_правка'].shift(-6) + \
                                   df.groupby(['Магазин', 'Товар'])['Продано_правка'].shift(-7)
                                   )

        df = df.dropna()
        df['Продажи_7д_вперёд'] = df['Продажи_7д_вперёд'].astype(int)

        # print(df.info())
        # print(df.isna().sum())

        # df.to_csv('C:/Все папки по жизни/Универ/dataset_for_learning.csv', index=False)

        print('Лаговые столбцы для обучения добавлены')

        return df

    def encoding_futures(self, df):
        df_encoding = df.copy()

        # Кодируем столбец 'Товар' с помощью LabelEncoder только для обучающих данных
        label_encoder_product = LabelEncoder()
        df_encoding['Товар'] = label_encoder_product.fit_transform(df_encoding['Товар'])

        label_encoder_shop = LabelEncoder()
        df_encoding['Магазин'] = label_encoder_shop.fit_transform(df_encoding['Магазин'])

        label_encoder = LabelEncoder()
        df_encoding['Категория'] = label_encoder.fit_transform(df_encoding['Категория'])

        label_encoder_2 = LabelEncoder()
        df_encoding['ПотребГруппа'] = label_encoder_2.fit_transform(df_encoding['ПотребГруппа'])

        label_encoder_3 = LabelEncoder()
        df_encoding['МНН'] = label_encoder_3.fit_transform(df_encoding['МНН'])


        numerical_columns = ['Цена',
                             'Температура (°C)', 'Давление (мм рт. ст.)',
                             'ПроданоСеть', 'ПоступилоСеть', 'ОстатокСеть', 'КоличествоЧековСеть',
                             'Продано_правка', 'Поступило_правка', "Остаток_правка", "Смоделированные_заказы",
                             'Продано_1д_назад', 'Поступило_1д_назад', 'Остаток_1д_назад', 'Заказ_1д_назад',
                             'Продано_частота_3д', 'Продано_частота_7д', 'Продано_частота_21д',
                             'Продано_темп_3д', 'Продано_темп_7д', 'Продано_темп_21д',
                             'ПроданоСеть_1д_назад', 'ПоступилоСеть_1д_назад', 'ОстатокСеть_1д_назад', 'КоличествоЧековСеть_1д_назад',
                             'ПроданоСеть_частота_3д', 'ПроданоСеть_частота_7д', 'ПроданоСеть_частота_21д',
                             'ПроданоСеть_темп_3д', 'ПроданоСеть_темп_7д', 'ПроданоСеть_темп_21д',
                             ]


        scaler = MinMaxScaler()
        df_encoding[numerical_columns] = scaler.fit_transform(df_encoding[numerical_columns])


        cat_columns = ['Товар', 'Магазин', 'Категория', 'ПотребГруппа', 'МНН',
                        'Акция', 'Выходной', 'ДеньНедели', 'Месяц', 'День', 'Год',
                        'Сезонность', 'Сезонность_точн']

        # encoder = OneHotEncoder(drop='first', sparse=False, handle_unknown='ignore')
        # df_encoding[one_hot_enc_col] = encoder.fit_transform(df_encoding[one_hot_enc_col])

        print('Масштабирование данных выполнено')

        return df_encoding, numerical_columns, cat_columns, label_encoder_product, label_encoder_shop

    def train_and_test(self, df, numerical_columns, cat_columns):
        # Целевая переменная - Продажи_7д_вперёд
        target_column = ['Продажи_7д_вперёд']

        max_date = df['Дата'].max()
        date_training = max_date - pd.Timedelta(days=30)

        train_data = df[df['Дата'] <= date_training]
        test_data = df[df['Дата'] > date_training]

        X_train = train_data[numerical_columns + cat_columns]
        y_train = train_data[target_column]

        X_test = test_data[numerical_columns + cat_columns]
        y_test = test_data[target_column]

        print(f"\nТренировочные данные: {X_train.shape, y_train.shape}")
        print(f"Тестовые данные: {X_test.shape, y_test.shape}")
        print(f"Дата начала обучения: {train_data['Дата'].min()} - конца: {train_data['Дата'].max()}")
        print(f"Дата начала теста: {test_data['Дата'].min()} - конца: {test_data['Дата'].max()}")

        test_preduction = test_data[['Дата', 'Магазин', 'Товар']].copy()
        test_preduction['Реальные значения'] = y_test[target_column].values
        test_preduction.head()

        y_test = np.abs(y_test.dropna())
        y_train= np.abs(y_train.dropna())
        X_train = X_train.dropna()
        X_test = X_test.dropna()

        print(f'Размерность X_test: {X_test.shape}\nРазмерность X_train: {X_train.shape}')
        print(f'Размерность y_test: {y_test.shape}\nРазмерность y_train: {y_train.shape}')
        print('Разбиение на выборки закончено\n')

        return X_train, y_train, X_test, y_test, test_preduction

    def optimize_catboost(self, X_train, y_train, X_test, y_test, cat_features):
        def objective(trial):
            params = {
                'iterations': trial.suggest_int('iterations', 1000, 8000),
                'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.05, log=True),
                'depth': trial.suggest_int('depth', 4, 10),
                'l2_leaf_reg': trial.suggest_float('l2_leaf_reg', 1, 50),
                'random_strength': trial.suggest_float('random_strength', 0.1, 5),
                'bootstrap_type': trial.suggest_categorical('bootstrap_type', ['Bernoulli']),    # 'Poisson',
                'min_data_in_leaf': trial.suggest_int('min_data_in_leaf', 50, 200),
                'max_ctr_complexity': trial.suggest_int('max_ctr_complexity', 1, 4),
                'loss_function': 'Huber:delta=0.5',
                'has_time': True,
                'cat_features': cat_features,
                'task_type': 'CPU',
                'devices': '0',
                'early_stopping_rounds': 500,
                'random_state': 42,
                'verbose': False,
                'eval_metric': 'RMSE',
            }

            model = cb.CatBoostRegressor(**params)
            model.fit(
                X_train, y_train,
                eval_set=(X_test, y_test),
                verbose=100 if trial.number == 0 else False,
            )

            return model.get_best_score()['validation']['RMSE']

        study = optuna.create_study(direction='minimize')
        study.optimize(objective, n_trials=5)

        best_params = study.best_params
        print("🔹 Лучшие параметры:", best_params)

        # Обучаем финальную модель на всех train данных
        best_model = cb.CatBoostRegressor(
            **best_params,
            loss_function='Huber:delta=0.5',
            cat_features=cat_features,
            task_type='CPU',
            devices='0',
            early_stopping_rounds=200,
            random_state=42,
            eval_metric='RMSE',
            verbose=100,
        )
        best_model.fit(X_train, y_train)

        # Предсказание на тестовых данных
        y_pred = best_model.predict(X_test)

        best_model.save_model('catboost_model.cbm')

        return best_model, best_params, y_pred

    def result_sum(self, test_preduction):
        # Группировка по Магазин и Товар
        agg_df = test_preduction.groupby(['Магазин', 'Товар']).agg({
            'Реальные значения': 'sum',
            'Предсказанные значения': 'sum'
        }).reset_index()

        # Разница между реальными и предсказанными
        agg_df['Разница'] = agg_df['Реальные значения'] - agg_df['Предсказанные значения']
        return agg_df

    def view_results_refactor_values(self, test_preduction, y_pred, y_test):
        test_preduction_copy = test_preduction.copy()

        test_preduction_copy['Предсказанные значения'] = y_pred

        y_true = test_preduction_copy['Реальные значения']
        y_pred = test_preduction_copy['Предсказанные значения'].clip(lower=0)

        y_pred =  np.round(y_pred).astype(int)
        y_true =  np.round(y_true).astype(int)

        # y_pred = np.floor(y_pred + 0.5).astype(int)
        # y_true = np.floor(y_true + 0.5).astype(int)

        test_preduction_copy['Реальные значения'] = y_true
        test_preduction_copy['Предсказанные значения'] = y_pred

        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        print(f'Root Mean Squared Error: {rmse}')

        mae = mean_absolute_error(y_true, y_pred)
        print(f'Mean Absolute Error: {mae}')

        result_metric_diff = 0
        result_real_values = 0
        result_metric_izl = 0
        for i in range(len(y_true)):
            result_metric = y_true.iloc[i] - y_pred.iloc[i]

            if result_metric > 0:
                result_metric_diff += result_metric

            elif result_metric == 0:
                result_real_values += y_true.iloc[i]

            else:
                result_metric_izl += result_metric

        result_metric_diff_count = 0
        result_real_values_count = 0
        result_metric_izl_count = 0
        for i in range(len(y_true)):
            result_metric = y_true.iloc[i] - y_pred.iloc[i]

            if result_metric > 0:
                result_metric_diff_count += 1

            elif result_metric == 0:
                result_real_values_count += 1

            else:
                result_metric_izl_count += 1


        print()
        print(f'Диффиктура (Реальное значение): {result_metric_diff}')
        print(f'Излишки (Реальное значение): {np.abs(result_metric_izl)}')
        print()
        print(f'Диффиктура (Количество): {result_metric_diff_count}')
        print(f'Излишки (Количество): {np.abs(result_metric_izl_count)}')
        print()
        print(f'Идепльно предсказанных значений (Количество): {result_real_values_count}')
        print(f'Идепльно предсказанных значений (Реальное значение): {result_real_values}')
        print()
        print(f'Сумма реальных продаж за 7 дней: {y_true.sum()}')
        print(f'Сумма предсказанных продаж за 7 дней: {y_pred.sum()}')
        print()
        # Подсчет количества каждого уникального значения
        unique_true, counts_true = np.unique(y_true, return_counts=True)
        unique_pred, counts_pred = np.unique(y_pred, return_counts=True)

        # Формируем DataFrame для реальных значений
        true_counts_df = pd.DataFrame({
            'Значение': unique_true,
            'Количество': counts_true
        })

        # Формируем DataFrame для предсказанных значений
        pred_counts_df = pd.DataFrame({
            'Значение': unique_pred,
            'Количество': counts_pred
        })

        # Вывод результатов в виде таблицы
        print("Распределение реальных значений:")
        print(true_counts_df)

        print("\nРаспределение предсказанных значений:")
        print(pred_counts_df)
        test_preduction_cat_1_difference = self.result_sum(test_preduction_copy)
        print('Общая разница между предсказанными и реальными значениями: ', test_preduction_cat_1_difference['Разница'].abs().sum())

        return test_preduction_copy

    def first_learning_model(self, df):
        df_copy = df.copy()

        df_with_lags = self.add_lag_values(df_copy)

        (df_encoding, numerical_columns, cat_columns,
         label_encoder_product, label_encoder_shop) = (
            self.encoding_futures(df_with_lags))

        X_train, y_train, X_test, y_test, test_preduction = (
            self.train_and_test(df_encoding, numerical_columns, cat_columns))

        best_model, best_params, y_pred = (
            self.optimize_catboost(X_train, y_train, X_test, y_test, cat_columns))

        test_prediction = self.view_results_refactor_values(test_preduction, y_pred, y_test)

        test_prediction['Товар'] = (label_encoder_product
                                         .inverse_transform(test_prediction['Товар']))
        test_prediction['Магазин'] = (label_encoder_shop
                                           .inverse_transform(test_prediction['Магазин']))

        return test_prediction



# 🔹 Лучшие параметры: {'iterations': 3639, 'learning_rate': 0.04499952517598313, 'depth': 6, 'l2_leaf_reg': 33.18064326468688, 'random_strength': 4.323500005207879, 'bootstrap_type': 'Poisson', 'min_data_in_leaf': 65, 'max_ctr_complexity': 2}
# 0:	learn: 5.6515361	total: 28.7ms	remaining: 1m 44s
# 100:	learn: 4.5972835	total: 2.56s	remaining: 1m 29s
# 200:	learn: 3.9435019	total: 4.99s	remaining: 1m 25s
# 300:	learn: 3.5066340	total: 7.43s	remaining: 1m 22s
# 400:	learn: 3.2626511	total: 10.2s	remaining: 1m 22s
# 500:	learn: 3.1107544	total: 12.9s	remaining: 1m 20s
# 600:	learn: 3.0085215	total: 15.6s	remaining: 1m 18s
# 700:	learn: 2.9158827	total: 18.3s	remaining: 1m 16s
# 800:	learn: 2.8411816	total: 21s	remaining: 1m 14s
# 900:	learn: 2.7856606	total: 23.7s	remaining: 1m 11s
# 1000:	learn: 2.7412525	total: 26.4s	remaining: 1m 9s
# 1100:	learn: 2.7063243	total: 29.1s	remaining: 1m 7s
# 1200:	learn: 2.6779118	total: 31.8s	remaining: 1m 4s
# 1300:	learn: 2.6589440	total: 34.6s	remaining: 1m 2s
# 1400:	learn: 2.6446519	total: 37.4s	remaining: 59.7s
# 1500:	learn: 2.6305871	total: 40.1s	remaining: 57.1s
# 1600:	learn: 2.6190014	total: 42.8s	remaining: 54.5s
# 1700:	learn: 2.6072308	total: 45.5s	remaining: 51.8s
# 1800:	learn: 2.5969923	total: 48.2s	remaining: 49.2s
# 1900:	learn: 2.5878280	total: 50.9s	remaining: 46.6s
# 2000:	learn: 2.5793335	total: 53.7s	remaining: 43.9s
# 2100:	learn: 2.5712396	total: 56.4s	remaining: 41.3s
# 2200:	learn: 2.5637613	total: 59.1s	remaining: 38.6s
# 2300:	learn: 2.5556848	total: 1m 1s	remaining: 36s
# 2400:	learn: 2.5484596	total: 1m 4s	remaining: 33.3s
# 2500:	learn: 2.5418277	total: 1m 7s	remaining: 30.6s
# 2600:	learn: 2.5361106	total: 1m 10s	remaining: 28s
# 2700:	learn: 2.5303465	total: 1m 12s	remaining: 25.3s
# 2800:	learn: 2.5239607	total: 1m 15s	remaining: 22.6s
# 2900:	learn: 2.5188136	total: 1m 18s	remaining: 19.9s
# 3000:	learn: 2.5139653	total: 1m 21s	remaining: 17.2s
# 3100:	learn: 2.5095200	total: 1m 23s	remaining: 14.5s
# 3200:	learn: 2.5045780	total: 1m 26s	remaining: 11.8s
# 3300:	learn: 2.4997665	total: 1m 29s	remaining: 9.13s
# 3400:	learn: 2.4951682	total: 1m 31s	remaining: 6.42s
# 3500:	learn: 2.4905915	total: 1m 34s	remaining: 3.73s
# 3600:	learn: 2.4865180	total: 1m 37s	remaining: 1.02s
# 3638:	learn: 2.4849874	total: 1m 38s	remaining: 0us
# Root Mean Squared Error: 2.769055646413111
# Mean Absolute Error: 0.6604010025062657
#
# Диффиктура (Реальное значение): 3002
# Излишки (Реальное значение): 2268
#
# Диффиктура (Количество): 1999
# Излишки (Количество): 850
#
# Идепльно предсказанных значений (Количество): 5131
# Идепльно предсказанных значений (Реальное значение): 755
#
# Сумма реальных продаж за 7 дней: 7414
# Сумма предсказанных продаж за 7 дней: 6680
#
# Распределение реальных значений:
# Значение  Количество
# 0          0        5287
# 1          1        1651
# 2          2         567
# 3          3         154
# 4          4          91
# 5          5          58
# 6          6          29
# 7          7          17
# 8          8          14
# 9          9          16
# 10        10          10
# 11        11          14
# 12        12           6
# 13        13           8
# 14        14           9
# 15        15           7
# 16        16           3
# 17        17           3
# 18        19           2
# 19        20           1
# 20        21           1
# 21        24           1
# 22        25           1
# 23        26           1
# 24        36           1
# 25        38           1
# 26        40           2
# 27        42           1
# 28        44           1
# 29        46           1
# 30        49           1
# 31        52           1
# 32        59           1
# 33        61           1
# 34        62           1
# 35        64           2
# 36        70           1
# 37        76           1
# 38        77           1
# 39        79           1
# 40        82           1
# 41        84           1
# 42        89           1
# 43        90           1
# 44        97           1
# 45        98           1
# 46       100           2
# 47       103           1
# 48       104           2
#
# Распределение предсказанных значений:
# Значение  Количество
# 0          0        6130
# 1          1        1250
# 2          2         264
# 3          3         100
# 4          4          38
# 5          5          37
# 6          6          17
# 7          7          29
# 8          8          17
# 9          9          15
# 10        10          14
# 11        11          11
# 12        12          13
# 13        13           8
# 14        14           2
# 15        15           4
# 16        16           1
# 17        85           1
# 18        86           1
# 19        88           1
# 20        95           1
# 21        96           4
# 22       102           6
# 23       103           2
# 24       104           2
# 25       105           2
# 26       106           3
# 27       108           1
# 28       109           1
# 29       110           2
# 30       111           1
# 31       112           2
# Общая разница между предсказанными и реальными значениями:  3732