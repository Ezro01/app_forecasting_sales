from sklearn.preprocessing import LabelEncoder, MinMaxScaler
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error
import catboost as cb
import optuna
from DB_operations import ModelStorage

class First_learning_model:
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

            "Заказы_правка" : 'Смоделированные_заказы',
        }

        df_copy = df_copy.rename(columns=column_rename_map)

        return df_copy

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

        encod_columns = ['Товар', 'Магазин', 'Категория', 'ПотребГруппа', 'МНН']
        df_encoding[encod_columns] = df_encoding[encod_columns].astype(str)

        # Кодируем столбец 'Товар' с помощью LabelEncoder только для обучающих данных
        label_encoder_product = LabelEncoder()
        df_encoding['Товар'] = label_encoder_product.fit_transform(df_encoding['Товар'])

        label_encoder_shop = LabelEncoder()
        df_encoding['Магазин'] = label_encoder_shop.fit_transform(df_encoding['Магазин'])

        label_encoder_category = LabelEncoder()
        df_encoding['Категория'] = label_encoder_category.fit_transform(df_encoding['Категория'])

        label_encoder_potreb_group = LabelEncoder()
        df_encoding['ПотребГруппа'] = label_encoder_potreb_group.fit_transform(df_encoding['ПотребГруппа'])

        label_encoder_mnn = LabelEncoder()
        df_encoding['МНН'] = label_encoder_mnn.fit_transform(df_encoding['МНН'])


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

        return (df_encoding, numerical_columns, cat_columns, label_encoder_product, 
            label_encoder_shop, label_encoder_category, 
            label_encoder_potreb_group, label_encoder_mnn, scaler)

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

    def learning_catboost(self, X_train, y_train, X_test, y_test, cat_features):

        model = cb.CatBoostRegressor(
            iterations=3000,
            eval_metric='RMSE',
            loss_function='Huber:delta=0.5',
            cat_features=cat_features,
            task_type='CPU',
            devices='0',
            early_stopping_rounds=200,
            verbose=100,
            random_state=42,
            use_best_model=True,
        )

        model.fit(
            X_train, y_train,
            eval_set=(X_test, y_test),
        )

        # Предсказание на тестовых данных
        y_pred = model.predict(X_test)

        # model.save_model('catboost_model.cbm')

        return model, y_pred

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

    def first_learning_model(self, df, db):
        df_copy = df.copy()

        df_copy = self.first_data_type_refactor(df_copy)

        df_with_lags = self.add_lag_values(df_copy)

        (df_encoding, numerical_columns, cat_columns,
         label_encoder_product, label_encoder_shop, 
         label_encoder_category, label_encoder_potreb_group, 
         label_encoder_mnn, scaler) = (
            self.encoding_futures(df_with_lags))


        X_train, y_train, X_test, y_test, test_preduction = (
            self.train_and_test(df_encoding, numerical_columns, cat_columns))

        model, y_pred = (
            self.learning_catboost(X_train, y_train, X_test, y_test, cat_columns))

        test_prediction = self.view_results_refactor_values(test_preduction, y_pred, y_test)

        test_prediction['Товар'] = (label_encoder_product
                                         .inverse_transform(test_prediction['Товар']))
        test_prediction['Магазин'] = (label_encoder_shop
                                           .inverse_transform(test_prediction['Магазин']))

        model_storage = ModelStorage(db)
        model_storage.save_models(label_encoder_product, label_encoder_shop, label_encoder_category, label_encoder_potreb_group, label_encoder_mnn, scaler, model, comment='first_learning_model')

        return test_prediction