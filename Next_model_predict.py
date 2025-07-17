import catboost as cb
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, MinMaxScaler

class Use_model_predict:
    def add_lag_values(self, df_first, df_next):
        df_first_copy = df_first.copy()
        df_next_copy = df_next.copy()

        unique_dates_count = df_next_copy['Дата'].nunique()

        df = pd.concat([df_first_copy, df_next_copy], ignore_index=True)

        max_dates = df['Дата'].max()
        days_for_lags = max_dates - pd.Timedelta(unique_dates_count+23)

        df = df[df['Дата'] > days_for_lags]

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

        df = df.dropna()

        unique_dates_list = df_next_copy['Дата'].dt.date.unique()

        df = df[df['Дата'].dt.date.isin(unique_dates_list)]


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

    def set_training_df(self, df_next, numerical_columns, cat_columns):
        df_next_copy = df_next.copy()
        df_predict = df_next_copy[numerical_columns + cat_columns]

        df_predict = np.abs(df_predict.dropna())

        result_preduction = df_predict[['Дата', 'Магазин', 'Товар']].copy()

        return df_predict, result_preduction


    def model_predict(self, df_next, cat_columns):

        df_test = df_next.copy()

        # Создаем пустую модель
        loaded_model = cb.CatBoostRegressor(cat_features=cat_columns)

        # Загружаем параметры
        loaded_model.load_model('catboost_model.cbm')

        y_pred = loaded_model.predict(df_test)


        return y_pred


    def view_results_refactor_values(self, test_preduction, y_pred, label_encoder_product, label_encoder_shop):
        test_preduction_copy = test_preduction.copy()

        test_preduction_copy['Предсказанные значения'] = y_pred

        y_predict = test_preduction_copy['Предсказанные значения'].clip(lower=0)
        y_predict = np.round(y_predict).astype(int)

        test_preduction_copy['Предсказанные значения'] = y_predict

        test_preduction_copy['Товар'] = (label_encoder_product
                                         .inverse_transform(test_preduction_copy['Товар']))
        test_preduction_copy['Магазин'] = (label_encoder_shop
                                           .inverse_transform(test_preduction_copy['Магазин']))

        return test_preduction_copy

    def use_model_predict(self, df_first, df_next):
        df_next_copy = df_next.copy()
        df_first_copy = df_first.copy()

        df_with_lags = self.add_lag_values(df_first_copy, df_next_copy)

        (df_encoding, numerical_columns, cat_columns,
         label_encoder_product, label_encoder_shop) = (
            self.encoding_futures(df_with_lags))

        df_predict, result_preduction = self.set_training_df(df_encoding,
                                                             numerical_columns, cat_columns)

        y_pred = self.model_predict(df_predict, cat_columns)

        result_predict = self.view_results_refactor_values(
            result_preduction, y_pred, label_encoder_product, label_encoder_shop)

        return result_predict