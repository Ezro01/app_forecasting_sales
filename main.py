import pandas as pd
import numpy as np
import time
from Preprocessing import Preprocessing_data
from Sales_recovery import Recovery_sales
from First_model_learning import First_learning_model
from Next_model_predict import Use_model_predict


def first_model_learn(df_first):
    df_first_copy = df_first.copy()

    processor = Preprocessing_data()
    sales_recovery = Recovery_sales()
    first_model_learn = First_learning_model()

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

def main():
    df_first = pd.read_csv("Dataframe_500_tovars_magazins.csv", parse_dates=["Дата"])
    df_next = pd.read_csv("test_df.csv", parse_dates=["Дата"])

    first_learning_model = first_model_learn(df_first)
    # if os.path.exists('catboost_model.cbm'):
    #     y_pred = model_predict(df_encoding)


if __name__ == "__main__":
    main()





