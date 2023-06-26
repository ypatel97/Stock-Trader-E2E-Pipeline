import requests
import json
import time
import pandas
import xgboost as xgb
from pyspark.ml.pipeline import PipelineModel
import tensorflow as tf
from io import StringIO
from TrainingDataRetriever import *


def main():

    # get data
    get_data()

    # read the models
    try:
        xgb_model, rf_model, tf_model = read_models()
    except Exception as e:
        print(str(e))
        print('Failed to read machine learning models')
        return




def get_data():

    MAX_RETRIES = 10
    SECONDS_PER_WEEK = 30#7 * 24 * 60 * 60
    retries = 0

    technical_indicators = ['SMA', 'EMA', 'WMA', 'DEMA', 'TEMA', 'TRIMA', 'KAMA', 'MAMA', 'VWAP', 'T3', 'MACD',
                            'WILLR', 'ADXR', 'APO', 'PPO', 'MOM', 'BOP', 'CMO', 'ROC', 'ROCR',
                            'AROON', 'AROONOSC', 'MFI', 'TRIX', 'ULTOSC', 'DX', 'MINUS_DI', 'PLUS_DI', 'MINUS_DM',
                            'PLUS_DM', 'BBANDS', 'MIDPOINT', 'MIDPRICE', 'SAR', 'TRANGE', 'ATR', 'NATR', 'AD', 'ADOSC',
                            'OBV', 'HT_TRENDLINE', 'HT_SINE', 'HT_TRENDMODE', 'HT_DCPERIOD', 'HT_DCPHASE', 'HT_PHASOR']

    # Continuously retrieve data every week
    while True:
        try:
            pass
            #retrieve_and_store(technical_indicators)
            #format_stored_data(technical_indicators)
        except Exception as e:

            # If failed, keep retrying up to 10 times
            print(f"API request failed: {e}")
            retries += 1

            # If retries exceed 10, skip the week, try next week
            if retries >= MAX_RETRIES:
                print("Max retries reached. Skipping this week.")
                retries = 0
                time.sleep(SECONDS_PER_WEEK)
            continue

        # Read the data
        df = pd.read_csv('FormattedData.csv')

        # Get the most recent row
        df = df.iloc[0, :]


def read_models():

    # Load xgboost gradient boost model
    xgboost_path = 'Models/XGBoostModel.txt'
    xgb_model = xgb.XGBRegressor()
    xgb_model.load_model(xgboost_path)

    # Load pyspark random forest regressor
    rf_path = 'Models/MLlibModel'
    rf_model = PipelineModel.load(rf_path)

    # Load tensor flow deep learning model
    tf_path = 'Models/TensorFlowModel'
    tf_model = tf.saved_model.load(tf_path)

    return xgb_model, rf_model, tf_model


if __name__ == '__main__':
    get_data()