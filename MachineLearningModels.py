from pyspark.sql import SparkSession
import xgboost as xgb
import tensorflow as tf
import pandas as pd

def main():

    # Read and organize the data
    data = pd.read_csv('FormattedData.csv')
    data = data.dropna()
    features = data.drop(columns=['Adjusted Close']).iloc[:-1]

    # The target will be the adjusted close price of NEXT week, to train the ML models on predicting future stock price
    target = data['Adjusted Close'][1:]






def pyspark_random_forest():
    pass

def xgb_gradient_boosting():
    pass

def tf_neural_network():
    pass

if __name__ == '__main__':
    main()
