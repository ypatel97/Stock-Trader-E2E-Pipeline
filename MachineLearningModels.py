from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
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

    # split the data (80% train, 20% test)


    # Train the models
    random_forest_model = pyspark_random_forest(features, target)
    gradient_boosting_model = xgb_gradient_boosting(features, target)
    neural_network_model = tf_neural_network(features, target)



def pyspark_random_forest(features, target):

    spark = SparkSession.builder\
        .appName("RandomForestModel")\
        .getOrCreate()



def xgb_gradient_boosting(features, target):
    pass

def tf_neural_network(features, target):
    pass

if __name__ == '__main__':
    main()
