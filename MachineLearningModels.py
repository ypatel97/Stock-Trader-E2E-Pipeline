from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import xgboost as xgb
import tensorflow as tf
import pandas as pd
import math

def main():

    # Read and organize the data
    data = pd.read_csv('FormattedData.csv')
    data = data.dropna()
    features = data.drop(columns=['Adjusted Close']).iloc[:-1]

    # The target will be the adjusted close price of NEXT week, to train the ML models on predicting future stock price
    target = data['Adjusted Close'][1:].to_frame()
    target = target.set_index(features['timestamp'])
    # Split the data (80% train, 20% test)
    split_ratio = 0.8
    split_idx = math.floor(len(features)*split_ratio)
    features = features.set_index('timestamp')


    # Generate the necessary datasets
    train_features = features.iloc[len(features)-split_idx:]
    test_features = features.iloc[:len(features)-split_idx]
    train_target = target.iloc[len(features)-split_idx:]
    test_target = target.iloc[:len(features)-split_idx]

    # Train the models
    random_forest_model = pyspark_random_forest(train_features, test_features, train_target, test_target)
    gradient_boosting_model = xgb_gradient_boosting(train_features, test_features, train_target, test_target)
    neural_network_model = tf_neural_network(train_features, test_features, train_target, test_target)



def pyspark_random_forest(train_features, test_features, train_target, test_target, eval=True, feature_engineering=False):

    # Create spark session
    spark = SparkSession.builder \
        .master('local[*]') \
        .config('spark.driver.memory', '15g') \
        .appName('Random Forest Regressor') \
        .getOrCreate()

    train_features.drop(['open', 'high', 'low', 'close', 'volume', 'dividend amount'], axis=1, inplace=True)
    test_features.drop(['open', 'high', 'low', 'close', 'volume', 'dividend amount'], axis=1, inplace=True)
    # Convert the Pandas DataFrames to PySpark DataFrames
    train_data = spark.createDataFrame(train_features.join(train_target))
    test_data = spark.createDataFrame(test_features.join(test_target))

    # Select the feature column names
    selected_columns = train_features.columns

    # Create a VectorAssembler instance
    assembler = VectorAssembler(inputCols=list(selected_columns), outputCol='features')

    # Apply the assembler to the training and testing data
    train_data = assembler.transform(train_data)
    test_data = assembler.transform(test_data)
    featureCol = 'features'

    # Feature engineering -- Tends to perform slightly worse than without feature engineering
    if feature_engineering:
        train_scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
        train_data = train_scaler.fit(train_data).transform(train_data)

        test_scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
        test_data = test_scaler.fit(test_data).transform(test_data)
        featureCol = 'scaled_features'


    rf = RandomForestRegressor(featuresCol=featureCol,
                               labelCol="Adjusted Close",
                               numTrees=25,
                               maxDepth=10,
                               bootstrap=False)

    # Train the Random Forest model
    model = rf.fit(train_data)

    if eval:
        # Make predictions on the test data
        predictions = model.transform(test_data)
        # Evaluate the model using RegressionEvaluator
        evaluator = RegressionEvaluator(labelCol="Adjusted Close", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        print(f"Root Mean Squared Error: {rmse}")



    return model


def xgb_gradient_boosting(train_features, test_features, train_target, test_target):
    pass

def tf_neural_network(train_features, test_features, train_target, test_target):
    pass



if __name__ == '__main__':
    main()
