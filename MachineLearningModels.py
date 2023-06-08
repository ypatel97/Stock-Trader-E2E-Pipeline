from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from sklearn.metrics import mean_squared_error
import xgboost as xgb
import tensorflow as tf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
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
    random_forest_model = pyspark_random_forest(train_features.copy(),
                                                test_features.copy(),
                                                train_target.copy(),
                                                test_target.copy())

    gradient_boosting_model = xgb_gradient_boosting(train_features.copy(),
                                                    test_features.copy(),
                                                    train_target.copy(),
                                                    test_target.copy())

    neural_network_model = tf_neural_network(train_features.copy(),
                                             test_features.copy(),
                                             train_target.copy(),
                                             test_target.copy())

    # Save the models
    path = 'Models/'
    random_forest_model.write().overwrite().save(path+"MLlibModel")
    gradient_boosting_model.save_model(path+"XGBoostModel.txt")
    tf.saved_model.save(neural_network_model, path+"TensorFlowModel")


def pyspark_random_forest(train_features, test_features, train_target, test_target, test=True, feature_engineering=False):

    # Create spark session
    spark = SparkSession.builder \
        .master('local[*]') \
        .config('spark.driver.memory', '15g') \
        .appName('Random Forest Regressor') \
        .getOrCreate()

    # Dropping lowest performing features after testing
    features_to_drop = ['HT_DCPHASE', 'HT_TRENDLINE', 'AROONOSC', 'ROC', 'PPO', 'MACD_Signal', 'T3']
    train_features.drop(features_to_drop, axis=1, inplace=True)
    test_features.drop(features_to_drop, axis=1, inplace=True)

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
        train_scaler = MinMaxScaler(inputCol='features', outputCol='scaled_features')
        train_data = train_scaler.fit(train_data).transform(train_data)

        test_scaler = MinMaxScaler(inputCol='features', outputCol='scaled_features')
        test_data = test_scaler.fit(test_data).transform(test_data)
        featureCol = 'scaled_features'


    rf = RandomForestRegressor(featuresCol=featureCol,
                               labelCol="Adjusted Close",
                               numTrees=30,
                               maxDepth=10,
                               bootstrap=True)

    # Train the Random Forest model
    model = rf.fit(train_data)

    if test:
        # Make predictions on the test data
        predictions = model.transform(test_data)
        # Evaluate the model using RegressionEvaluator
        evaluator = RegressionEvaluator(labelCol="Adjusted Close", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        predictions = predictions.toPandas()
        print(f"Root Mean Squared Error: {rmse}")

    return model

def xgb_gradient_boosting(train_features, test_features, train_target, test_target, test=False):

    # Dropping lowest performing features after testing
    features_to_drop = ['HT_DCPHASE', 'HT_TRENDLINE', 'AROONOSC', 'ROC', 'PPO', 'MACD_Signal', 'T3']
    train_features.drop(features_to_drop, axis=1, inplace=True)
    test_features.drop(features_to_drop, axis=1, inplace=True)

    params = {
        "objective": "reg:squarederror",
        "eta": .67,
        "max_depth": 10,
        "min_child_weight": 1,
        "alpha": 0,
        "lambda": 1,
        "gamma": 0,
        "subsample": 1,
        "colsample_bytree": 1,
        "eval_metric": "rmse"
    }

    # Train model
    xgb_reg_model = xgb.XGBRegressor(**params)
    xgb_reg_model.fit(train_features, train_target)

    if test:
        # Fit the model
        predictions = xgb_reg_model.predict(test_features)

        # Evaluate the model
        rmse = math.sqrt(mean_squared_error(test_target, predictions))
        # xgb.plot_importance(xgb_reg_model)
        # plt.show()
        print(f'rmse: {rmse} ')

    return xgb_reg_model

def tf_neural_network(train_features, test_features, train_target, test_target, test=False):

    # Convert the data to numpy arrays
    X_train = np.array(train_features)
    y_train = np.array(train_target)
    X_test = np.array(test_features)
    y_test = np.array(test_target)

    # Normalize the input features
    mean = np.mean(X_train, axis=0)
    std = np.std(X_train, axis=0)
    X_train = (X_train - mean) / std
    X_test = (X_test - mean) / std

    activation_func = 'selu'

    model = tf.keras.models.Sequential([
        tf.keras.layers.Dense(64, activation=activation_func, input_shape=(X_train.shape[1],)),
        tf.keras.layers.Dense(32, activation=activation_func),
        tf.keras.layers.Dense(16, activation=activation_func),
        tf.keras.layers.Dense(8, activation=activation_func),
        tf.keras.layers.Dense(1)
    ])

    model.compile(optimizer='rmsprop', loss='mean_squared_error')

    model.fit(X_train, y_train, epochs=164, batch_size=8, validation_data=(X_test, y_test))

    if test:
        predictions = model.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        rmse = math.sqrt(mse)
        print(f'rmse: {rmse}')

    return model

if __name__ == '__main__':
    main()
