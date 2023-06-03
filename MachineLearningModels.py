from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
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
    # random_forest_model = pyspark_random_forest(train_features, test_features, train_target, test_target)
    gradient_boosting_model = xgb_gradient_boosting(train_features, test_features, train_target, test_target)
    # neural_network_model = tf_neural_network(train_features, test_features, train_target, test_target)

    # TODO: extract the models into an exportable format for Docker/EC2, also add a way to run metrics from new data

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

def xgb_gradient_boosting(train_features, test_features, train_target, test_target, test=True, gs=True):

    # Dropping lowest performing features after testing
    features_to_drop = ['HT_DCPHASE', 'HT_TRENDLINE', 'AROONOSC', 'ROC', 'PPO', 'MACD_Signal', 'T3']
    train_features.drop(features_to_drop, axis=1, inplace=True)
    test_features.drop(features_to_drop, axis=1, inplace=True)

    if gs:
        # Params for the model
        params = {
            "objective": ["reg:squarederror"],
            "eta": np.arange(0.0, 1.0, 0.01),
            "max_depth": range(3, 11),  # 6
            "min_child_weight": [1],
            "alpha": np.arange(0.0, 1.0, 0.01),  # 0.14
            "lambda": np.arange(0.0, 1.0, 0.01),  # 0.98
            "gamma": [0],
            "subsample": [1],
            "colsample_bytree": [1],
            "eval_metric": ["rmse"]
        }

        xgb_model = xgb.XGBRegressor()
        best_params = None
        best_rmse = float('inf')

        for i in range(10, len(train_features)-2):
            train_fold_features = train_features[:i + 1]
            train_fold_target = train_target[:i + 1]

            # Create GridSearchCV object with the XGBRegressor model, parameter grid, and custom scoring
            grid_search = GridSearchCV(estimator=xgb_model, param_grid=params, scoring='neg_mean_squared_error')

            # Fit the grid search model on the training fold data
            grid_search.fit(train_fold_features, train_fold_target)

            # Get the best model
            best_model = grid_search.best_estimator_

            # Make predictions on the validation fold
            val_predictions = best_model.predict(train_features[i + 1:i + 2])

            # Calculate RMSE
            rmse = np.sqrt(mean_squared_error(train_target[i + 1:i + 2], val_predictions))

            # Update best parameters and best RMSE
            if rmse < best_rmse:
                best_params = grid_search.best_params_
                best_rmse = rmse

        print(f'best params: {best_params}, best rmse: {best_rmse}')
    else:
        params = {
            "objective": "reg:squarederror",
            "eta": 0.6,
            "max_depth": 6,  # 6
            "min_child_weight": 1,
            "alpha": .14,  # 0.14
            "lambda": .98,  # 0.98
            "gamma": 0,
            "subsample": 1,
            "colsample_bytree": 1,
            "eval_metric": "rmse"
        }
        # Train model
        xgb_reg_model = xgb.XGBRegressor(**params)
        xgb_reg_model.fit(train_features, train_target)

    # Fit the model
    predictions = xgb_reg_model.predict(test_features)

    # Evaluate the model
    rmse = math.sqrt(mean_squared_error(test_target, predictions))

    if test:
        # xgb.plot_importance(xgb_reg_model)
        # plt.show()
        print(f'rmse: {rmse} param: {n}')

    return xgb_reg_model

def tf_neural_network(train_features, test_features, train_target, test_target, test=True):

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

    activation_func = ['selu', 'linear'] # (selu, linear,

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

if __name__ == '__main__':
    main()
