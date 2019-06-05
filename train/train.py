import math
import random
import logging
import datetime
from time import time
from dateutil.relativedelta import relativedelta
from multiprocessing import Pool, cpu_count, Value


# external modules
import numpy as np
import pandas as pd
from pyod.models.knn import KNN
import xgboost as xgb
from google.cloud import bigquery
from sklearn.metrics import mean_absolute_error

# local modules
from train.query_train import getTrainQuery
from preprocess.preprocess import preProcessFlight, handleDummies

# configuration file
from config import *

def getTrainingDates():
    '''Get start and end dates'''

    now = datetime.datetime.now()

    delta_start = relativedelta(years=YEARS_TRAIN, months=MONTHS_TRAIN)
    date_train_start = now - delta_start

    delta_end = relativedelta(days=1)
    date_train_end = now - delta_end

    DATE1 = date_train_start.strftime("%Y-%m-%d")
    DATE2 = date_train_end.strftime("%Y-%m-%d")

    return (DATE1, DATE2)

def removeOutliers(df_flights_list, contamination=0.001, n_neighbors=1000, method='mean'):
    '''Remove Outliers'''

    lf_array = []
    for flights in df_flights_list:
        lf_array.append(flights.lf.values)  
    lf_array = np.array(lf_array)

    # Train kNN detector
    outlier_model = KNN(contamination=contamination, n_neighbors=n_neighbors, method=method)
    outlier_model.fit(lf_array)

    # Get the prediction labels
    outliers_labels = outlier_model.labels_  # binary labels (0: inliers, 1: outliers)

    df_flights_list = [df_flight for index, df_flight in enumerate(df_flights_list) if outliers_labels[index] == 0]

    return df_flights_list

def getModel(country):
    '''Return trained model'''

    client = bigquery.Client()

    DATE1, DATE2 = getTrainingDates()

    query = getTrainQuery(DATE1, DATE2, country, MAX_AP, STEPS)

    logging.info('Querying data from BigQuery')
    # Querying BigQuery   
    df_flights = client.query(query).to_dataframe().reset_index(drop=True)
    
    # Get Day & Month
    df_flights[['day', 'month']] = df_flights.departure_date.apply(lambda value: pd.Series((lambda value: [value.isoweekday(), str(value)[5:7]])(value)))

    logging.info('Preprocessing data')
    # Preprocess data
    with Pool(processes=cpu_count()) as pool:
        df_flights_list = pool.map(preProcessFlight, df_flights.iterrows())

    # Remove none data from list
    df_flights_list = [df_flight for df_flight in df_flights_list if df_flight is not None]

    logging.info('Removing outliers from dataset')
    # Remove Outliers
    df_flights_list = removeOutliers(df_flights_list)

    # Create one dataframe with all flights
    df_all_flights = pd.concat(df_flights_list)

    logging.info('Adding dummy variables')
    # Get dummy variables of dataframe
    df_all_flights = handleDummies(df_all_flights)

    df_all_flights = df_all_flights[df_all_flights['lf'] != 0]

    logging.info('Getting X, y')
    # Split data for X and y
    X = df_all_flights.drop('lf', axis=1).reset_index(drop=True)
    y = df_all_flights['lf'].reset_index(drop=True)

    # Get traning columns for columns matching
    xtrain_columns = X.columns.tolist()

    logging.info('Get hyperparameters for xgboost algorithm')
    # Get hyperparameters for xgboost algorithm
    all_params = XGB_PARAMS[country]

    params = {
        'max_depth': all_params['max_depth'],
        'colsample_bytree': all_params['colsample_bytree'],
        'subsample': all_params['subsample'],
        'learning_rate': all_params['learning_rate'],
        'objective': 'reg:linear',
        'eval_metric': 'mae',
        'n_jobs': cpu_count()
    }

    logging.info('Transforming dataset to DMatrix')
    dtrain = xgb.DMatrix(X, label=y)

    logging.info('Training xgboost model')

    # Train model
    xgbmodel = xgb.train(params, dtrain, num_boost_round=all_params['num_boost_round'])

    y_pred_train = xgbmodel.predict(xgb.DMatrix(X))
    mae_train = mean_absolute_error(y_pred_train, y)
    logging.info('Train mean absolute error: {}'.format(mae_train))

    return (xgbmodel, xtrain_columns)
