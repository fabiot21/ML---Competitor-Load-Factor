import logging
import datetime
from multiprocessing import Pool, cpu_count, Value

# external modules
import pandas as pd
import xgboost as xgb
from google.cloud import bigquery

# local modules
from predict.query_competitor import getCompetitorQuery
from preprocess.preprocess import preProcessCompetitorFlight, handleDummies

# configuration file
from config import *

def getPrediction(model, xtrain_columns, competitor, country):
	'''Return dataframe with load factor estimations'''

	client = bigquery.Client()

	query = getCompetitorQuery(competitor, country)

	logging.info('Querying data from BigQuery')
	# Querying BigQuery 
	df = client.query(query).to_dataframe().reset_index(drop=True)

	if df.empty:
		raise Exception("Skipping prediction for {} due to missing data".format(competitor))

	df_flights = df.copy()

	# Get Day & Month
	df_flights[['day', 'month']] = df_flights.departure_date.apply(lambda value: pd.Series((lambda value: [value.isoweekday(), str(value)[5:7]])(value)))

	logging.info("Processing data")
	# Preprocess data
	with Pool(processes=cpu_count()) as pool:
		df_flights_list = pool.map(preProcessCompetitorFlight, df_flights.iterrows())
	
	# Remove none data from list
	df_flights_list = [df_flight for df_flight in df_flights_list if df_flight is not None]

	if len(df_flights_list) == 0:
		raise Exception("Skipping prediction for {} due to missing data".format(competitor))

	# Create one dataframe with all flights
	result_all_df_competitor = pd.concat(df_flights_list).reset_index(drop=True)

	logging.info('Adding dummy variables')
	# Get dummy variables of dataframe
	result_all_df_competitor = handleDummies(result_all_df_competitor)

	# Get unique id values
	uuid_col = result_all_df_competitor['uuid'].values.tolist()

	logging.info('Matching dataframe with model columns')
	# Match dataframe with model columns
	drop_columns_comp = []
	for col in result_all_df_competitor.columns.tolist():
	    if col not in xtrain_columns:
	        drop_columns_comp.append(col)

	result_all_df_competitor = result_all_df_competitor.drop(drop_columns_comp, axis = 1)

	append_columns_comp = []
	for col in xtrain_columns:
	    if col not in result_all_df_competitor.columns.tolist():
	        append_columns_comp.append(col)

	for col in append_columns_comp:
	    result_all_df_competitor[col] = 0

	result_all_df_competitor = result_all_df_competitor[xtrain_columns]


	logging.info('Getting load factor estimations')
	# Get Estimations
	lf_prediction = model.predict(xgb.DMatrix(result_all_df_competitor))

	result_all_df_competitor['predicted_load_factor'] = lf_prediction.tolist()

	result_all_df_competitor = result_all_df_competitor.round(2)

	result_all_df_competitor['uuid'] = uuid_col

	logging.info('Building result dataframe for {}'.format(competitor))
	# Merge resulting dataframe with original dataframe by uuid
	df_result = df.merge(result_all_df_competitor, how='inner', on='uuid')

	df_result[['carrier', 'flight']] = df_result.flight_no.apply(lambda value: pd.Series((lambda value: [value[:2], int(value[2:])])(value)))

	df_result[['origin', 'destination']] = df_result.route.apply(lambda value: pd.Series((lambda value: [value[:3], value[3:]])(value)))

	df_result['observation_date'] = datetime.date.today()

	df_result = df_result[[
					'observation_date',
					'carrier', 
					'origin',
					'destination', 
					'flight', 
					'departure_date', 
					'departure_time', 
					'predicted_load_factor'
				]]

	return df_result