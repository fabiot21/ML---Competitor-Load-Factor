import numpy as np
import pandas as pd

# configuration file
from config import MAX_AP, STEPS

def fillPrice0(df_flight):
    '''Fill price of ap 0 with ap 1'''
    df_flight.at[0, 'price'] = df_flight.at[1, 'price']
    return df_flight

def fillBetweenPrices(df_flight):
    '''Interpolate missing prices'''
    df_flight['price'] = df_flight['price'].interpolate(limit=3)
    return df_flight

def applyShiftPrice(df_flight, steps):
    '''Shift prices for lag variables'''
    for i in range(1, steps + 1):
        df_flight['price_{}'.format(i)] = df_flight['price'].shift(-i)
        
    df_flight = df_flight.dropna().reset_index(drop=True)
    return df_flight

def applyPriceDelta(df_flight, steps):
    '''Get delta of lag variables'''

    for i in reversed(range(2, steps + 1)):
        df_flight['price_delta_{}'.format(i - 1)] = (df_flight['price_{}'.format(i - 1)] / df_flight['price_{}'.format(i)]) - 1
        
    df_flight['price_delta_0'] = (df_flight['price'] / df_flight['price_1']) - 1
    
    return df_flight

def handleDummies(df):
    '''Get dummy variables'''
    df = pd.concat([df, pd.get_dummies(df['day'], prefix="day")], axis=1).drop('day', axis=1)
    df = pd.concat([df, pd.get_dummies(df['departure_hour'], prefix="hr")], axis=1).drop('departure_hour', axis=1)
    df = pd.concat([df, pd.get_dummies(df['month'], prefix="month")], axis=1).drop('month', axis=1)
    df = pd.concat([df, pd.get_dummies(df['route'], prefix="route")], axis=1).drop('route', axis=1)
        
    return df

def preProcessData(row, train):
    '''Process data of every flights'''
    df_flight = pd.DataFrame(row['data'])
    
    if train:
        # Remove canceled flights (without ap 0)  or flights that dont have the required history
        if not (0 in df_flight['ap'].values and df_flight.shape[0] == MAX_AP + STEPS + 1):
            return None
        
        df_flight = fillPrice0(df_flight)
        
        df_flight = fillBetweenPrices(df_flight)
    else:
        # Remove flights that dont have the required history
        if not df_flight.shape[0] == STEPS + 1:
            return None

    # Remove flights that still have null prices
    if not (df_flight[df_flight['price'].isna()].empty):
        return None    

    df_flight = applyShiftPrice(df_flight, STEPS)

    df_flight = applyPriceDelta(df_flight, STEPS)

    return df_flight

def preProcessFlight(data, train=True):
    row = data[1]
    
    new_df = preProcessData(row, train)
    try:
        new_df['route'] = row['route']
        new_df['month'] = row['month']
        new_df['departure_hour'] = row['departure_hour']
        new_df['day'] = row['day']
        if not train:
            new_df['uuid'] = row['uuid']
    except:
        return None
    
    return new_df

def preProcessCompetitorFlight(data):
    return preProcessFlight(data, False)