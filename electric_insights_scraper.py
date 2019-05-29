# -*- coding: utf-8 -*-
"""
Data scraper for Electric Insights
Dates are given in GMT, generation in GW
Author: Ayrton Bourn - ayrtonbourn@outlook.com
"""


""" Imports """
import requests
import pandas as pd
from datetime import datetime


""" Functions """
def get_json(start_date, end_date, time_group):  
    ## Convert dates to a format ok for the API
    start_date = datetime.strftime(start_date, '%Y-%m-%d')
    end_date = datetime.strftime(end_date, '%Y-%m-%d')
    
    ## Make the API call and parse as JSON
    response = requests.get('http://drax-production.herokuapp.com/api/1/generation-mix?date_from={}&date_to={}&group_by={}'.format(start_date, end_date, time_group))
    r_json = response.json()
    
    return r_json

def json_to_df(r_json):
    ## Extract relevant lists of dicts and vals from json
    list_of_gen_dicts = [item['value'] for item in r_json]
    list_of_start_times = [item['start'] for item in r_json]
    
    ## Use lists of dicts and vals to create df
    df = pd.DataFrame(list_of_gen_dicts)
    df['UTC'] = list_of_start_times

    return df

def process_df(df):
    ## Extract interconnector and pumped storage data from column of dictionaries in df
    df = df.drop(columns='pumpedStorage')
    
    df_net_interconnectors = pd.DataFrame(list(df['balance'].values))
    df = df.merge(df_net_interconnectors, left_index=True, right_index=True)
    df = df.drop(columns=['balance', 'exports', 'imports'])
    
    ## Format datetimes and set as index
    df['UTC'] = pd.to_datetime(df['UTC'])
    df = df.set_index('UTC')
    
    ## Rename columns
    df = df.rename(columns={'pumpedStorage':'pumped_storage'})
    
    return df

def date_range_to_proc_EI_df(start_date, end_date):
    time_group = '30m'
    
    r_json = get_json(start_date, end_date, time_group)
    df = json_to_df(r_json)
    df = process_df(df)
    
    return df 