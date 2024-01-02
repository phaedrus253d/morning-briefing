# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import os
import json
import datetime

import requests
from requests.auth import HTTPBasicAuth



import warnings





def load_data():
    check_data()
    with open('./data/tables.txt') as f:
        tableNames = f.read().splitlines()
    db = pd.Series(index=tableNames)
    for index, value in db.items():
        db[index] = pd.read_pickle('./data/'+str(index)+'.pkl')
    
    log = pull_data()
    
    return db


# Check if we need to convert sqlite to pandas.
# If there are not pandas databases in ./data and there
# is a .database.db3 file it ensures ./data exists and
# calls convert_sql_to_pandas
def check_data():
    if not os.path.exists('./data/tables.txt'):
        if os.path.exists('./.database.db3'):
            if not os.path.exists('./data'):
                os.makedirs('./data')
            convert_sql_to_pandas()
        else:
            print("No sqlite3 or pandas data found, exiting")
            exit()
            
            


def convert_sql_to_pandas():
    con = sqlite3.connect(".database.db3")
    sql_query = """SELECT name FROM sqlite_master  
      WHERE type='table';"""
    cur = con.cursor()
    cur.execute(sql_query)
    tables = cur.fetchall()
    tableList = []
    for table in tables:
        pd_table = pd.read_sql_query("SELECT * from "+table[0], con)
        pd_table.to_pickle('./data/'+table[0]+'.pkl')
        tableList.append(table[0]+'\n')
        
    with open('./data/tables.txt', 'w') as f:
        f.writelines(tableList)
    
def get_atimelogger_types(auth_header):
    """
    Retrieve types data from aTimeLogger.
    :param auth_header: auth header for request data.
    :return: A dataframe for types data.
    """
    r_type = requests.get("https://app.atimelogger.com/api/v2/types",
                      auth=auth_header)

    types = json.loads(r_type.text)
    tdf = pd.DataFrame.from_dict(types['types'])
    
    return tdf

def get_atimelogger_intervals(auth_header, INTERVAL_MAX=100):
    """
    Retrieve new intervals data from aTimeLogger. Number of entries is limited by INTERVAL_MAX.
    :param auth_header: auth header for request data.
    :return: A dataframe for intervals data.
    """

    r_interval = requests.get("https://app.atimelogger.com/api/v2/intervals",
                              params={'limit': INTERVAL_MAX, 'order': 'desc'},
                              auth=auth_header)
    intervals = json.loads(r_interval.text)
    edf = pd.DataFrame.from_dict(intervals['intervals'])

    # Convert to times
    #edf['from'] = edf['from'].apply(lambda x : datetime.date.fromtimestamp(float(x)))
    #edf['to'] = edf['to'].apply(lambda x : datetime.date.fromtimestamp(float(x)))
    edf['type'] = edf['type'].astype(str)
    edf['type_id'] = edf['type'].str.extract(r": \'(.*?)\'")
    return edf


def pull_data():
    warnings.filterwarnings("ignore")
    
    #START_DATE = (datetime.now() - pd.DateOffset(days=30)).strftime("%Y-%m-%d")
    #END_DATE =  (datetime.now() + pd.DateOffset(days=1)).strftime("%Y-%m-%d")
    #print(f'Data from {START_DATE} through {END_DATE}, not including the latest date.')
    
    with open("./credentials.json", "r") as file:
        credentials = json.load(file)
        atimelogger_cr = credentials['atimelogger']
        USERNAME = atimelogger_cr['USERNAME']
        PASSWORD = atimelogger_cr['PASSWORD']
    
    auth_header = HTTPBasicAuth(USERNAME, PASSWORD)
    
    types_atimelogger_df = get_atimelogger_types(auth_header)
    types_atimelogger_df.head(1)
    
    entries_atimelogger_df = get_atimelogger_intervals(auth_header)
    
    log_df = pd.merge(entries_atimelogger_df, types_atimelogger_df[['guid', 'name']], left_on = 'type_id', right_on = 'guid')
    log_df.head(1) 
    log_df.to_pickle('./log.pkl')
    return log_df