# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import os
import json
import datetime

import requests
from requests.auth import HTTPBasicAuth

#from datetime import datetime

import warnings





def load(pullNew = True, location='./data', debug=False):
    columnsToKeep = ['guid', 'start', 'finish', 'name', 'is_deleted']
    try:
        db = pd.read_pickle(location+'/db.pkl')
    except:
        check_data()
        tableNames = [ x[:-4] for x in os.listdir(location)]
        db = pd.Series(index=tableNames, dtype=object)
        for index, value in db.items():
            db[index] = pd.read_pickle(location+'/'+str(index)+'.pkl')
        
        
        db = db['time_interval2']
    if pullNew:
        oldNumEntries = db.shape[0]
        print("Old data had", db.shape[0],"entries")
        backup=location+'/backups/db-'+datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')+'.pkl'
        db.to_pickle(backup)
        if debug:
            log = pd.read_pickle('./log.pkl')
        else:
            log = pull_data()
        print("Retrieved data had", log.shape,"entries")
        log = log.rename(columns={"guid_x":"guid", "from":"start", "to":"finish","deleted":"is_deleted"})
        log['guid'] = log['guid'].str.upper()
        
        db = pd.concat([db[columnsToKeep], log[columnsToKeep]])
        db = db.drop_duplicates()
        print("New data has", db.shape[0],"entries")
        print("Added", db.shape[0]-oldNumEntries, "entries")
        db.to_pickle(location+"/db.pkl")
    return db[columnsToKeep]


# Check if we need to convert sqlite to pandas.
# If there are not pandas databases in ./data and there
# is a .database.db3 file it ensures ./data exists and
# calls convert_sql_to_pandas
def check_data():
    if not os.path.exists('./data/time_interval2.pkl'):
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
    for table in tables:
        pd_table = pd.read_sql_query("SELECT * from "+table[0], con)
        pd_table.to_pickle('./data/'+table[0]+'.pkl')
    
    fix_time_interval()

    
    
def fix_time_interval():
    table = pd.read_pickle('./data/time_interval2.pkl')
    activities = pd.read_pickle('./data/activity_type.pkl')
    table['start'] = table['start'].apply(lambda x : datetime.datetime.fromtimestamp(float(x)))
    table['finish'] = table['finish'].apply(lambda x : datetime.datetime.fromtimestamp(float(x)))
    table = pd.merge(table,activities[['id','name']], left_on='activity_type_id', right_on='id', how='left')
    table.to_pickle('./data/time_interval2.pkl')
    
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
    edf['from'] = edf['from'].apply(lambda x : datetime.datetime.fromtimestamp(float(x)))
    edf['to'] = edf['to'].apply(lambda x : datetime.datetime.fromtimestamp(float(x)))
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
    
    entries_atimelogger_df = get_atimelogger_intervals(auth_header)
    
    log_df = pd.merge(entries_atimelogger_df, types_atimelogger_df[['guid', 'name']], left_on = 'type_id', right_on = 'guid')
    log_df.to_pickle('./log.pkl')
    return log_df