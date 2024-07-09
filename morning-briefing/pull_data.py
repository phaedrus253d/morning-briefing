# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import os
import json
import datetime

import requests
from requests.auth import HTTPBasicAuth
import python_weather
import asyncio
import openmeteo_requests

import requests_cache
from retry_requests import retry

#from datetime import datetime

import warnings
columnsToKeep = ['guid', 'start', 'finish', 'name', 'is_deleted']




def load(pullNew = True, location='./data', debug=False, backup=True, verbose=True):
    columnsToKeep = ['guid', 'start', 'finish', 'name', 'is_deleted']
    try:
        if verbose:
            print("Loading saved database")
        db = pd.read_pickle(location+'/db.pkl')
    except:
        import_exported_database()
        tableNames = [ x[:-4] for x in os.listdir(location)]
        db = pd.Series(index=tableNames, dtype=object)
        for index, value in db.items():
            db[index] = pd.read_pickle(location+'/'+str(index)+'.pkl')


        db = db['time_interval2']
    if pullNew:
        oldNumEntries = db.shape[0]
        if verbose:
            print("Old data had", db.shape[0],"entries")
        if backup:
            backup=location+'/backups/db-'+datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')+'.pkl'
            db.to_pickle(backup)
        if debug:
            log = pd.read_pickle('./log.pkl')
        else:
            log = pull_data()
        if verbose:
            print("Retrieved data had", log.shape,"entries")
        log = log.rename(columns={"guid_x":"guid", "from":"start", "to":"finish","deleted":"is_deleted"})
        log['guid'] = log['guid'].str.upper()

        db = pd.concat([db[columnsToKeep], log[columnsToKeep]])
        db = db.drop_duplicates()

        if verbose:
            print("Added", db.shape[0]-oldNumEntries, "entries")
        db.to_pickle(location+"/db.pkl")
    return db[columnsToKeep]


# Check if we need to convert sqlite to pandas.
# If there are not pandas databases in ./data and there
# is a .database.db3 file it ensures ./data exists and
# calls convert_sql_to_pandas
def import_exported_database(db = './.database.db3', overwrite=False):
    if not os.path.exists('./data/time_interval2.pkl') or overwrite:
        if os.path.exists(db):
            if not os.path.exists('./data'):
                os.makedirs('./data')
            convert_sql_to_pandas(db)
            db = pd.read_pickle('./data/time_interval2.pkl')
            return db[columnsToKeep]

        else:
            print("No sqlite3 or pandas data found, exiting")
            exit()




def convert_sql_to_pandas(db):
    con = sqlite3.connect(db)
    sql_query = """SELECT name FROM sqlite_master
      WHERE type='table';"""
    cur = con.cursor()
    cur.execute(sql_query)
    tables = cur.fetchall()
    for table in tables:
        pd_table = pd.read_sql_query("SELECT * from "+table[0], con)
        pd_table.to_pickle('./data/'+table[0]+'.pkl')

    fix_time_interval()

def merge_current_and_new_db(newdb='./.database.db3'):
    db = pd.read_pickle('./data/db.pkl')
    oldNumEntries = db.shape[0]
    print("Old data had", oldNumEntries ,"entries")
    backup='./data/backups/db-'+datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')+'.pkl'
    db.to_pickle(backup)
    newdb = import_exported_database(newdb, overwrite=True)
    db = pd.concat([db, newdb])
    db = db.drop_duplicates()
    print("New data has", db.shape[0],"entries")
    print("Added", db.shape[0]-oldNumEntries, "entries")
    db.to_pickle("./data/db.pkl")

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
    return


def get_weather():
    # config: https://open-meteo.com/en/docs#latitude=42.3751&longitude=-71.1056&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,cloud_cover,wind_speed_10m,uv_index&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch&timezone=America%2FNew_York
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
    	"latitude": 42.3751,
    	"longitude": -71.1056,
    	"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth", "cloud_cover", "wind_speed_10m", "uv_index"],
    	"daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max", "apparent_temperature_min", "sunrise", "sunset", "uv_index_max", "uv_index_clear_sky_max", "precipitation_sum", "rain_sum", "showers_sum", "snowfall_sum", "precipitation_hours", "precipitation_probability_max", "wind_speed_10m_max", "wind_gusts_10m_max"],
    	"temperature_unit": "fahrenheit",
    	"wind_speed_unit": "mph",
    	"precipitation_unit": "inch",
    	"timezone": "auto"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
    hourly_precipitation_probability = hourly.Variables(4).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(5).ValuesAsNumpy()
    hourly_rain = hourly.Variables(6).ValuesAsNumpy()
    hourly_showers = hourly.Variables(7).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(8).ValuesAsNumpy()
    hourly_snow_depth = hourly.Variables(9).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(10).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(11).ValuesAsNumpy()
    hourly_uv_index = hourly.Variables(12).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
    	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
    	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
    	freq = pd.Timedelta(seconds = hourly.Interval()),
    	inclusive = "left"
    )}
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["dew_point_2m"] = hourly_dew_point_2m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["precipitation_probability"] = hourly_precipitation_probability
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["rain"] = hourly_rain
    hourly_data["showers"] = hourly_showers
    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["snow_depth"] = hourly_snow_depth
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["uv_index"] = hourly_uv_index

    hourly_dataframe = pd.DataFrame(data = hourly_data)

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_apparent_temperature_max = daily.Variables(3).ValuesAsNumpy()
    daily_apparent_temperature_min = daily.Variables(4).ValuesAsNumpy()
    daily_sunrise = daily.Variables(5).ValuesAsNumpy()
    daily_sunset = daily.Variables(6).ValuesAsNumpy()
    daily_uv_index_max = daily.Variables(7).ValuesAsNumpy()
    daily_uv_index_clear_sky_max = daily.Variables(8).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(9).ValuesAsNumpy()
    daily_rain_sum = daily.Variables(10).ValuesAsNumpy()
    daily_showers_sum = daily.Variables(11).ValuesAsNumpy()
    daily_snowfall_sum = daily.Variables(12).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(13).ValuesAsNumpy()
    daily_precipitation_probability_max = daily.Variables(14).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(15).ValuesAsNumpy()
    daily_wind_gusts_10m_max = daily.Variables(16).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
    	start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
    	end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
    	freq = pd.Timedelta(seconds = daily.Interval()),
    	inclusive = "left"
    )}
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["apparent_temperature_max"] = daily_apparent_temperature_max
    daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
    daily_data["sunrise"] = daily_sunrise
    daily_data["sunset"] = daily_sunset
    daily_data["uv_index_max"] = daily_uv_index_max
    daily_data["uv_index_clear_sky_max"] = daily_uv_index_clear_sky_max
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["rain_sum"] = daily_rain_sum
    daily_data["showers_sum"] = daily_showers_sum
    daily_data["snowfall_sum"] = daily_snowfall_sum
    daily_data["precipitation_hours"] = daily_precipitation_hours
    daily_data["precipitation_probability_max"] = daily_precipitation_probability_max
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max

    daily_dataframe = pd.DataFrame(data = daily_data)
    return hourly_dataframe, daily_dataframe



#get_weather()
hours, days = get_weather()

#merge_current_and_new_db()