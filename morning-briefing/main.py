#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 13:17:33 2024

@author: trevor
"""
import pull_data

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime as dt
#import seaborn as sns
import matplotlib.pyplot as plt
debug = True

def bin_data(db, startTime, numBins, frequency='H', t=None):
    # If we pass in the date_range, add an extra index to avoid overindexing
    if t:
        t = pd.date_range(t[0], periods=len(t)+1, freq=t.freq)
    # otherwise generate the date range ourselves
    else:
        t = pd.date_range(startTime, periods=numBins+1, freq=frequency)
    bins = pd.DataFrame(pd.Timedelta(0), index=t, columns=db['name'].unique())
    
    # Cut out any data that ended before the start time
    #db = db[db['finish'] < startTime]
    
    binSize = t[1] - t[0]
    for i in range(0,numBins):
        if i%1000 == 0 and len(t) > 10000:
            print("progress: {:.2f}%".format(i/t.shape[0]*100))
        startedBeforeBegin = db['start']<t[i]
        startedBeforeEnd = db['start'] < t[i+1]
        endedAfterBegin = db['finish'] > t[i]
        endedAfterEnd = db['finish'] > t[i+1]
        
        
        spans = db[startedBeforeBegin & endedAfterEnd]
        if spans.shape[0]>0:
            bins.loc[t[i]][spans.iloc[0]['name']] += binSize
        else:
            startedWithin = startedBeforeEnd & ~startedBeforeBegin
            endedWithin = ~endedAfterEnd & endedAfterBegin
            partials = db[startedWithin | endedWithin]
            for index, row in partials.iterrows():
                bins.loc[t[i]][row['name']] += min(t[i+1], row['finish']) - max(t[i], row['start'])
        
    return fix_columns(bins.iloc[:-1])

def fix_columns(bins):
    if 'Walk' in bins.columns:
        bins.loc[:,'Exercise'] += bins['Walk']
        bins = bins.drop('Walk', axis=1)
        
    if 'Data analysis' in bins.columns:
        bins['Learning'] += bins['Data analysis']
        bins = bins.drop('Data analysis', axis=1)
        
    if 'Gold panning' in bins.columns:
        bins['Experiences'] += bins['Gold panning']
        bins = bins.drop('Gold panning', axis=1)
        
    if 'Liesure' in bins.columns:
        bins['Leisure'] = bins['Liesure']
        bins = bins.drop('Liesure', axis=1)
    
    return bins
    
def categorize_columns(bins):
    new = pd.DataFrame(index = bins.index)
    new['Sleep'] = bins['Sleep']
    new['Work'] = bins[['TA', 'NASA', 'Work(tutoring)', 'Lectures/class', 'Coursework', 'Research']].sum(axis=1)
    new['Obligations'] = bins[['Help friend', 'Exercise', 'Family', 'Chores']].sum(axis=1)
    new['Volunteering'] = bins[['Volunteering', 'Fire/EMS']].sum(axis=1)
    new['High leisure'] = bins[['Mikayla', 'Music', 'Experiences', 'You Na', 'Learning', 'Philosophy', 'Creation', 'Social']].sum(axis=1)
    new['Low leisure'] = bins[['Media', 'Leisure', 'Video games']].sum(axis=1)
    return new

def data_by_time_of_day(db, firstDay = None, lastDay = None, step = pd.Timedelta(1, 'h')):
    #Trim incomplete days
    if not firstDay:
        firstDay = dt.date(db['start'].min()+pd.Timedelta(23, 'h'))
    if not lastDay:
        lastDay = dt.date(db['finish'].max())
    numDays = lastDay - firstDay
    print("Binning data")
    if debug:
        bins = pd.read_pickle('minutes.pkl')
    else:
        bins = bin_data(db, firstDay, int(numDays/step), frequency = step)
    
    print("Calculating intervals from",bins.index[0],"to",bins.index[-1])
    #print("Minimum bin:", bins.sum(axis=1).min())
    #print("Maximum bin:", bins.sum(axis=1).max())
    
    
    # convert index to time of day by subtracting off the day
    bins.index = bins.index - bins.index.floor('d')
    times = bins.index.unique()
    day = pd.DataFrame(index=times, columns=bins.columns)
    sums = pd.DataFrame(index=times, columns=bins.columns)
    stddev = pd.DataFrame(index=times, columns=bins.columns)
    for t in times:
        binsInRange = bins[bins.index == t]
        day.loc[t] = binsInRange.mean()
        stddev.loc[t] = binsInRange.std()
    
    
    return bins, day, stddev
 
def plot_day_data(day, stddev):
    print(day.sum().sum())
    timeStep = (day.index[1]-day.index[0])
    day = day/timeStep
    stddev = stddev/timeStep
    day.index = np.linspace(0, 24, num=day.shape[0])
    #day = day.astype('timedelta64[ns]')
    print(day.sum().sum())
    fig, ax = plt.subplots(1, 1)
    ax.stackplot(day.index, day.values.T, labels = day.columns)
    ax.legend()
    #plt.plot(day['Sleep'])
    #plt.fill_between(day['Sleep'] - stddev['Sleep'], day['Sleep']+stddev['Sleep'], color='b', alpha=0.2)
    plt.show()
    
    
def get_signoff():
    with open('./signoffs.txt') as f:
        signoffs = f.read().splitlines()
        i = np.randint(0, len(signoffs))
        return signoffs[i]
    

debug = False
# Load data
db = pull_data.load(pullNew=True, debug=debug)
#dbOld = load_data(pullNew=False, location='./dataOld')
print(db.shape)
# Load list of signoffs from file


today = dt.date(dt.today())
yesterday = today - pd.Timedelta(1, 'D')

# %%
[bins, day, stddev] = data_by_time_of_day(db, step=pd.Timedelta(1, 'm'))
bins.to_pickle('minutes.pkl')
day.to_pickle('day.pkl')
stddev.to_pickle('stddev.pkl')


#day = pd.read_pickle('day.pkl')
#stddev = pd.read_pickle('stddev.pkl')
plot_day_data(day,stddev)
plot_day_data(categorize_columns(day), categorize_columns(stddev))


# %%

def productivity_report(db, day=None, numDaysPast=1):
    if not day: # default to yesterday
        day = dt.date(dt.today()) - pd.Timedelta(numDaysPast, 'D')
    data = categorize_columns(bin_data(db, day, numBins=numDaysPast, frequency = 'D')).sum()
    #data.shape()
    consciousHours = data.sum() - data['Sleep']
    productive = (data['Obligations']+data['Work']+data['Volunteering'])/consciousHours
    highLeisure = data['High leisure']/consciousHours
    lowLeisure = data['Low leisure']/consciousHours
    report = "You spent {:.0%} of your time in production, {:.0%} "\
        "of your time in high leisure, and {:.0%} of your time in low leisure."\
            .format(productive, highLeisure, lowLeisure)
    return report

print(productivity_report(db, numDaysPast = 365))
print(productivity_report(db, numDaysPast = 1))

#hours.to_pickle('hours.pkl')
#bins.to_pickle('bins.pkl')
#hours = pd.read_pickle('hours.pkl')
#bins = bin_data(db, yesterday, 24, frequency='h')
#print(bins)
# Print a random signoff
#print(signoffs[random.randint(0, len(signoffs)-1)])
