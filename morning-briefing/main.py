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



def bin_data(db, startTime, numBins, frequency='H', t=None):
    # If we pass in the date_range, add an extra index to avoid overindexing
    if t:
        t = pd.date_range(t[0], periods=len(t)+1, freq=t.freq)
    # otherwise generate the date range ourselves
    else:
        t = pd.date_range(startTime, periods=numBins+1, freq=frequency)
    bins = pd.DataFrame(pd.Timedelta(0), index=t, columns=db['name'].unique())
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
    return bins.iloc[:-1]

def data_by_time_of_day(db, firstDay = None, lastDay = None, step = pd.Timedelta(1, 'H')):
    #Trim incomplete days
    if not firstDay:
        firstDay = dt.date(db['start'].min()+pd.Timedelta(23, 'h'))
    if not lastDay:
        lastDay = dt.date(db['finish'].max())
    numDays = lastDay - firstDay
    bins = bin_data(db, firstDay, int(numDays/step), frequency = step)
    print("Generating data from",bins.index[0],"to",bins.index[-1])
    #print("Minimum bin:", bins.sum(axis=1).min())
    #print("Maximum bin:", bins.sum(axis=1).max())
    
    #Sum by hour
    hours = pd.DataFrame(columns=bins.columns)
    for i in range(0,24):
        hours.loc[i] = bins[bins.index.hour == i].sum()
    
    
    return bins, hours
    
    
# Load data
db = pull_data.load(pullNew=True, debug=True)
#dbOld = load_data(pullNew=False, location='./dataOld')
print(db.shape)
# Load list of signoffs from file
with open('./signoffs.txt') as f:
    signoffs = f.read().splitlines()

today = dt.date(dt.today())
yesterday = today - pd.Timedelta(1, 'D')

[bins, hours] = data_by_time_of_day(db, step=pd.Timedelta(1, 'm'))
print(hours.sum().sum())
hours.to_pickle('hours.pkl')
bins.to_pickle('bins.pkl')
#hours = pd.read_pickle('hours.pkl')
#bins = bin_data(db, yesterday, 24, frequency='h')
#print(bins)
# Print a random signoff
#print(signoffs[random.randint(0, len(signoffs)-1)])
