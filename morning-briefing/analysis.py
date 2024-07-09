#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 09:38:32 2024

@author: trevor
"""
import pandas as pd
from datetime import datetime as dt



def bin_data(db, startTime=None, numBins=None, frequency='h', t=None, allBins = False):

    # If we want the entire range, calculate it ourselves
    if allBins:
        startTime = dt.date(db['start'].min()+pd.Timedelta(23, 'h'))
        endTime = dt.date(db['finish'].max())
        numDays = endTime - startTime
        numBins = int(numDays/pd.Timedelta(1, frequency))

    # If we pass in the date_range, add an extra index to avoid overindexing
    if t:
        t = pd.date_range(t[0], periods=len(t)+1, freq=t.freq)
    # otherwise generate the date range ourselves.
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
            bins.loc[t[i], spans.iloc[0]['name']] += binSize
        else:
            startedWithin = startedBeforeEnd & ~startedBeforeBegin
            endedWithin = ~endedAfterEnd & endedAfterBegin
            partials = db[startedWithin | endedWithin]
            for index, row in partials.iterrows():
                bins.loc[t[i], row['name']] += min(t[i+1], row['finish']) - max(t[i], row['start'])

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

def data_by_time_of_day(db, firstDay = None, lastDay = None, step = pd.Timedelta(1, 'h'), debug=False):
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

def divide_into_classes(db, day=None, numDaysPast=1):
    if not day: # default to yesterday
        day = dt.date(dt.today()) - pd.Timedelta(numDaysPast, 'D')
    data =categorize_columns(bin_data(db, day, numBins=numDaysPast, frequency = 'D')).sum()
    #data.shape()
    consciousHours = data.sum() - data['Sleep']
    out = {
        'conscious' : consciousHours,
        'productive': (data['Obligations']+data['Work']+data['Volunteering'])/consciousHours,
        'highLeisure' : data['High leisure']/consciousHours,
        'lowLeisure' : data['Low leisure']/consciousHours,
            }

    return pd.Series(out)