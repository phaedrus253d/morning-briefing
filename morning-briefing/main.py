#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 13:17:33 2024

@author: trevor
"""
import pull_data
import analysis
import graphing

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime as dt
#import seaborn as sns
import matplotlib.pyplot as plt
debug = True

    
def get_signoff():
    with open('./signoffs.txt') as f:
        signoffs = f.read().splitlines()
        i = np.randint(0, len(signoffs))
        return signoffs[i]
    
def productivity_report(db, day=None, numDaysPast=1):
    if not day: # default to yesterday
        day = dt.date(dt.today()) - pd.Timedelta(numDaysPast, 'D')
    data = analysis.categorize_columns(analysis.bin_data(db, day, numBins=numDaysPast, frequency = 'D')).sum()
    #data.shape()
    consciousHours = data.sum() - data['Sleep']
    productive = (data['Obligations']+data['Work']+data['Volunteering'])/consciousHours
    highLeisure = data['High leisure']/consciousHours
    lowLeisure = data['Low leisure']/consciousHours
    report = "You spent {:.0%} of your time in production, {:.0%} "\
        "of your time in high leisure, and {:.0%} of your time in low leisure."\
            .format(productive, highLeisure, lowLeisure)
    return report

#debug = False
# Load data
db = pull_data.load(pullNew=True, debug=debug, backup=False)
#dbOld = load_data(pullNew=False, location='./dataOld')


# Get today and yesterday
today = dt.date(dt.today())
yesterday = today - pd.Timedelta(1, 'D')

# %% Graphing
[bins, day, stddev] = analysis.data_by_time_of_day(db, step=pd.Timedelta(1, 'm'))
bins.to_pickle('minutes.pkl')
day.to_pickle('day.pkl')
stddev.to_pickle('stddev.pkl')


#day = pd.read_pickle('day.pkl')
#stddev = pd.read_pickle('stddev.pkl')
graphing.plot_day_data(day,stddev)
graphing.plot_day_data(analysis.categorize_columns(day), analysis.categorize_columns(stddev))


# %%



print(productivity_report(db, numDaysPast = 365))
print(productivity_report(db, numDaysPast = 1))

#hours.to_pickle('hours.pkl')
#bins.to_pickle('bins.pkl')
#hours = pd.read_pickle('hours.pkl')
#bins = bin_data(db, yesterday, 24, frequency='h')
#print(bins)
# Print a random signoff
#print(signoffs[random.randint(0, len(signoffs)-1)])
