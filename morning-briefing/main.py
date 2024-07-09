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
import os
from datetime import datetime as dt
#import seaborn as sns
import argparse

debug = True


def get_signoff():
    with open('./signoffs.txt') as f:
        signoffs = f.read().splitlines()
        i = np.randint(0, len(signoffs))
        return signoffs[i]

def productivity_report(db, day=None, numDaysPast=1):
    [consciousHours, productive, highLeisure, lowLeisure] = analysis.divide_into_classes(db, day, numDaysPast)
    if numDaysPast == 1:
        time = 'yesterday'
    elif numDaysPast == 365:
        time='last year'
    else:
        time = 'the last {} days in production'.format(numDaysPast)
    report = "You spent {:.0%} of your time {time} in production, {:.0%} "\
        "of your time in high leisure, and {:.0%} of your time in low leisure."\
            .format(productive, highLeisure, lowLeisure, time=time)
    return report

#debug = False

#dbOld = load_data(pullNew=False, location='./dataOld')


# Get today and yesterday
today = dt.date(dt.today())
yesterday = today - pd.Timedelta(1, 'D')


#hours.to_pickle('hours.pkl')
#bins.to_pickle('bins.pkl')
#hours = pd.read_pickle('hours.pkl')
#bins = bin_data(db, yesterday, 24, frequency='h')
#print(bins)
# Print a random signoff
#print(signoffs[random.randint(0, len(signoffs)-1)])


parser=argparse.ArgumentParser()
parser.add_argument("--ti", type=int, default=25)
parser.add_argument("--v", type=int, default=25)
parser.add_argument("--cr", type=int, default=25)
parser.add_argument("--w", type=int, default=25)
parser.add_argument("--bins", type=str, default='', help='Interval to bin by (m, h, d) skipped if not passed')
parser.add_argument("--input", type=str, help = ".npy file containing multiple compositions")
parser.add_argument("--update", action = 'store_true')
parser.add_argument("--graphing", action = 'store_true')
parser.add_argument("--report", action = 'store_true')
parser.add_argument("--debug", action = 'store_true')
args=parser.parse_args()

if args.debug:
    args.update=False
    debug=True
    args.report=True
# Load data
db = pull_data.load(pullNew=args.update, debug=debug, backup=False)



# %% Graphing
if args.graphing:
    [bins, day, stddev] = analysis.data_by_time_of_day(db, step=pd.Timedelta(1, 'm'))
    bins.to_pickle('minutes.pkl')
    day.to_pickle('day.pkl')
    stddev.to_pickle('stddev.pkl')


    #day = pd.read_pickle('day.pkl')
    #stddev = pd.read_pickle('stddev.pkl')
    graphing.plot_day_data(day,stddev)
    graphing.plot_day_data(analysis.categorize_columns(day), analysis.categorize_columns(stddev))


# %%


if args.report:
    print(productivity_report(db, numDaysPast = 365))
    print(productivity_report(db, numDaysPast = 1))