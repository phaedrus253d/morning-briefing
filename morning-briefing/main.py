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
        i = np.random.randint(0, len(signoffs))
        return signoffs[i]

def productivity_report(db):
    yesterday = analysis.divide_into_classes(db)
    lastyear = analysis.divide_into_classes(db, numDaysPast=365)
    hours = yesterday*(yesterday['conscious']/pd.Timedelta(1, 'h'))

    report = "Yesterday you spent {:.1f} hours in production, {:.1f} "\
        "hours in high leisure, and {:.1f} hours in low leisure."\
            .format(hours['productive'],
                    hours['highLeisure'],
                    hours['lowLeisure'])


    compare = yesterday > lastyear
    compare = compare.replace({True: "more", False: "less"})
    diff = ((yesterday - lastyear)/lastyear).abs()
    report += "\nThis is {:.0%} {} productive, {:.0%} {} active leisure, "\
        "and {:.0%} {} passive leisure than average for the last year.".format(
            diff['productive'], compare['productive'],
            diff['highLeisure'],compare['highLeisure'],
            diff['lowLeisure'], compare['lowLeisure'])

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
parser.add_argument("--bins", type=str, default='m', choices=['m', 'h', 'd', 'y'],
                    help='Interval to bin by')
parser.add_argument("--recalculatebins", action = 'store_true')
parser.add_argument("--update", action = 'store_true')
parser.add_argument("--backup", action = 'store_true')
parser.add_argument("--graphing", action = 'store_true')
parser.add_argument("--report", action = 'store_true')
parser.add_argument("--verbose", action = 'store_true')
parser.add_argument("--debug", action = 'store_true')
args=parser.parse_args()

if args.debug:
    args.update=False
    debug=True
    args.report=True
    args.verbose = False

# Load data
db = pull_data.load(pullNew=args.update, debug=debug, backup=args.backup, verbose=args.verbose)

if args.recalculatebins:
    [bins, day, stddev] = analysis.bin_data(db, step=pd.Timedelta(1, args.bins))
#if args.bins != '':
    [bins, day, stddev] = analysis.data_by_time_of_day(db, step=pd.Timedelta(1, 'm'))
    bins.to_pickle('minutes.pkl')
    day.to_pickle('day.pkl')
    stddev.to_pickle('stddev.pkl')

# %% Graphing
if args.graphing:



    #day = pd.read_pickle('day.pkl')
    #stddev = pd.read_pickle('stddev.pkl')
    graphing.plot_day_data(day,stddev)
    graphing.plot_day_data(analysis.categorize_columns(day), analysis.categorize_columns(stddev))


# %%


if args.report:
    report = [
        "Good morning!",
        productivity_report(db),
        get_signoff()]

    [print(x,'\n') for x in report]
