#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 09:41:04 2024

@author: trevor
"""
import matplotlib.pyplot as plt
import numpy as np
 
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
    