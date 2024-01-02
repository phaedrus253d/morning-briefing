#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 13:17:33 2024

@author: trevor
"""
from pull_data import *

import pandas as pd
import numpy as np
import random
import os

# Load data
db = load_data()

# Load list of signoffs from file
with open('./signoffs.txt') as f:
    signoffs = f.read().splitlines()


# Print a random signoff
#print(signoffs[random.randint(0, len(signoffs)-1)])
