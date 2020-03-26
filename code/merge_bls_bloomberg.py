###############################################################################
# This script takes the CSVs outputted from the clean_bloomberg.py and 
# data_grabs.py and merges them together using the Bloomberg data as 
# authoritative for the overlappping time period
###############################################################################

import pandas as pd
import datetime as dt
import os

bb_mins = pd.read_csv('/app/data/processed/bb_FOMCminutes.csv') # Fed minutes
bb_stats = pd.read_csv('/app/data/processed/bb_FOMCstatements.csv') # Fed Statements
bls = pd.read_csv('/app/data/processed/macro_release_dates.csv') # BLS data

# For testing purposes
bb_mins = pd.read_csv('data/processed/bb_FOMCminutes.csv') # Fed minutes
bb_stats = pd.read_csv('data/processed/bb_FOMCstatements.csv') # Fed Statements
bls = pd.read_csv('data/processed/macro_release_dates.csv') # BLS data

# Fix datetime 
bls.loc[:, 'releasedate'] = pd.to_datetime(bls.loc[:, 'releasedate'])

# Make all the variables that we need to line things up with the BLS data
bb_mins.loc[:,'releasedate'] = bb_mins.apply(lambda row : dt.datetime(row.releaseyear, row.releasemonth, row.releaseday, row.releasehour, row.releaseminute), axis=1)
bb_mins.loc[:,'coveredyear'] = bb_mins.loc[:,'releaseyear']
bb_mins.loc[:,'coveredperiod'] = bb_mins.apply(lambda row : row.releasedate.dayofyear  , axis=1)
bb_mins.loc[:, 'freq'] = 365
bb_mins.rename({'Name':'release'}, inplace = True, axis=1)

# Do it again for the statements
bb_stats.loc[:,'releasedate'] = bb_stats.apply(lambda row : dt.datetime(row.releaseyear, row.releasemonth, row.releaseday, row.releasehour, row.releaseminute), axis=1)
bb_stats.loc[:,'coveredyear'] = bb_stats.loc[:,'releaseyear']
bb_stats.loc[:,'coveredperiod'] = bb_stats.apply(lambda row : row.releasedate.dayofyear  , axis=1)
bb_stats.loc[:, 'freq'] = 365
bb_stats.rename({'Name':'release'}, inplace = True, axis=1)

# Drop any FED data from BLS that's also in the range of Bloomberg Data
bls.drop(bls.index[bls.release.eq('FOMC meeting') & bls.releasedate.dt.date.gt(bb_stats.releasedate.min().date()) & bls.releasedate.dt.date.lt(bb_stats.releasedate.max().date()) ], inplace = True)
bls.drop(bls.index[bls.release.eq('FOMC minutes') & bls.releasedate.dt.date.gt(bb_mins.releasedate.min().date()) & bls.releasedate.dt.date.lt(bb_mins.releasedate.max().date()) ], inplace = True)

# Append the Bloomberg Data
bls = bls.append(bb_stats).append(bb_mins)

bls.to_csv('data/processed/macro_release_dates.csv', index=False)