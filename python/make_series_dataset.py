###############################################################################
"""  make_series_Dataset.py
Author: Joe Saia

Description: Script to take raw data series in data/raw/ and process them into one csv
The raw data is either a csv or an excel notebook that contains information on
a single asset series at a daily frequency

Inputs: data/raw/bloomberg and data/raw/fred. Excel sheets with the series and 
additional information. Directories define the source of the data

Outputs: data/processed/assets.csv and data/processed/returns.csv. CSV files
with a column for the time series of each asset. Either in levels or returns. 
Returns are either log differences or level differences based on how the asset
is quoted. 
"""
###############################################################################

import os
import pandas as pd
import re
import datetime

# Directories
proj_dir = '../'  # '/research/hf_factors/' ## UPDATE THIS
rawdata_dir = proj_dir + 'data/raw/'
bloomberg_dir = rawdata_dir + 'bloomberg/'
fred_dir = rawdata_dir + 'FRED/'
data_dir = proj_dir + "data/processed/"
BLS_dir = proj_dir + 'data/BLS/'
fed_dir = proj_dir + 'data/fed_data/'
futures_dir = proj_dir + 'data/futures/'

# Functions to read in files and clean names
def read_bloom(f, dir):
    """
    function to read in bloomberg data
    Inputs: 
        dir - Directory where data is saved
        f - file name
    Returns: pandas dataframe with date and Last Price columns
    """
    data = pd.read_excel(dir + f, usecols=[0, 1])
    sname = re.sub(r"\.xlsx", "", f)
    sname = re.sub(r"^(\s+)", "", sname)
    sname = re.sub(r" COMDTY", "", sname)
    sname = re.sub(r" CURNCY", "", sname)
    sname = re.sub(r" Index", "", sname)
    return data.rename(index=str, columns={'Last Price': sname})


def read_fred(f, dir):
    """
    function to read in Fred data
    Inputs: 
        dir - Directory where data is saved
        f - file name
    Returns: pandas dataframe with date and value columns
    """
    data = pd.read_excel(dir + f, names=['Date', 'value'], skiprows=10)
    sname = re.sub(r"\.xlsx", "", f)
    sname = re.sub(r"^(\s+)", "", sname)
    sname = re.sub(r" COMDTY", "", sname)
    sname = re.sub(r" CURNCY", "", sname)
    sname = re.sub(r" Index", "", sname)
    return data.rename(index=str, columns={'value': sname})


## Read in data and merge
bloom_files = os.listdir(bloomberg_dir)
data = read_bloom(bloom_files[0], bloomberg_dir)
for f in bloom_files[1:]:
    print('Reading in ' + f)
    tmp = read_bloom(f, bloomberg_dir)
    data = data.merge(tmp, how='outer', on='Date', sort=True)

fred_files = os.listdir(fred_dir)
for f in fred_files:
    print('Reading in ' + f)
    tmp = read_fred(f, fred_dir)
    data = data.merge(tmp, how='outer', on='Date', sort=True)

# Read in GSS data and merge
gss = pd.read_csv(fed_dir + "gss_forward_rates.csv")
gss['date'] = pd.to_datetime(gss['date'])
gss.rename(index=str, columns={"date": "Date"}, inplace=True)
data = data.merge(gss, how='outer', on='Date', sort=True)


# Read in GSW data and merge
gsw = pd.read_csv(fed_dir + "gsw_tips_rates.csv")
gsw['date'] = pd.to_datetime(gsw['date'])
gsw.rename(index=str, columns={"date": "Date"}, inplace=True)
data = data.merge(gsw, how='outer', on='Date', sort=True)

# Read in ED and FF futures data and merge
fut = pd.read_csv(futures_dir + 'FUTURES_cleaned.csv')
fut['date'] = pd.to_datetime(fut['date'])
fut.rename(index=str, columns={"date": "Date"}, inplace=True)
# DFFF_close_meeting  DFFF_subsequent_meeting   DED1Q  DED2Q  DED3Q  DED4Q
data = data.merge(fut, how='outer', on='Date', sort=True)

# Read in FOMC and Bloomberg data
bls_dates = pd.read_csv(BLS_dir + 'employment_dates.csv',
                        names=["Date"], usecols=[2], skiprows=1)
bls_dates.loc[:, 'Date'] = pd.to_datetime(bls_dates['Date'])
bls_dates.loc[:, 'jobsday'] = True
fomc_dates = pd.read_csv(fed_dir + 'fomc_dates.csv', names=["Date"])
fomc_dates.loc[:, 'Date'] = pd.to_datetime(fomc_dates['Date'])
fomc_dates.loc[:, 'fomc'] = True
minutes_dates = pd.read_csv(
    fed_dir + 'minutes_dates.csv', names=["Date"], usecols=[0], skiprows=1)
minutes_dates.loc[:, 'Date'] = pd.to_datetime(minutes_dates['Date'])
minutes_dates.loc[:, 'minutes'] = True


data = data.merge(fomc_dates, how='left', on='Date')
data = data.merge(bls_dates, how='left', on='Date')
data = data.merge(minutes_dates, how='left', on='Date')


data.loc[:, 'jobsday'].fillna(False, inplace=True)
data.loc[:, 'fomc'].fillna(False, inplace=True)
data.loc[:, 'minutes'].fillna(False, inplace=True)
data.loc[data['jobsday'], 'minutes'] = False
data.loc[:, 'wedns'] = (data.loc[:, 'Date'].dt.weekday == 2) & (data.loc[:, 'fomc'] == 0) & \
    (data.loc[:, 'minutes'] == 0) & (data.loc[:, 'jobsday'] == 0)
data.loc[:, 'thur'] = (data.loc[:, 'Date'].dt.weekday == 3) & (data.loc[:, 'fomc'] == 0) & \
                      (data.loc[:, 'minutes'] == 0) & (
                          data.loc[:, 'jobsday'] == 0)
data.loc[:, 'tues'] = (data.loc[:, 'Date'].dt.weekday == 1) & (data.loc[:, 'fomc'] == 0) & \
                      (data.loc[:, 'minutes'] == 0) & (
                          data.loc[:, 'jobsday'] == 0)
data.loc[:, 'zlb'] = (pd.to_datetime('2008-12-17') < data.loc[:, 'Date']
                      ) & (data.loc[:, 'Date'] < pd.to_datetime('2016-12-15'))
data.loc[:, 'fomc-zlb'] = data['fomc'] & (-data['zlb'])
data.loc[:, 'minutes+zlb'] = data['minutes'] | (data['fomc'] & data['zlb'])

data.columns = map(str.upper, data.columns)
# Convert Eurodollar quotes to implied interest rate
data.loc[:, "ED1"] = 100 - data["ED1"]
data.loc[:, "ED2"] = 100 - data["ED2"]
data.loc[:, "ED3"] = 100 - data["ED3"]

# Calculate returns suitably
returns = data[['DATE', 'TUES', 'WEDNS', 'THUR', 'JOBSDAY',
                'FOMC', 'MINUTES', 'ZLB', 'FOMC-ZLB', 'MINUTES+ZLB']]

# These will straight differences
rates = ["ED1", "ED2", "ED3", "VIX", "FEDFUNDS", "DFEDTAR", "DGS30", "DGS20", "DGS10",
         "DGS7", "DGS6MO", "DGS5", "DGS3MO", "DGS3", "DGS2", "DGS1MO", "DGS1",
         "DFII30", "DFII20", "DFII10", "DFII7", "DFII5", "DBAA", "DAAA",
         "TREAS1FORWARD", "TREAS2FORWARD", "TREAS3FORWARD", "TREAS4FORWARD",
         "TREAS5FORWARD", "TREAS6FORWARD", "TREAS7FORWARD", "TREAS8FORWARD",
         "TREAS9FORWARD", "TREAS10FORWARD", "TREAS11FORWARD", "TREAS12FORWARD",
         "TREAS13FORWARD", "TREAS14FORWARD", "TREAS15FORWARD", "TREAS16FORWARD",
         "TREAS17FORWARD", "TREAS18FORWARD", "TREAS19FORWARD", "TREAS20FORWARD",
         "TREAS21FORWARD", "TREAS22FORWARD", "TREAS23FORWARD", "TREAS24FORWARD",
         "TREAS25FORWARD", "TREAS26FORWARD", "TREAS27FORWARD",
         "TREAS28FORWARD", "TREAS29FORWARD",
         "TIPS2FORWARD", "TIPS3FORWARD", "TIPS4FORWARD",
         "TIPS5FORWARD", "TIPS6FORWARD", "TIPS7FORWARD", "TIPS8FORWARD",
         "TIPS9FORWARD", "TIPS10FORWARD", "TIPS11FORWARD", "TIPS12FORWARD",
         "TIPS13FORWARD", "TIPS14FORWARD", "TIPS15FORWARD", "TIPS16FORWARD",
         "TIPS17FORWARD", "TIPS18FORWARD", "TIPS19FORWARD", "TIPS20FORWARD"
         ]

# These will be percent changes
prices = ["CL1", "CL6", "CL12", "CL24", "SI1", "S1", "LC1", "HG1", "GC1", "S5COND", "S5CONS", "S5ENRS", "S5FINL", "S5HLTH", "SPX",
          "S5INDU", "S5INFT", "S5MATR", "S5RLST", "S5TELS", "S5UTIL", "RIY", "RTY", "RAY", "NASDAQ", "USDEUR", "USDGBP", "USDJPY",
          "USDCAD", "USDAUD", "USDMXN"]
returns_raw = ["DFFF_CLOSE_MEETING", "DFFF_SUBSEQUENT_MEETING",
               "DED1Q", "DED2Q", "DED3Q", "DED4Q"]

for s in rates:
    print('calculating returns for ' + s)
    returns.loc[:, s] = 100*data[s].diff(1)
returns.loc[:, 'VIX'] /= 100

for s in prices:
    print('calculating log returns for ' + s)
    returns.loc[:, s] = 100*data[s].pct_change(1)

for s in returns_raw:
    print('adding ' + s + ' to returns dataset (was already returns)')
    returns.loc[:, s] = 100*data[s]

# subtract out SPX daily return from subindices to get relative returns
for s in ["S5COND", "S5CONS", "S5ENRS", "S5FINL", "S5HLTH", "S5INDU", "S5INFT", "S5MATR", "S5RLST", "S5TELS", "S5UTIL"]:
    returns.loc[:, s] -= returns["SPX"]


# Write the datasets to CSV's
returns.to_csv(data_dir + "returns.csv", index=False)
data.to_csv(data_dir + "prices.csv", index=False)
