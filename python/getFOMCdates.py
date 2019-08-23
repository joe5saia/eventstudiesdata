import urllib.request
import re
import datetime
import pandas as pd

## Script to grab a list of all FOMC meetings
# Need to spot check what meetings it grabs for historical events
# The historical dates before 2012 are based off the agenda pdfs which don't always exist


## Code to get recent meetings
url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
raw = urllib.request.urlopen(url).read()
datesRaw = re.findall('monetarypolicy/fomcminutes[0-9]{8}.htm', str(raw))
datesStr = list(set([re.findall('[0-9]{8}', dd)[0] for  dd in datesRaw]))
dates = [datetime.datetime.strptime(dd, '%Y%m%d') for dd in datesStr]

## Code to get historical meetings
# Get years first
url = 'https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm'
start = 1965
end = min(dates).year

for year in range(start, end):
	url = 'https://www.federalreserve.gov/monetarypolicy/fomchistorical' + str(year) + '.htm'
	raw = urllib.request.urlopen(url).read()
	datesRaw = re.findall('monetarypolicy/files/FOMC[0-9]{8}Agenda.pdf', str(raw))
	datesStr.extend(list(set([re.findall('[0-9]{8}', dd)[0] for  dd in datesRaw])))

dates = [datetime.datetime.strptime(dd, '%Y%m%d') for dd in datesStr]
dates.sort()
pd.DataFrame(dates).to_csv('fomc_dates.csv',index=False, header=False)
