import urllib.request
import pandas as pd
import zipfile
import xml
import re
import datetime
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import os

# Directories
proj_dir = '/app/'  # '/research/hf_factors/' ## UPDATE THIS
rawdata_dir = proj_dir + 'data/raw/'
bloomberg_dir = rawdata_dir + 'bloomberg/'
fred_dir = rawdata_dir + 'FRED/'
proc_dir = proj_dir + 'data/processed/'
down_dir = proc_dir + 'downloads/'


def gss_forward():
    ################################################################################
    # Download GSS data on forward rates and save them as CSV
    ################################################################################
    url = 'https://www.federalreserve.gov/econresdata/researchdata/feds200628.zip'
    #url ='https://www.federalreserve.gov/econresdata/researchdata/feds200628.xls'
    urllib.request.urlretrieve(url, down_dir + 'feds200628.zip')
    with zipfile.ZipFile(down_dir + 'feds200628.zip', 'r') as zip_ref:
        zip_ref.extractall(proc_dir)

    # Read XML data into a dataframe and the save as CSV
    tree = ET.parse(proc_dir + 'feds200628.xml')
    root = tree.getroot()
    dates = []
    values = []
    # Each child is a yieldcurve type - maturity. 1 year instaneous forward rate is the 60th child
    for idx, val in enumerate(root[1][60][1:]):
        dates.append(val.attrib['TIME_PERIOD'])
        values.append(val.attrib['OBS_VALUE'])
    data = pd.DataFrame(data={'date': pd.to_datetime(
        dates), 'treas1forward': pd.to_numeric(values)})
    data.loc[data['treas1forward'] < -9998, 'treas1forward'] = None
    # Loop over the rest of the maturties and merge together
    for snum in range(61, 89):
        sname = 'treas' + str(snum-59) + 'forward'
        dates = []
        values = []
        for idx, val in enumerate(root[1][snum][1:]):
            dates.append(val.attrib['TIME_PERIOD'])
            values.append(val.attrib['OBS_VALUE'])
        tmp = pd.DataFrame(data={'date': pd.to_datetime(
            dates), sname: pd.to_numeric(values)})
        tmp.loc[tmp[sname] < -9998, sname] = None
        data = data.merge(tmp, how='left', on='date')

    # Save
    data.to_csv(proc_dir + 'gss_forward_rates.csv', index=False)
    # Remove huge xml file
    os.remove(proc_dir + 'feds200628.xml')


def gss_tips():
    ################################################################################
    # Download GSW Tips data on forward rates and save them as CSV
    ################################################################################
    url = 'https://www.federalreserve.gov/econresdata/researchdata/feds200805.zip'
    urllib.request.urlretrieve(url, down_dir + 'feds200805.zip')
    with zipfile.ZipFile(down_dir + "feds200805.zip", 'r') as zip_ref:
        zip_ref.extractall(proc_dir)

    # Read XML data into a dataframe and the save as CSV
    tree = ET.parse(proc_dir + 'feds200805.xml')
    root = tree.getroot()
    dates = []
    values = []
    # Each child is a yieldcurve type - maturity. 1 year instaneous forward rate is the 60th child
    for idx, val in enumerate(root[1][39][1:]):
        dates.append(val.attrib['TIME_PERIOD'])
        values.append(val.attrib['OBS_VALUE'])
    data = pd.DataFrame(data={'date': pd.to_datetime(
        dates), 'tips2forward': pd.to_numeric(values)})
    data.loc[data['tips2forward'] < -9998, 'tips2forward'] = None
    # Loop over the rest of the maturties and merge together
    for snum in range(39, 39+18):
        sname = 'tips' + str(snum-36) + 'forward'
        dates = []
        values = []
        for idx, val in enumerate(root[1][snum][1:]):
            dates.append(val.attrib['TIME_PERIOD'])
            values.append(val.attrib['OBS_VALUE'])
        tmp = pd.DataFrame(data={'date': pd.to_datetime(
            dates), sname: pd.to_numeric(values)})
        tmp.loc[tmp[sname] < -9998, sname] = None
        data = data.merge(tmp, how='left', on='date')

    # Save
    data.to_csv(proc_dir + 'gsw_tips_rates.csv', index=False)
    # Remove huge xml file
    os.remove(proc_dir + 'feds200805.xml')


def blsjobsdays():
    ################################################################################
    # Grab BLS job's report dates
    ################################################################################
    url = 'https://www.bls.gov/bls/news-release/empsit.htm'
    raw = urllib.request.urlopen(url).read()

    html = str(raw.decode('utf-8', 'ignore'))
    bs = BeautifulSoup(html, "lxml")
    raw = bs.findAll(lambda tag: tag.name == 'li' and len(
        re.findall('empsit\_[0-9]{6,8}\.', str(tag))) > 0)

    T = []
    D = []
    M = []
    Y = []
    for rr in raw:
        t = ' '.join(rr.text.split()[0:2])
        m = datetime.datetime.strptime(t, '%B %Y').month
        y = datetime.datetime.strptime(t, '%B %Y').year
        if len(re.findall('empsit\_[0-9]{8}\.', str(rr))) > 0:
            dRaw = re.findall('empsit\_[0-9]{8}', str(rr))[0]
            dRaw = re.split('\_', dRaw)[1]
            d = datetime.datetime.strptime(dRaw, '%m%d%Y').date()
        elif len(re.findall('empsit\_[0-9]{6}\.', str(rr))) > 0:
            dRaw = re.findall('empsit\_[0-9]{6}', str(rr))[0]
            dRaw = re.split('\_', dRaw)[1]
            d = datetime.datetime.strptime(dRaw, '%m%d%y').date()
        else:
            print("OH NO! NOT WORKING " + rr)
        T.append(t)
        D.append(d)
        Y.append(y)
        M.append(m)

    pd.DataFrame({'DataMonth': M, 'DataYear': Y, 'ReleaseDate': D}).to_csv(
        proc_dir + 'employment_dates.csv', index=False, header=True)


def fomcdates():
    url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
    raw = urllib.request.urlopen(url).read()
    datesRaw = re.findall('monetarypolicy/fomcminutes[0-9]{8}.htm', str(raw))
    datesStr = list(set([re.findall('[0-9]{8}', dd)[0] for dd in datesRaw]))
    dates = [datetime.datetime.strptime(dd, '%Y%m%d') for dd in datesStr]

    # Code to get historical meetings
    # Get years first
    url = 'https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm'
    start = 1965
    end = min(dates).year

    for year in range(start, end):
        url = 'https://www.federalreserve.gov/monetarypolicy/fomchistorical' + \
            str(year) + '.htm'
        raw = urllib.request.urlopen(url).read()
        datesRaw = re.findall(
            'monetarypolicy/files/FOMC[0-9]{8}Agenda.pdf', str(raw))
        datesStr.extend(
            list(set([re.findall('[0-9]{8}', dd)[0] for dd in datesRaw])))

    dates = [datetime.datetime.strptime(dd, '%Y%m%d') for dd in datesStr]
    dates.sort()
    pd.DataFrame(dates, columns=['date']).to_csv(
        proc_dir + 'fomc_dates.csv', index=False, header=True)

def minutes_dates():
    ## Find when the historical data starts
    url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
    raw = urllib.request.urlopen(url).read()
    datesRaw = re.findall('monetarypolicy/fomcminutes[0-9]{8}.htm', str(raw))
    datesStr = list(set([re.findall('[0-9]{8}', dd)[0] for dd in datesRaw]))
    dates = [datetime.datetime.strptime(dd, '%Y%m%d') for dd in datesStr]
    lastYearHistorical = min(dates).year-1

    ## Grab the minute release dates
    datesDF = pd.DataFrame({'release':[],'minutes':[]})
    firstYear = 1993
    for year in range(firstYear,lastYearHistorical+1):
        print("Reading in data for the year " + str(year))
        if year <= lastYearHistorical:
            url = "https://www.federalreserve.gov/monetarypolicy/fomchistorical" + str(year) + ".htm"
        else:
            url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
        raw = urllib.request.urlopen(url).read()
        soup=BeautifulSoup(raw, "lxml")
        body = soup.find('body')
        if year <= lastYearHistorical:
            paragraphs = body.findAll('p')
        else:
            paragraphs = body.findAll('div')

        releases = [re.findall('\(Released[\s\,a-zA-Z0-9]*\)',str(pp)) for pp in paragraphs]
        releaseInd = [ii for ii in range(len(releases)) if len(releases[ii]) > 0]
        releaseDates = [re.findall('[A-Z][a-z]*[\s]*[0-9]*\,[\s]*[0-9]{4}',releases[ii][0])[0] for ii in releaseInd]
        minDates = [re.findall('[0-9]{8}',str(paragraphs[ii]))[0] for ii in releaseInd]

        minDatesD = [datetime.datetime.strptime(dd, '%Y%m%d') for dd in minDates]
        releaseDatesD = []
        for dd in releaseDates:
            try:
                d = datetime.datetime.strptime(dd, '%b %d, %Y')
            except:
                try:
                    d = datetime.datetime.strptime(dd, '%B %d, %Y')
                except:
                    pass
            releaseDatesD.append(d)

        datesDFyear = pd.DataFrame({'release':releaseDatesD, 'minutes':minDatesD})
        datesDF = datesDF.append(datesDFyear)
    #print(datesDF)

    # Hack to drop duplicate rows. Not sure whats going on here
    datesDF.drop_duplicates(inplace=True)
    datesDF.to_csv(proc_dir + 'minutes_dates.csv',index=False, header=True)

gss_forward()
gss_tips()
blsjobsdays()
fomcdates()
minutes_dates()