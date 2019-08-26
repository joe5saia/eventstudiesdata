import urllib.request
import pandas as pd
import zipfile
import xml
import re
import datetime
import datetime as dt
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


def parseReleaseDates(dd):
    try:
        d = datetime.datetime.strptime(dd, '%b %d, %Y')
    except:
        try:
            d = datetime.datetime.strptime(dd, '%B %d, %Y')
        except:
            pass
    return(d)


def minutes_dates():
    # Find when the historical data starts
    url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
    raw = urllib.request.urlopen(url).read()
    datesRaw = re.findall('monetarypolicy/fomcminutes[0-9]{8}.htm', str(raw))
    datesStr = list(set([re.findall('[0-9]{8}', dd)[0] for dd in datesRaw]))
    dates = [datetime.datetime.strptime(dd, '%Y%m%d') for dd in datesStr]
    lastYearHistorical = min(dates).year-1

    # Grab the minute release dates
    datesDF = pd.DataFrame({'release': [], 'minutes': []})
    firstYear = 1993
    for year in range(firstYear, lastYearHistorical+1):
        print("Reading in data for the year " + str(year))
        if year <= lastYearHistorical:
            url = "https://www.federalreserve.gov/monetarypolicy/fomchistorical" + \
                str(year) + ".htm"
        else:
            url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
        raw = urllib.request.urlopen(url).read()
        soup = BeautifulSoup(raw, "lxml")
        body = soup.find('body')
        if year <= lastYearHistorical:
            paragraphs = body.findAll('p')
        else:
            paragraphs = body.findAll('div')

        releases = [re.findall('\(Released[\s\,a-zA-Z0-9]*\)', str(pp))
                    for pp in paragraphs]
        releaseInd = [ii for ii in range(
            len(releases)) if len(releases[ii]) > 0]
        releaseDates = [re.findall(
            '[A-Z][a-z]*[\s]*[0-9]*\,[\s]*[0-9]{4}', releases[ii][0])[0] for ii in releaseInd]
        minDates = [re.findall('[0-9]{8}', str(paragraphs[ii]))[0]
                    for ii in releaseInd]

        minDatesD = [datetime.datetime.strptime(
            dd, '%Y%m%d') for dd in minDates]
        releaseDatesD = [parseReleaseDates(dd) for dd in releaseDates]
        releaseDays = [x.day for x in releaseDatesD]
        releaseMonths = [x.month for x in releaseDatesD]
        releaseYears = [x.year for x in releaseDatesD]

        out = pd.DataFrame({'name': 'FOMC minutes', 'month': releaseMonths,
                            'year': year, 'day': releaseDays, 'hour': 2, 'minute': 30})

        datesDFyear = pd.DataFrame(
            {'release': releaseDatesD, 'minutes': minDatesD})

        datesDF = datesDF.append(datesDFyear)
    # print(datesDF)

    # Hack to drop duplicate rows. Not sure whats going on here
    datesDF.drop_duplicates(inplace=True)

    # datesDF.to_csv(proc_dir + 'minutes_dates.csv', index=False, header=True)


def parseDates(datesIn):
    # Helper function to clean some BLS dates
    datesIn = [re.sub('Sept\.', 'Sep.', dd) for dd in datesIn]
    datesIn = [re.sub('Sept', 'Sep', dd) for dd in datesIn]
    datesIn = [re.sub('\*', '', dd) for dd in datesIn]
    datesIn = [re.sub('Sepember', 'September', dd) for dd in datesIn]
    dates = []
    fmts = ['%b. %d', '1 %b. %d', '%b %d', '%B %d', '%B %d',
            '%b. %d, %Y', '%b %d, %Y', '%B %d, %Y', '%B %d, %Y']
    tfmt = ' %I:%M %p'
    for dd in datesIn:
        d = ''
        for ff in fmts:
            try:
                d = dt.datetime.strptime(dd.strip(), ff+tfmt)
            except:
                pass
        dates.append(d)
    m = [dd.month for dd in dates]
    d = [dd.day for dd in dates]
    H = [dd.hour for dd in dates]
    M = [dd.minute for dd in dates]
    return (m, d, H, M)


##----------------------------------------------------------------------------##
##-- FIRST, WORK WITH RECENT RELEASES WHICH ARE WRITTEN IN TABLE FORMAT ------##
##----------------------------------------------------------------------------##
def parseHTML(bs, year):
    if year < 2018:
        tables = bs.findAll(lambda tag: tag.name == 'table')
    else:
        # Website format changes in 2018
        tables = bs.findAll(lambda tag: tag.name == 'table')[1:]
    dates = []
    times = []
    descs = []
    for table in tables:
        rows = table.findAll(lambda tag: tag.name == 'tr')
        for row in rows:
            cols = row.findAll(lambda tag: tag.name == 'td')
            if len(cols) > 0:
                for col in cols:
                    if col['class'][0] == 'date-cell':
                        dates.append(col.text)
                    elif col['class'][0] == 'time-cell':
                        times.append(col.text)
                    elif col['class'][0] == 'desc-cell':
                        descs.append(col.text)
    datesParsed = [dt.datetime.strptime(dd, '%A, %B %d, %Y') for dd in dates]
    times = [tt.replace('\xa0', '12:00 AM') for tt in times]
    timesParsed = [dt.datetime.strptime(dd, '%I:%M %p') for dd in times]
    releaseMON = [dd.month for dd in datesParsed]
    releaseDAY = [dd.day for dd in datesParsed]
    releaseMIN = [dd.minute for dd in timesParsed]
    releaseHOUR = [dd.hour for dd in timesParsed]
    descs = [re.split('for', dd)[0].strip() for dd in descs]
    for rr in ['\(Annual\)', '\,[\s]+[0-9]{4}', '\(Quarterly\)', '\(Monthly\)',
               '\:[\s]+[0-9\-]+', '\(P\)', '\(R\)']:
        descs = [re.sub(rr, '', dd).strip() for dd in descs]

    out = pd.DataFrame({'name': descs, 'month': releaseMON, 'year': year, 'day': releaseDAY,
                        'hour': releaseHOUR, 'minute': releaseMIN})
    return (out)

##----------------------------------------------------------------------------##
##-- FIRST, WORK WITH RECENT RELEASES WHICH ARE WRITTEN IN PLAIN TEXT --------##
##----------------------------------------------------------------------------##


def parseTXT(bs, year):
    table = bs.find('pre').contents[0]
    lines = re.split('[\r\n]+', table)
    firstLine = [ll for ll in range(len(lines)) if len(
        re.findall('Release Name', lines[ll])) > 0][0]
    lines = lines[(firstLine+1): (len(table)-1)]
    lines = [re.sub('\t', '    ', ll) for ll in lines]
    lines = [ll for ll in lines if len(ll.strip()) > 0]

    names = [ll[0:[mm.end() for mm in re.finditer(
        '[0-9]{4}', ll)][0]] for ll in lines]
    releaseName = [re.split(',', nn)[0].strip() for nn in names]
    releasePeriod = [re.split(',', nn)[1].strip() for nn in names]

    times = [ll[[mm.end() for mm in re.finditer(
        '[0-9]{4}', ll)][0]:] for ll in lines]
    times = [re.sub('\(p\)', '', tt) for tt in times]
    times = [re.sub('\(r\)', '', tt) for tt in times]
    times = [re.sub('[\s]+', ' ', tt) for tt in times]
    releaseMON, releaseDAY, releaseHOUR, releaseMIN = parseDates(times)

    out = pd.DataFrame({'name': releaseName, 'month': releaseMON, 'year': year, 'day': releaseDAY,
                        'hour': releaseHOUR, 'minute': releaseMIN})
    return(out)


def parseBLScalendar(year):
    print(f"Downloading and parsing {year} BLS calendar")
    url = 'https://www.bls.gov/schedule/' + str(year) + '/home.htm'
    raw = urllib.request.urlopen(url)
    html = str(raw.read().decode('utf-8', 'ignore'))
    bs = BeautifulSoup(html, "lxml")
    releaseDF = parseHTML(bs, year)
    if releaseDF.shape[0] == 0:
        releaseDF = parseTXT(bs, year)
    return(releaseDF)


def getBLScalendars():
    out = parseBLScalendar(1999)
    for yy in range(2017, 2019):
        out.append(parseBLScalendar(yy))
    return(out)


# a = getBLScalendars()
# print(a)

a = minutes_dates()
print(a.head())
