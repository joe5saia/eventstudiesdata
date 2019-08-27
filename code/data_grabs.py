import urllib.request
import pandas as pd
import zipfile
import xml
import re
import datetime as dt
from datetime import datetime as dtdt
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


def parseESRdate(raw):
    ################################################################################
    # some dates are in mmddyy format and others are in mmddyyyy format
    # Return a date object using the correct formating
    ################################################################################
    if len(raw) == 8:
        return dtdt.strptime(raw, '%m%d%Y')
    elif len(raw) == 6:
        return dtdt.strptime(raw, '%m%d%y')
    else:
        print(f'{raw} was an invalid date. Replacing with 19000101')
        return dtdt.strptime('01011900', '%m%d%Y')


def parseESRcovereddate(rawstring):
    ################################################################################
    # Cleans the <a \a> field from release links on the BLS website and turns the
    # the date into a datetime object
    ################################################################################
    title = re.findall(
        '[A-Z][a-z]{1,} [0-9]{4} Employment', str(rawstring.a).replace(u'\xa0', ' '))[0]
    d = ' '.join(title.split()[0:2])
    return dtdt.strptime(d, '%B %Y')


def blsjobsdays():
    ################################################################################
    # Grab BLS job's report dates
    # Description: The BLS issues a press announcement for each employment situation
    # report release. 'https://www.bls.gov/bls/news-release/empsit.htm' is the page
    # that links to each of these reports. We can get the dates by parsing the relevant
    # links.
    #  TODO : Check that these dates are correct for reports that were delayed due
    # to government shutdowns
    # This function will eventually be superseeded by the BLS release calendar method
    ################################################################################
    url = 'https://www.bls.gov/bls/news-release/empsit.htm'
    raw = urllib.request.urlopen(url).read()

    html = str(raw.decode('utf-8', 'ignore'))
    bs = BeautifulSoup(html, "lxml")
    raw = bs.findAll(lambda tag: tag.name == 'li' and len(
        re.findall('empsit\_[0-9]{6,8}\.', str(tag))) > 0)

    releasedays = [parseESRdate(re.findall(
        '[0-9]{6,8}', str(x.a))[0]).day for x in raw]
    releasemonths = [parseESRdate(re.findall(
        '[0-9]{6,8}', str(x.a))[0]).month for x in raw]
    releaseyears = [parseESRdate(re.findall(
        '[0-9]{6,8}', str(x.a))[0]).year for x in raw]

    coveredyear = [parseESRcovereddate(x).year for x in raw]
    coveredmonth = [parseESRcovereddate(x).month for x in raw]

    return pd.DataFrame({'release': 'Employment Situation Report', 'releaseyear': releaseyears, 'releasemonth': releasemonths,
                         'releaseday': releasedays, 'releasehour': 8, 'releaseminute': 30, 'freq': 12,
                         'coveredyear': coveredyear, 'coveredperiod': coveredmonth})


def fomcdates():
    ################################################################################
    # Grabs the FOMC meeting dates
    # Description: The FOMC releases a statment after their meetings (historically
    # only when a rate decision was made). The FOMC posts its meeting materials on
    # its website. This script parses the webpage for the link to the minutes and
    # saves them.
    #  TODO : We should honestly porobably be using the meeting dates displayed on
    # the webpage and not the minute links but this seems to work.
    # Also need to check to make sure statements are always released at 2:30
    ################################################################################
    # Grab the non-historical meetings first
    url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
    raw = urllib.request.urlopen(url).read()
    datesRaw = re.findall('monetarypolicy/fomcminutes[0-9]{8}.htm', str(raw))
    datesStr = [re.findall('[0-9]{8}', dd)[0] for dd in datesRaw]
    dates = [dtdt.strptime(dd, '%Y%m%d') for dd in datesStr]

    # Code to get historical meetings
    # Get years first
    url = 'https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm'
    start = 1965
    end = min(dates).year

    # The historical data has a seperate page for each year. Loop through them
    for year in range(start, end):
        url = 'https://www.federalreserve.gov/monetarypolicy/fomchistorical' + \
            str(year) + '.htm'
        raw = urllib.request.urlopen(url).read()
        datesRaw = re.findall(
            'monetarypolicy/files/FOMC[0-9]{8}Agenda.pdf', str(raw))
        datesStr.extend([re.findall('[0-9]{8}', dd)[0] for dd in datesRaw])

    dates = [dtdt.strptime(dd, '%Y%m%d') for dd in datesStr]
    dates.sort()

    releasedays = [x.day for x in dates]
    releasemonths = [x.month for x in dates]
    releaseyears = [x.year for x in dates]

    coveredday = [(x - dtdt(x.year, 1, 1)).days + 1 for x in dates]

    return pd.DataFrame({'release': 'FOMC meeting', 'releaseyear': releaseyears, 'releasemonth': releasemonths,
                         'releaseday': releasedays, 'releasehour': 2, 'releaseminute': 30, 'freq': 365,
                         'coveredyear': releaseyears, 'coveredperiod': coveredday})


def parseReleaseDates(dd):
    try:
        d = dtdt.strptime(dd, '%b %d, %Y')
    except:
        try:
            d = dtdt.strptime(dd, '%B %d, %Y')
        except:
            pass
    return(d)


def minutes_dates():
    # Find when the historical data starts
    url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
    raw = urllib.request.urlopen(url).read()
    datesRaw = re.findall('monetarypolicy/fomcminutes[0-9]{8}.htm', str(raw))
    datesStr = list(set([re.findall('[0-9]{8}', dd)[0] for dd in datesRaw]))
    dates = [dtdt.strptime(dd, '%Y%m%d') for dd in datesStr]
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

        minDatesD = [dt.datetime.strptime(dd, '%Y%m%d') for dd in minDates]
        releaseDatesD = [parseReleaseDates(dd) for dd in releaseDates]
        releaseDays = [x.day for x in releaseDatesD]
        releaseMonths = [x.month for x in releaseDatesD]
        releaseYears = [x.year for x in releaseDatesD]
        coveredYear = [x.year for x in minDatesD]
        coveredPeriod = [(x - dt.datetime(x.year, 1, 1)
                          ).days + 1 for x in minDatesD]

        out = pd.DataFrame({'release': 'FOMC minutes', 'releaseyear': year, 'releasemonth': releaseMonths,
                            'releaseday': releaseDays, 'releasehour': 14, 'releaseminute': 00, 'freq': 365,
                            'coveredyear': coveredYear, 'coveredperiod': coveredPeriod})

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
