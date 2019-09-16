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
data_dir = proj_dir + 'data/'


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
    # TODO : Check that these dates are correct for reports that were delayed due
    # to government shutdowns
    # This function will eventually be superseeded by the BLS release calendar method
    ################################################################################
    print('Reading in BLS jobs day dates')
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
                         'releaseday': releasedays, 'releasehour': 8, 'releaseminute': 30,
                         'coveredyear': coveredyear, 'coveredperiod': coveredmonth, 'freq': 12, })


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
    print('Reading FOMC meeting dates')
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
        print('Reading FOMC meeting dates for ' + str(year))
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
                         'releaseday': releasedays, 'releasehour': 2, 'releaseminute': 30,
                         'coveredyear': releaseyears, 'coveredperiod': coveredday,  'freq': 365})


def parseReleaseDates(dd):
    try:
        d = dtdt.strptime(dd, '%b %d, %Y')
    except:
        try:
            d = dtdt.strptime(dd, '%B %d, %Y')
        except:
            pass
    return d


def minutes_dates():
    # Find when the historical data starts
    print('Reading in FOMC minutes dates')
    url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
    raw = urllib.request.urlopen(url).read()
    datesRaw = re.findall('monetarypolicy/fomcminutes[0-9]{8}.htm', str(raw))
    datesStr = list(set([re.findall('[0-9]{8}', dd)[0] for dd in datesRaw]))
    dates = [dtdt.strptime(dd, '%Y%m%d') for dd in datesStr]
    lastYearHistorical = min(dates).year-1

    # Grab the minute release dates
    firstYear = 1993
    outdfs = []
    for year in range(firstYear, lastYearHistorical + 2):
        print('Reading in FOMC minutes dates for ' + str(year))
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

        outdfs.append(pd.DataFrame({'release': 'FOMC minutes', 'releaseyear': releaseYears, 'releasemonth': releaseMonths,
                                    'releaseday': releaseDays, 'releasehour': 14, 'releaseminute': 00,
                                    'coveredyear': coveredYear, 'coveredperiod': coveredPeriod, 'freq': 365}))
    out = pd.concat(outdfs)
    out = out.sort_values(['releaseyear', 'releasemonth', 'releaseday'])
    # Hack to drop duplicate rows. Not sure whats going on here
    out.drop_duplicates(inplace=True)
    return out


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


def calcovered(raw):
    # Take in date string and return a tuple (freq, covered period, covered year)
    # Just a bunch of logic to handle BLS varried date formatting
    # returns (0,0, 2099) for annoying releases that cannot be easily parsed
    clean = re.split('strong>', str(raw.contents[0]))[2].strip()[4:-4].strip()
    # print(clean)
    if clean == '':
        return (0, 0, 2099)
    elif 'Bi-Annual' in clean:
        return (0, 0, 2099)
    elif 'Biennial' in clean:
        return (0, 0, 2099)
    elif 'Midyear' in clean:
        return (0, 0, 2099)
    elif 'Annual' in clean:
        if clean == 'Annual':
            return (0, 0, 2099)
        else:
            return (1, 1, int(re.split(' ', clean)[1]))
    elif len(clean.split(' ')) == 1:
        if len(clean.split('-')) == 1:
            return (1, 1, int(clean))
        else:
            return (0, 0, 2099)
    elif 'Quarter' in clean:
        year = int(re.split(' ', clean)[2])
        qnum = adj2num(re.split(' ', clean)[0])
        return (4, qnum, year)
    else:
        year = int(re.split(' ', clean)[1])
        mnum = dtdt.strptime(re.split(' ', clean)[0], '%B').month
        return (12, mnum, year)


def adj2num(raw):
    if raw == 'First':
        return 1
    elif raw == 'Second':
        return 2
    elif raw == 'Third':
        return 3
    elif raw == "Fourth":
        return 4
    else:
        print(raw + " cannot be parsed for a quarter number")
        return 0

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
    covered = []
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
                        descs.append(
                            re.split('strong>', str(col.contents[0]))[1][0:-2])
                        covered.append(calcovered(col))

    datesParsed = [dt.datetime.strptime(dd, '%A, %B %d, %Y') for dd in dates]
    times = [tt.replace('\xa0', '12:00 AM') for tt in times]
    timesParsed = [dt.datetime.strptime(dd, '%I:%M %p') for dd in times]
    releaseMON = [dd.month for dd in datesParsed]
    releaseDAY = [dd.day for dd in datesParsed]
    releaseMIN = [dd.minute for dd in timesParsed]
    releaseHOUR = [dd.hour for dd in timesParsed]
    coveredyear = [x[2] for x in covered]
    coveredperiod = [x[1] for x in covered]
    coveredfreq = [x[0] for x in covered]
    for rr in ['\(Annual\)', '\,[\s]+[0-9]{4}', '\(Quarterly\)', '\(Monthly\)',
               '\:[\s]+[0-9\-]+', '\(P\)', '\(R\)']:
        descs = [re.sub(rr, '', dd).strip() for dd in descs]

    out = pd.DataFrame({'release': descs, 'releaseyear': year, 'releasemonth': releaseMON, 'releaseday': releaseDAY,
                        'releasehour': releaseHOUR, 'releaseminute': releaseMIN, 'coveredyear': coveredyear, 'coveredperiod': coveredperiod, 'freq': coveredfreq})
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

    out = pd.DataFrame({'release': releaseName, 'releaseyear': year, 'releasemonth': releaseMON, 'releaseday': releaseDAY,
                        'releasehour': releaseHOUR, 'releaseminute': releaseMIN, 'coveredyear': 1950, 'coveredperiod': 1, 'freq': 0})
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
    return releaseDF


def getBLScalendars():
    out = parseBLScalendar(1999)
    for yy in range(1999, 2019):
        out = out.append(parseBLScalendar(yy))
    return out.drop_duplicates()


def main():
    #df = blsjobsdays()
    df = getBLScalendars()
    df = df.append(fomcdates(), ignore_index=True)
    df = df.append(minutes_dates(), ignore_index=True)
    df['releasedate'] = pd.Series([dtdt(df.loc[i, 'releaseyear'], df.loc[i, 'releasemonth'], df.loc[i,
                                                                                                    'releaseday'], df.loc[i, 'releasehour'], df.loc[i, 'releaseminute']) for i in range(len(df))])

    df = df[['releasedate', 'release', 'releaseyear', 'releasemonth',
             'releaseday', 'releasehour', 'releaseminute', 'coveredyear', 'coveredperiod', 'freq']]
    df = df.set_index(['releasedate', 'release'])
    df.sort_index(inplace=True)
    df.to_csv('/app/output/macro_release_dates.csv')
    return df


main()
