from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime as dt
import urllib.request
import os
import wget
from itertools import product
from dateutil import parser
import numpy as np
indir = 'raw/BLS/'



def main():
    out   = pd.DataFrame()
    for yy in range(2000,2018):
        downloadReleases(str(yy))
    files = os.listdir(indir)
    for ff in files:
        releaseDF = parseHTML(ff)
        if releaseDF.shape[0] == 0:
            releaseDF = parseTXT(ff)
        out = out.append(releaseDF)
            
    out.to_csv('BLSreleases.csv',index=False)
    return(out)

def downloadReleases(year):
    url = 'https://www.bls.gov/schedule/' + year + '/home.htm'
    if not os.path.isfile(indir + 'BLSreleases' + year + '.htm'):
        downloadedFilename = wget.download(url)
        os.rename(downloadedFilename, indir + 'BLSreleases' + year + '.htm')


##----------------------------------------------------------------------------##
##-- FIRST, WORK WITH RECENT RELEASES WHICH ARE WRITTEN IN TABLE FORMAT ------##
##----------------------------------------------------------------------------##
def parseHTML(fname):
    html = open(indir + fname, 'rb').read()
    html = str(html.decode('utf-8', 'ignore'))
    bs = BeautifulSoup(html,"lxml")
    tables = bs.findAll(lambda tag: tag.name=='table')

    dates = []
    times = []
    descs = []
    for tt in range(len(tables)):
        table = tables[tt]
        rows    = table.findAll(lambda tag: tag.name=='tr')
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
    datesParsed = [dt.strptime(dd,'%A, %B %d, %Y') for dd in dates]
    times = [tt.replace('\xa0','12:00 AM') for tt in times]
    timesParsed = [dt.strptime(dd,'%I:%M %p') for dd in times]
    releaseMON  = [dd.month for dd in datesParsed]
    releaseDAY  = [dd.day for dd in datesParsed]
    releaseMIN = [dd.minute for dd in timesParsed]
    releaseHOUR = [dd.hour for dd in timesParsed]
    year = re.findall('[0-9]{4}',fname)[0]
    descs = [re.split('for',dd)[0].strip() for dd in descs]
    for rr in ['\(Annual\)','\,[\s]+[0-9]{4}','\(Quarterly\)','\(Monthly\)',
               '\:[\s]+[0-9\-]+','\(P\)','\(R\)']:
        descs = [re.sub(rr,'',dd).strip() for dd in descs]
    
    out = pd.DataFrame({'name':descs, 'month':releaseMON,'year':year,'day':releaseDAY,
                        'hour':releaseHOUR,'minute':releaseMIN})    
    return(out)
##----------------------------------------------------------------------------##
##-- FIRST, WORK WITH RECENT RELEASES WHICH ARE WRITTEN IN PLAIN TEXT --------##
##----------------------------------------------------------------------------##
def parseTXT(fname):
    year = re.findall('[0-9]{4}',fname)[0]
    html = open(indir + fname,'rb').read()
    html = str(html.decode('utf-8', 'ignore'))
    bs = BeautifulSoup(html,"lxml")
    table = bs.find('pre').contents[0]
    lines=re.split('[\r\n]+',table)
    firstLine = [ll for ll in range(len(lines)) if len(re.findall('Release Name',lines[ll]))>0][0]
    lines = lines[(firstLine+1):(len(table)-1)]
    lines = [re.sub('\t','    ',ll) for ll in lines]
    lines = [ll for ll in lines if len(ll.strip())>0]

    names = [ll[0:[mm.end() for mm in re.finditer('[0-9]{4}',ll)][0]] for ll in lines]
    releaseName   = [re.split(',',nn)[0].strip() for nn in names]
    releasePeriod = [re.split(',',nn)[1].strip() for nn in names]

    times = [ll[[mm.end() for mm in re.finditer('[0-9]{4}',ll)][0]:] for ll in lines]
    times = [re.sub('\(p\)','',tt) for tt in times]
    times = [re.sub('\(r\)','',tt) for tt in times]
    times = [re.sub('[\s]+',' ',tt) for tt in times]
    releaseMON,releaseDAY,releaseHOUR,releaseMIN = parseDates(times)
    
    out = pd.DataFrame({'name':releaseName, 'month':releaseMON,'year':year,'day':releaseDAY,
                        'hour':releaseHOUR,'minute':releaseMIN})
    return(out)


##----------------------------------------------------------------------------##
## Functions for cleaning
##----------------------------------------------------------------------------##

## parsing dates 
def parseDates(datesIn):
    datesIn = [re.sub('Sept\.', 'Sep.',dd) for dd in datesIn]
    datesIn = [re.sub('Sept', 'Sep',dd) for dd in datesIn]
    datesIn = [re.sub('\*', '',dd) for dd in datesIn]    
    datesIn = [re.sub('Sepember', 'September',dd) for dd in datesIn]
    dates = []
    fmts = ['%b. %d','1 %b. %d','%b %d','%B %d', '%B %d',
            '%b. %d, %Y','%b %d, %Y','%B %d, %Y', '%B %d, %Y']
    tfmt = ' %I:%M %p'
    for dd in datesIn:
        d = ''
        for ff in fmts:
            try:
                d = dt.strptime(dd.strip(), ff+tfmt)
            except:
                pass
        dates.append(d)
    m = [dd.month for dd in dates]
    d = [dd.day for dd in dates]
    H = [dd.hour for dd in dates]
    M = [dd.minute for dd in dates]    
    return(m,d,H,M)

## clean database some more
db = main()
db = db.loc[db.hour > 0]
        
if __name__ == "__main__":
    main()
