##----------------------------------------------------------------------------##
## This file converts Bloomberg event datsets into a standardized format      ##
##                                                                            ##
## Input:  Excel files in data/bloomberg                                      ##
## Output: A csv in /data/processed/                                          ##
##----------------------------------------------------------------------------##
import pandas as pd
import re
from datetime import datetime as dt
import datetime
indir =  '../data/bloomberg/'
outdir =  '../data/processed/'


##----------------------------------------------------------------------------##
## Functions to get us started                                                ##
##----------------------------------------------------------------------------##
## open and append several Bloomberg Excel files 
def openBBfiles(DIR,fnames,additional_vars=[]):
    DF = pd.DataFrame()
    for ff in fnames:
        df = pd.read_excel(DIR + ff + '.xlsx',dtype={'Date Time':str})
        df = df.loc[:,['Date', 'Time','Event','Period']+additional_vars]
        df.loc[:,'source'] = ff
        DF = DF.append(df,sort=True)
    
    DF = DF.reset_index(drop=True)
    return(DF)


## Add dates for when the release occurred
def parseBBdates(DF, datevar='Date',timevar='Time'):
    ## Bloomberg does this fun thing where it uses 2-digit years, except
    ## in 2000, where they use 000. 
    DF.loc[:,datevar] = [re.sub('000','00',dd) if type(dd) is str
                         else dd for dd in DF.loc[:,datevar]]
    DF.loc[:,'date']  = [dt.strptime(dd,'%m/%d/%y') if type(dd) is str
                         else dd for dd in list(DF.loc[:,datevar])]

    ## widen the release information 
    DF.loc[:,'releaseyear'] = [dd.date().year for dd in DF.date]
    DF.loc[:,'releasemonth'] = [dd.date().month for dd in DF.date]
    DF.loc[:,'releaseday'] = [dd.date().day for dd in DF.date]
    DF.loc[:,'releasehour'] = [dd.hour for dd in DF.loc[:,timevar]]
    DF.loc[:,'releaseminute'] = [dd.minute for dd in DF.loc[:,timevar]]
    DF = DF.drop([datevar,timevar,'date'],axis=1)
    return(DF)

## Elongates a dataframe to have multiple observations if a given column
## has more than one entry. 
def expandDF(DF, var,sep='\,|\;'):
    splits = [re.split(sep,str(dd)) for dd in list(DF[var])]
    for ss in range(len(splits)): 
        if len(splits[ss]) > 1:
            for ii in range(len(splits[ss])):
                new = DF.copy().loc[ss,]
                new.loc[var] = splits[ss][ii]
                DF = DF.append(new)
                DF = DF.reset_index(drop=True)
    splits = [re.split(sep,str(dd)) for dd in list(DF[var])]
    DF     = DF.loc[[len(ss)==1 for ss in splits],:]
    return(DF)
##----------------------------------------------------------------------------##
## First work with the macroeconomic news releases                            ##
##----------------------------------------------------------------------------##

## Read in excel files
macro = openBBfiles(indir,['labor','gdp','prices'])

## Only keep indicators we care about (Note: several indicators may be 
## released at once--this step is mostly useful if you are calculating 
## surprises in the release variables.
macro = macro.rename({'Event':'Name'},axis=1)
events2keep = ["Unemployment Rate", "Change in Nonfarm Payrolls", 
               "CPI MoM", "CPI Ex Food and Energy MoM", 
               "PCE Core Deflator MoM","PCE Deflator MoM",
               "GDP Annualized QoQ"]
macro = macro.loc[macro.Name.isin(events2keep),]


## Add dates for when the release occurred
macro = parseBBdates(macro)

## add dates that the release was about: for monthly
macro.loc[:,'monthly'] = macro.source.isin(['labor','prices'])
refmo = [dt.strptime('01'+mm+'1992','%d%b%Y').date().month for mm in
         list(macro.loc[macro.monthly,'Period'])]
refym = [dd for dd in macro.loc[macro.monthly,'releaseyear']]
refym = [yy - 1 if mm == 12 else yy for yy,mm in zip(refym,refmo)]
macro.loc[macro.monthly,'coveredyear'] = refym
macro.loc[macro.monthly,'coveredperiod'] = refmo
macro.loc[macro.monthly,'freq'] = 12

## add dates that the period was about: for quarterly
refq = [int(dd[0]) for dd in macro.loc[~macro.monthly,'Period']]
refyq = [dd for dd in macro.loc[~macro.monthly,'releaseyear']]
refyq = [qq - 1 if qq == 4 else qq for qq,mm in zip(refyq,refmo)]
macro.loc[~macro.monthly,'coveredyear'] = refyq
macro.loc[~macro.monthly,'coveredperiod'] = refq
macro.loc[~macro.monthly,'freq'] = 4

## Give events nice short names 
ugly2nice = {"Unemployment Rate"           : "U"   ,
             "Change in Nonfarm Payrolls"  : "JOBS", 
             "CPI MoM"                     : "CPI" , 
             "CPI Ex Food and Energy MoM"  : "CPIX", 
             "PCE Core Deflator MoM"       : "PCE" , 
             "PCE Deflator MoM"            : "PCEX", 
             "GDP Annualized QoQ"          : "GDP" }
for uu in ugly2nice.keys():
    macro.loc[macro.Name==uu,'Name'] = ugly2nice[uu]

## Drop a few things and save
macro = macro.drop(['Period', 'source', 'monthly'],axis=1)
macro = macro.sort_values(by=['releaseyear','releasemonth',
                              'releaseday','Name'])
macro.to_csv(outdir + 'bb_macro.csv',index=False)


##----------------------------------------------------------------------------##
## Next we will read in and parse Fed events                                  ##
##                                                                            ##
## WORK IN PROGRESS ! WORK IN PROGRESS ! WORK IN PROGRESS ! WORK IN PROGRESS  ##
##                                                                            ##
##----------------------------------------------------------------------------##

## Read in Fed files
fed = openBBfiles(indir,['bloomberg_fomc_1995_2006',
                         'bloomberg_fomc_2007_2013',
                         'bloomberg_fomc_2014_2019'],['Ticker','Category'])
fed = fed.rename({'Event':'Name'},axis=1)
## add release information: first drop anything with no time
fed = fed.loc[fed.Time.notna(),]
fed = parseBBdates(fed)

## Split these into
##   1. Statement releases 
##   2. Minutes releases
##   3. Speeches/testimony by Fed policymakers


## 1. Statement releases: Note that before there was a lower bound and
##    upper bound (each of which now have their own Bloomberg ticker)
##    Bloomberg used what's now the upper bound ticker, FDTR. So we
##    will only keep this since it's consistent over time and it only matters
##    if we were, again, wanting to look at the expected values for these.
statements = fed.loc[fed.Ticker=='FDTR Index',:]
statements = statements.drop(['Period','Ticker','source','Category'],axis=1)
statements.loc[:,'Name'] = 'FOMC meeting'
statements.to_csv(outdir + 'bb_FOMCstatements.csv',index=False)

## 2. Minutes releases
minutes    = fed.loc[fed.Ticker=='FEDMMINU Index',:]
minutes    = minutes.drop(['Period','Ticker','source','Category'],axis=1)
minutes.loc[:,'Name']    = 'FOMC minutes'
minutes.to_csv(outdir + 'bb_FOMCminutes.csv',index=False)

## 3. Speeches/testimony by Fed poliymakers
##
##    We are going to find speeches by looking for names of know FOMC
##    members (and ensuring they were working at the Fed when we
##    see them giving a speech---thanks, Geithner). I'm doing this because
##    the way that these event names are formatted isn't consistent within
##    Bloomberg's data. Here are a few ways that speeches are mentioned: 
## 
##      1. Typical is to have "Fed's NAME Speaks at ..."
##      2. Sometimes people are on panels, so "Fed's XX at panel..."
##      3. Chairs don't seem to always get "Fed" with their name.

## bring in the list of FOMC names and their dates in office     
fomc = pd.read_csv('../data/FOMCjobs.csv')
fomc.loc[:,'start'] = [dt.strptime(dd,'%Y-%m-%d').date() for dd in fomc.start]
fomc.loc[:,'end']   = [dt.strptime(dd,'%Y-%m-%d').date() for dd in fomc.end]

## Next: clean out some places/events that have names that are like FOMC
##        member names. before "merging" with fomc dataset
fomc_wannabes =  {'Jackson[\s]*Hole'  : 'JHOLE',
                  'Robert Morris'     : 'RMORE', 
                  'Johns Hopkins'     : 'JHOP',
                  'Johnson City'      : 'JCITY',
                  'Duke University'   : 'DU',
                  'Stern School'      : 'NYUBIZ',
                  'George Washington' : 'GDUB',
                  'Volcker Rule'      : 'VRULE',
                  'Morris County'     : 'MCOUNTY'}
for old in fomc_wannabes.keys():
    fed.Name = [re.sub(old,fomc_wannabes[old],dd) for dd in fed.Name]

## Now look for the FOMC names in each description of each event
fomcNameRE = '[\s^]+' + '[\s$]+|[\s^]+'.join(list(fomc.lastNames)).lower()+'[\s$]+'
potentials = [','.join(re.findall(fomcNameRE,str(ss.lower()))) for ss in fed.Name]
fed.loc[:,'potentials'] = potentials

## If there were multiple matches, we'll say that each person gave a speech
fed = expandDF(fed, 'potentials',sep='\,')
fed.loc[:,'potentials'] = [pp.strip().upper() for pp in fed.potentials]

## Merge with FOMC tenure information and only keep speeches
## by people that were actively in the FOMC
speeches = fed.merge(fomc,left_on = 'potentials',right_on='lastNames',how='inner')
speeches.loc[:,'date'] = [datetime.date(yy,mm,dd) for yy,mm,dd in
                      zip(speeches.releaseyear,speeches.releasemonth,speeches.releaseday)]
speeches = speeches.loc[(speeches.date >= speeches.start) &
                        (speeches.date <= speeches.end)   &
                        (speeches.potentials != ''), :]

## Note: The "tenure" variables are mostly annual, though we do make adjustments for some
## people that made a change in the middle of the year.
## what remains here looks good. 
#speeches.loc[:,'fedin'] = [len(re.findall('[fF]ed',dd))>0 for dd in speeches.Name]
#speeches.loc[~speeches.fedin,['Name','date']]

## Drop 'canceled' events, and senate votes
speeches = speeches.loc[[len(re.findall('[\s^]+[Cc]anceled[\s$]+',dd))==0 for dd in speeches.Name],:]
speeches = speeches.loc[[len(re.findall('[\s^]+[Vv]ote[s]*[\s$]+',dd))==0 for dd in speeches.Name],:]

## experimental: try dropping speeches that don't have a coded bloomberg category
speeches = speeches.loc[speeches.Category.notna(),:]

## Duplicates: First, we can drop duplicate person/date/time. But we need to look
## into multiple speeches on one day at different times--indicates that timestamp
## may be wrong.
speeches = speeches.loc[~speeches.duplicated(subset=['date','releasehour','releaseminute','lastNames'],keep='first'),:]
twoSpeechesOneDay = speeches.loc[speeches.duplicated(subset=['date','lastNames'],keep=False),:]

twoSpeechesOneDay.groupby(['title','lastNames'])['lastNames'].count()
twoSpeechesOneDay[['Name','date','releasehour','releaseminute','title','lastNames']].to_excel('problems.xlsx')
