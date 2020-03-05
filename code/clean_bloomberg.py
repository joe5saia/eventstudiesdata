##----------------------------------------------------------------------------##
## This file converts Bloomberg event datsets into a standardized format      ##
##                                                                            ##
## Input:  Excel files in data/bloomberg                                      ##
## Output: A csv in /data/processed/                                          ##
##----------------------------------------------------------------------------##
import pandas as pd
import re
from datetime import datetime as dt 
indir =  '../data/bloomberg/'
outdir =  '../data/processed/'


##----------------------------------------------------------------------------##
## First work with the macroeconomic news releases                            ##
##----------------------------------------------------------------------------##

## Read in excel files
macro = pd.DataFrame()
for ff in ['labor','gdp','prices']:
    df = pd.read_excel(indir + ff + '.xlsx',dtype={'Date Time':str})
    df = df.loc[:,['Date Time','Event','Period']]
    df.loc[:,'source'] = ff
    macro = macro.append(df,sort=True)

## Quick renaming 
macro = macro.rename({'Date Time':'Date','Event':'Name'},axis=1)
macro = macro.reset_index(drop=True)


## Only keep indicators we care about (Note: several indicators may be released
## at once--this step is mostly useful if you are calculating surprises in the
## release variables. 
events2keep = ["Unemployment Rate", "Change in Nonfarm Payrolls", 
               "CPI MoM", "CPI Ex Food and Energy MoM", 
               "PCE Core Deflator MoM","PCE Deflator MoM","GDP Annualized QoQ"]
macro = macro.loc[macro.Name.isin(events2keep),]


## Add dates for when the release occurred
macro.loc[:,'Date'] = [re.sub('000','00',str(dd)) for dd in macro.Date]
macro.loc[:,'datetime'] = [dt.strptime(dd,'%m/%d/%y %H:%M') for dd in list(macro.Date)]
macro = macro.drop('Date',axis=1)

## add dates that the release was about: for monthly
macro.loc[:,'monthly'] = macro.source.isin(['labor','prices'])
refmo = [dt.strptime('01'+mm+'1992','%d%b%Y').date().month for mm in
         list(macro.loc[macro.monthly,'Period'])]
refym = [dd.year for dd in macro.loc[macro.monthly,'datetime']]
refym = [yy - 1 if mm == 12 else yy for yy,mm in zip(refym,refmo)]
macro.loc[macro.monthly,'coveredyear'] = refym
macro.loc[macro.monthly,'coveredperiod'] = refmo
macro.loc[macro.monthly,'freq'] = 12

## add dates that the period was about: for quarterly
refq = [int(dd[0]) for dd in macro.loc[~macro.monthly,'Period']]
refyq = [dd.year for dd in macro.loc[~macro.monthly,'datetime']]
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

## widen the release information 
macro.loc[:,'releaseyear'] = [dd.date().year for dd in macro.datetime]
macro.loc[:,'releasemonth'] = [dd.date().month for dd in macro.datetime]
macro.loc[:,'releaseday'] = [dd.date().day for dd in macro.datetime]
macro.loc[:,'releasehour'] = [dd.time().hour for dd in macro.datetime]
macro.loc[:,'releaseminute'] = [dd.time().minute for dd in macro.datetime]

## Drop a few things and save
macro = macro.drop(['Period', 'source', 'monthly','datetime'],axis=1)
macro.to_csv(outdir + 'bb_macro.csv',index=False)
