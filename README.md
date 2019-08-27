# eventstudiesdata
Docker container to make data set with macroeconomic data announcements.

## Summary
This is a simple docker app to assemble a database with information on
the timings of various macroeconomic data releases and Federal Reserve
announcements. The only goal is spit out a file with as many as possible
macroeconomic events in a standarized format that one can use in 
any project. 

## Docker Commands

## Data Structure

| Name | releaseyear | releasemonth | releaseday | releasehour | releaseminute | freq | coveredyear | coveredperiod |
| ---  | ---  | ----- | --- | ---- | ------ | ---- | ----------- | ------------- |
| Series name | Year of release | Month of release | Day of release | Hour of release | Minute of Release | Frequency of data (monthly = 12, quarterly = 4, annual = 1) | Year that the data released pertains to | Within year period of data (either month or quarter) |
