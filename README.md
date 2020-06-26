# sgx-scribe
Python application to get data from https://www.sgx.com/research-education/derivatives

`Python Version >= Python 3.7.5`
## Config options : 
A sample of *config.ini* : 
```
# File Download for one single day, can add multiple days with comma delimited 
[day_wise_download]
date = 20200623

# This is date range, it will download files between "START" and "END" dates
[date_range_download]
start = 20200620
end = 20200622
```
## Running the application : 

1. Do a git clone, change the config file as per your need and run `python main.py` . Download folder will be created with the files and logs can be checked in the logs folder.

## Current Limitation : 
Can only download historic data for last 2 years [until 20180120], because `trading date to download link` mapping does not follow a pattern and is done in the code in initialization stage. This is *one time process* and the mapping is update in the `.internal` folder.

To increase the range of mapping[historic], change the code line number `227` : `self._INTERNAL_RANGE_START` to whatever range is required
