import logging
import logging.config
import configparser
from datetime import date, timedelta
import requests
from tqdm import *
from pathlib import Path
import sys
import holidays
from trading_calendars import get_calendar
import pandas as pd
import os
import warnings
from multiprocessing import Pool, cpu_count


class main:
    
    def read_config(self):
        self.day_wise_download_configs = {}
        self.fileLogger.info("Config read starts")
        
        if self.all_good:
            self.fileLogger.info("Reading dates from \"day wise download\" ")
            self.fileLogger.info("User wants to download data for {}".format(self.config['day_wise_download']['date']))
            self.day_wise_download_configs = self.config['day_wise_download']['date']

        self.fileLogger.info("Config read ends")

    def download_day_wise(self):
        self.fileLogger.info("Creating download sub-folders if does not exists")
        webpx_path = "{}/{}".format(self.config['download_folder']['location'],"WEBPXTICK_DT")
        p = Path(webpx_path)
        p.mkdir(parents=True,exist_ok=True)
        
        tc_path = "{}/{}".format(self.config['download_folder']['location'],"TC")
        p = Path(tc_path)
        p.mkdir(parents=True,exist_ok=True)
        self.fileLogger.info("Download day wise data starts")
        
        arr = self.day_wise_download_configs.split(",")
        pool = Pool(cpu_count())    
        for value in arr:
            self.fileLogger.info("Looking for mapping code {} ".format(value))
            tc_download_df = self.mapping_df_tc[self.mapping_df_tc['date']==int(value)]
            webpxtick_download_df = self.mapping_df_webpxtick_dt[self.mapping_df_webpxtick_dt['date']==int(value)]
            # Downloading for TC
            if tc_download_df.empty:
                self.fileLogger.info("[TC] Mapping not found for {} ,let's fetch it from the site".format(value))
                #Mapping not found, let's fetch it from the site
                #self._mapper()
                #self.download_day_wise()
            else:
                # for i in tc_download_df['id']:
                #     print(i)
                #TODO download all ids ... need a for loop here
                self.fileLogger.info("[TC] Mapping available for {} -> {}".format(value,tc_download_df['id'].iat[0]))
                self.download_data(tc_download_df['id'].iat[0], value, "tc", tc_path)
                self.logger.info("[TC] File for {} date can be found here -> {}/{}/TC_{}.txt".format(value,self.config['download_folder']['location'],"TC",tc_download_df['id'].iat[0]))
            # Downloading for WEBPXTICK_DT
            if webpxtick_download_df.empty:
                self.fileLogger.info("[WEBPXTICK_DT] Mapping not found for {}, let's fetch it from the site".format(value))
                #Mapping not found, let's fetch it from the site
                #self._mapper()
                #self.download_day_wise()
            else:
                self.fileLogger.info("[WEBPXTICK_DT] Mapping available for {} -> {}".format(value,webpxtick_download_df['id'].iat[0]))
                self.download_data(webpxtick_download_df['id'].iat[0], value, "webpx", webpx_path)
                self.logger.info("[WEBPXTICK_DT] File for {} date can be found here -> {}/{}/WEBPXTICK_DT_{}.zip".format(value,self.config['download_folder']['location'],"WEBPXTICK_DT",tc_download_df['id'].iat[0]))
        
        self.fileLogger.info("OK, Download day wise data ends")
    
    def download_data(self, key, value, switch, path):
        if switch == "tc":
            self.logger.info("[TC] Downloading data for date {}".format(value))
            url = "{}/{}/{}".format("https://links.sgx.com/1.0.0/derivatives-historical",key,"TC.txt")
            name = "{}/{}_TC.txt".format(path,key)
        elif switch == "webpx":
            self.logger.info("[WEBPXTICK_DT] Downloading data for date {}".format(value))
            url = "{}/{}/{}".format("https://links.sgx.com/1.0.0/derivatives-historical",key,"WEBPXTICK_DT.zip")
            name = "{}/{}_WEBPXTICK_DT.zip".format(path,key)

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
    
    # Helper functions 
    def _store_in_array(self, arr, i, switch):
        if switch is "tc":
            for a in arr:
                new_row = {'id':str(i), 'date':str(a)}
                self.mapping_df_tc = self.mapping_df_tc.append(new_row, ignore_index=True)
                self.fileLogger.info("[TC] New mapping added : {} -> {}".format(str(i),str(a)))
        elif switch is "webpx":
            for a in arr:
                new_row = {'id':str(i), 'date':str(a)}
                self.mapping_df_webpxtick_dt = self.mapping_df_webpxtick_dt.append(new_row, ignore_index=True)
                self.fileLogger.info("[WEBPXTICK_DT] New mapping added : {} -> {}".format(str(i),str(a)))
    
    def _config_basic_checks(self):
        self.fileLogger.info("Checks on config file starts")
        self.all_good = True

        # 1. Check for download folder
        if not ('download_folder' in self.config):
            self.fileLogger.error("Downlaod folder config missing, will end the program!")
            self.fileLogger.error("Please provide download config in the config.ini file")
            self.logger.error("Downlaod folder config missing, will end the program!")
            self.logger.error("Please provide download config in the config.ini file")
            self.all_good = False
            sys.exit()

        # 2. Check for day_wise_download
        if not ('day_wise_download' in self.config):
            self.fileLogger.warn("day_wise_download config missing")
            self.logger.warn("No single date provided")
        
        # 3. Check for date_range_download
        if not ('date_range_download' in self.config):
            self.fileLogger.warning("date_range_download config missing")
            self.logger.warn("No range for historic data provided")
        
        self.fileLogger.info("Checks on config file ends")
        
    def __init__(self):
        print("There are {} CPUs on this machine ".format(cpu_count()))
        # Defining internal variables
        self._INTERNAL_RANGE_START = 4660
        self._INTERNAL_RANGE_END = 4665
        
        warnings.simplefilter(action='ignore', category=FutureWarning)
        logging.config.fileConfig('logging.conf')
        self.logger = logging.getLogger('stdoutLog')
        self.fileLogger = logging.getLogger('fileLog')
        self.fileLogger.info("================================= SGX SCRIBE =================================")
        self.fileLogger.info("Created logging objects")
        self.logger.info("Program starts")
        self.logger.info("Initialising...")
        self.fileLogger.info("Creating directories, if does not exists")

        p = Path('logs')
        p.mkdir(exist_ok=True)
        p = Path('.internal')
        p.mkdir(exist_ok=True)
        self.fileLogger.info("OK, Creating directories done")

        self.fileLogger.info("Reading user input configs")
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.fileLogger.info("OK, Config file present")

        self.fileLogger.info("Mapping File Number to date") 
        self.mapping_file_tc = ".internal/mapping_tc.csv"
        self.mapping_file_webpxtick_dt = ".internal/mapping_webpxtick_dt.csv"
        column_names = ["id", "date"]

        self.fileLogger.info("Loading existing mapping")
        if(os.stat(self.mapping_file_tc).st_size == 0):
            self.mapping_df_tc = pd.DataFrame(columns = column_names)
        else:
            data = pd.read_csv(self.mapping_file_tc) 
            self.mapping_df_tc = pd.DataFrame(data, columns = column_names)
        if(os.stat(self.mapping_file_webpxtick_dt).st_size == 0):
            self.mapping_df_webpxtick_dt = pd.DataFrame(columns = column_names)
        else:
            data = pd.read_csv(self.mapping_file_webpxtick_dt) 
            self.mapping_df_webpxtick_dt = pd.DataFrame(data, columns = column_names)
        
        self.fileLogger.info("OK, Existing mapping loaded")
        self.fileLogger.info("Checking for new data")
        for i in reversed(range(self._INTERNAL_RANGE_START,self._INTERNAL_RANGE_END)):
            if i not in self.mapping_df_tc['id'].values:
                df = pd.read_csv('https://links.sgx.com/1.0.0/derivatives-historical/{}/TC.txt'.format(i), sep='\t')
                self._store_in_array(df['Business_Date'].unique(), i , "tc" )
            # else:
            #     self.fileLogger.info("[TC] Mapping already present for {}".format(i))

        self.fileLogger.info("Saving TC mapping back to disc")
        self.mapping_df_tc.to_csv(self.mapping_file_tc)
        self.fileLogger.info("OK, TC mapping completed")

        for i in reversed(range(self._INTERNAL_RANGE_START,self._INTERNAL_RANGE_END)):
            if i not in self.mapping_df_webpxtick_dt['id'].values:               
                df = pd.read_csv('https://links.sgx.com/1.0.0/derivatives-historical/{}/WEBPXTICK_DT.zip'.format(i), sep=',')
                self._store_in_array(df['Trade_Date'].unique(), i , "webpx")
            # else:
            #     self.fileLogger.info("[WEBPXTICK_DT] Mapping already present for {}".format(i))
        
        self.fileLogger.info("Saving WEBPXTICK_DT mapping back to disc")
        self.mapping_df_webpxtick_dt.to_csv(self.mapping_file_webpxtick_dt)
        self.fileLogger.info("OK, WEBPXTICK_DT mapping completed")
        self.logger.info("Program initiated, downloading starts...")
        
        self.fileLogger.info("Getting MIN and MAX mapping!")
        # self._MIN_TC = 
        # self._MAX_TC = 
        # self._MIN_WEBPX = 
        # self._MAX_WEBPX = 

sgx_scribe = main()
sgx_scribe._config_basic_checks()
sgx_scribe.read_config()
sgx_scribe.download_day_wise()
