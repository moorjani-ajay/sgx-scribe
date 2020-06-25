import logging
import logging.config
import configparser
from datetime import datetime, timedelta
import requests
from pathlib import Path
import sys
import pandas as pd
import os
import warnings
from multiprocessing import Pool, cpu_count


class main:
    
    def read_config(self):
        self.day_wise_download_configs = {}
        
        if self.day_wise_flag:
            self.fileLogger.info("Config read starts")
            self.fileLogger.info("Reading dates from \"day wise download\" ")
            self.fileLogger.info("User wants to download data for {}".format(self.config['day_wise_download']['date']))
            self.day_wise_download_configs = self.config['day_wise_download']['date']

        if self.range_flag:
            self.fileLogger.info("Reading dates from \"date range download\" ")
            self.fileLogger.info("User wants to download data from {} -> {}".format(self.config['date_range_download']['start'], self.config['date_range_download']['end']))
            self.historic_start = self.config['date_range_download']['start']
            self.historic_end = self.config['date_range_download']['end']

        self.fileLogger.info("Config read ends")

    def download_day_wise(self, historicflag):
        if historicflag:
            arr = self.historic_days
        else:
            arr = self.day_wise_download_configs.split(",")
        self.fileLogger.info("Creating download sub-folders if does not exists")

        p = Path("downloads/WEBPXTICK_DT")
        p.mkdir(parents=True,exist_ok=True)
        p = Path("downloads/TC")
        p.mkdir(parents=True,exist_ok=True)
        p = Path("downloads/TickData_structure")
        p.mkdir(parents=True,exist_ok=True)
        p = Path("downloads/TC_structure")
        p.mkdir(parents=True,exist_ok=True)

        self.fileLogger.info("Download day wise data starts")
        
        files = []
        for r, d, f in os.walk("downloads"):
            for file in f:
                    files.append(os.path.join(r, file))

        urls = []
        for value in arr:
            self.fileLogger.info("Looking for mapping code {} ".format(value))
            tc_download_df = self.mapping_df_tc[self.mapping_df_tc['date']==int(value)]
            webpxtick_download_df = self.mapping_df_webpxtick_dt[self.mapping_df_webpxtick_dt['date']==int(value)]
            # Check if files are already available
            # Logic for TC
            if tc_download_df.empty: 
                 # and historicflag is False:
                self.fileLogger.info("[TC] Mapping not found for {} ,let's fetch it from the site [FEATURE] ".format(value))
            else:
                for i in tc_download_df['id']:
                    if "downloads/TC/{}_TC.txt".format(i) not in files:
                        urls.append("{}/{}/{}".format("https://links.sgx.com/1.0.0/derivatives-historical",i,"TC.txt"))
                        urls.append("{}/{}/{}".format("https://links.sgx.com/1.0.0/derivatives-historical",i,"TC_structure.dat"))
                        
                        
                    else:
                        self.fileLogger.info("[TC] File already present for {} ".format(i))
            
            # Logic for WEBPXTICK_DT
            if webpxtick_download_df.empty:
                self.fileLogger.info("[WEBPXTICK_DT] Mapping not found for {} ,let's fetch it from the site".format(value))
            else:
                for i in webpxtick_download_df['id']:
                    if "downloads/WEBPXTICK_DT/{}_WEBPXTICK_DT.zip".format(i) not in files:
                        urls.append("{}/{}/{}".format("https://links.sgx.com/1.0.0/derivatives-historical",i,"WEBPXTICK_DT.zip"))
                        urls.append("{}/{}/{}".format("https://links.sgx.com/1.0.0/derivatives-historical",i,"TickData_structure.dat"))
                    else:
                        self.fileLogger.info("[WEBPXTICK_DT] File already present for {} ".format(i))
    
        pool = Pool(cpu_count())
        self.urls = list(dict.fromkeys(urls))
        results = pool.map(self.download_data, self.urls)
        pool.close()
        pool.join()
        self.fileLogger.info("OK, Downloading ended")
    
    def download_history(self):
        self.historic_days=[]
        if self.historic_start < self.historic_end:
            s_date_object = datetime.strptime(self.historic_start, "%Y%m%d")
            e_date_object = datetime.strptime(self.historic_end, "%Y%m%d")
            day = timedelta(days=1)
            while s_date_object <= e_date_object:
                self.historic_days.append(s_date_object.strftime('%Y%m%d'))
                s_date_object = s_date_object + day
        
        self.download_day_wise(True)
        self.logger.info("Download completed, program ended")
        self.fileLogger.info("Download completed, program ended")    

    def download_data(self, url):
        try:
            arr = url.split('/')
            self.logger.info("Start:{}[{}]".format(arr[5],arr[6]))
            name = "downloads/{}/{}_{}".format(arr[6].split(".")[0],arr[5],arr[6])
            self.fileLogger.info("Downloading data {}".format(url))
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(name, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
            self.logger.info("End:{}[{}]".format(arr[5],arr[6]))
        except Exception as e:
            self.logger.error(e)
            self.fileLogger.error(e)
            
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
        self.day_wise_flag = True
        self.range_flag = True

        try:
            dat = self.config['day_wise_download']['date']
        except Exception as e:
            self.day_wise_flag = False
            self.logger.warning("Skipping {}".format(e))
            self.fileLogger.warning("some checks in _config_basic_checks failed : -> {}".format(e))
        
        try:
            dat = self.config['date_range_download']['start']
            dat = self.config['date_range_download']['end']
        except Exception as e:
            self.range_flag = False
            self.logger.warning("Skipping {}".format(e))
            self.fileLogger.warning("some checks in _config_basic_checks failed : -> {}".format(e))

        self.fileLogger.info("Checks on config file ends")
    
    def _mapping_func(self):
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
        
        #Checking for future data 
        self._MIN_TC = self.mapping_df_tc[self.mapping_df_tc['id']==self.mapping_df_tc['id'].min()]
        self._MAX_TC = self.mapping_df_tc[self.mapping_df_tc['id']==self.mapping_df_tc['id'].max()]
        self._MIN_WEBPX = self.mapping_df_webpxtick_dt[self.mapping_df_webpxtick_dt['id']==self.mapping_df_webpxtick_dt['id'].min()]
        self._MAX_WEBPX = self.mapping_df_webpxtick_dt[self.mapping_df_webpxtick_dt['id']==self.mapping_df_webpxtick_dt['id'].max()]
        
        #self._INTERNAL_RANGE_START = max(self._MAX_WEBPX['id'])
        self._INTERNAL_RANGE_END = max(self._MAX_WEBPX['id']) + 3

        for i in reversed(range(self._INTERNAL_RANGE_START,self._INTERNAL_RANGE_END)):
            if i not in self.mapping_df_tc['id'].values:
                try:
                    df = pd.read_csv('https://links.sgx.com/1.0.0/derivatives-historical/{}/TC.txt'.format(i), sep='\t')
                    self._store_in_array(df['Business_Date'].unique(), i , "tc" )
                except Exception as e:
                    self.fileLogger.info("Cannot map [ignore] {}".format(e))

        self.fileLogger.info("Saving TC mapping back to disc")
        
        self.mapping_df_tc.drop_duplicates(keep = False, inplace = True)
        self.mapping_df_tc.to_csv(self.mapping_file_tc)
        
        self.fileLogger.info("OK, TC mapping completed")

        for i in reversed(range(self._INTERNAL_RANGE_START,self._INTERNAL_RANGE_END)):
            if i not in self.mapping_df_webpxtick_dt['id'].values:
                try:               
                    df = pd.read_csv('https://links.sgx.com/1.0.0/derivatives-historical/{}/WEBPXTICK_DT.zip'.format(i), sep=',')
                    self._store_in_array(df['Trade_Date'].unique(), i , "webpx")
                except Exception as e:
                    self.fileLogger.info("Cannot map [ignore] {}".format(e))
        
        self.fileLogger.info("Saving WEBPXTICK_DT mapping back to disc")
        
        self.mapping_df_webpxtick_dt.drop_duplicates(keep = False, inplace = True)
        self.mapping_df_webpxtick_dt.to_csv(self.mapping_file_webpxtick_dt)
        
        self.fileLogger.info("OK, WEBPXTICK_DT mapping completed")

        self.logger.info("Program initiated, downloading starts...")

    def __init__(self):
        # print("There are {} CPUs on this machine ".format(cpu_count()))
        # Defining internal variables
        self._INTERNAL_RANGE_START = 4660
        
        self._INTERNAL_RANGE_END = 4663

        p = Path('logs')
        p.mkdir(exist_ok=True)
        p = Path('.internal')
        p.mkdir(exist_ok=True)

        warnings.simplefilter(action='ignore', category=FutureWarning)
        logging.config.fileConfig('logging.conf')
        self.logger = logging.getLogger('stdoutLog')
        self.fileLogger = logging.getLogger('fileLog')
        self.fileLogger.info("================================= SGX SCRIBE =================================")
        self.fileLogger.info("Created logging objects")
        self.logger.info("Program starts")
        self.logger.info("Initialising...")
        self.fileLogger.info("Creating directories, if does not exists")

        
        self.fileLogger.info("OK, Creating directories done")

        self.fileLogger.info("Reading user input configs")
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.fileLogger.info("OK, Config file present")
        
        self.fileLogger.info("Getting MIN and MAX mapping!")


sgx_scribe = main()
sgx_scribe._mapping_func()
sgx_scribe._config_basic_checks()
sgx_scribe.read_config()

if sgx_scribe.day_wise_flag:
    sgx_scribe.download_day_wise(False)
if sgx_scribe.range_flag:
     sgx_scribe.download_history()
