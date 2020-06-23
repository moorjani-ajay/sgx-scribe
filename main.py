import logging
import logging.config
import configparser
from datetime import date, timedelta
import requests
from tqdm import *
from pathlib import Path
import sys


class main:
    
    def read_config(self):
        dt = date.today()
        self.link_configs = {}
        self.day_wise_download_configs = {}
        self.logger.info("Step 2 : Config read starts")
        
        if self.all_good:
            for config_sec in self.config.sections():
                if(config_sec.startswith('file')):
                    self.logger.info("Reading all \"file\" configurations, link to download")
                    self.link_configs[config_sec] = dict(base = self.config[config_sec]['base'], filename = self.config[config_sec]['filename'])
            
            for obj in self.config['day_wise_download']:
                self.logger.info("Reading all \"day wise download\" configurations")
                self.day_wise_download_configs[obj]=self.config['day_wise_download'][obj]

        self.logger.info("Step 2 : Config read ends")

    def config_basic_checks(self):
        self.logger.info("Step 1 : Checks on config file starts")
        self.all_good = True

        # 1. Check for download folder
        if not ('download_folder' in self.config):
            self.logger.error("Downlaod folder config missing, will end the program!")
            self.logger.error("Please provide download config in the config.ini file")
            self.all_good = False
            sys.exit()

        # 2. Check for day_wise_download
        if not ('day_wise_download' in self.config):
            self.logger.error("day_wise_download config missing, will end the program!")
            self.logger.error("Please provide day_wise_download config in the config.ini file")
            self.all_good = False
            sys.exit()
        
        # 3. Check for date_range_download
        if not ('date_range_download' in self.config):
            self.logger.warning("date_range_download config missing, program will continue")
            self.logger.warning("Please provide date_range_download config in the config.ini file, to download historic files")
        
        self.logger.info("Step 1 : Checks on config file ends")

    def download_day_wise(self):
        self.logger.info("Step 3 : Download day wise data starts")
        for (key,value) in self.day_wise_download_configs.items():

            p = Path(self.config['download_folder']['location'] + key)
            p.mkdir(parents=True,exist_ok=True)
            
            self.logger.info("Getting config for link to download")
            
            for obj in value.split(","):
                    
                    d0 = date(2007, 9, 15) # Static definition of the date
                    d1 = date(int(obj.split("-")[0]), int(obj.split("-")[1]), int(obj.split("-")[2]))
                    
                    if(d1.weekday()<5):
                        delta = d1 - d0
                        self.download_data(self.link_configs[key]['base'], delta.days, self.link_configs[key]['filename'], obj, key)
                    else:
                        self.logger.info("It's a weekend skipping")
        
        self.logger.info("Step 3 : Download day wise data ends")
    
    def download_data(self, base, days, filename, date, key):
        url = base+str(days)+"/"+filename
        name = "downloads/"+key+"/"+date+"_WEBPXTICK_DT.zip"

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(name, 'wb') as f:
                pbar = tqdm(total=int(r.headers['Content-Length']))
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        pbar.update(len(chunk))
    
    def __init__(self):
        p = Path('logs')
        p.mkdir(exist_ok=True)
        logging.config.fileConfig('logging.conf')
        # create logger
        self.logger = logging.getLogger('stdoutLog')
        self.fileLogger = logging.getLogger('fileLog')
        # 'application' code
        # logger.debug('debug message')
        # logger.info('info message')
        # logger.warning('warn message')
        # logger.error('error message')
        # logger.critical('critical message')
        # fileLogger.debug('debug message in file')
        # Readint the config file 
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        #logger.info("Hello")
        # Read the config file
        # Set logging parameters override
        # Start downloading day-wise files
        # Start downloading historic files
        # End

    
sgx_scribe = main()
sgx_scribe.config_basic_checks()
sgx_scribe.read_config()
sgx_scribe.download_day_wise()