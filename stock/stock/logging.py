# -*- coding: utf-8 -*
import datetime
import logging

data_date = datetime.date.today().strftime('%Y%m%d')
FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.DEBUG, filename='stock/log/scrapy_'+data_date+'.log', filemode='w', format=FORMAT)

def error_log(self,message):
    logging.error(message)

def debug_log(self,message):
    logging.debug(message)

def info_log(self,message):
    logging.info(message)

def warning_log(self,message):
    logging.warning(message)

def critical_log(self,message):
    logging.critical(message)