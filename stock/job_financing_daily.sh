#!/usr/bin/env bash

cd /src/scrapy/stock

scrapy crawl financing  #融資融券

cd /src/scrapy/stock/jobs

#python financing.py  #抓出融資融券異常股票