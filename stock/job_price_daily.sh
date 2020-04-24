#!/usr/bin/env bash

#cd /src/scrapy/stock

#scrapy crawl price  #股票每日價格


cd /src/scrapy/stock/jobs

python stockpricebyD.py

python stockprice_ma.py