#!/usr/bin/env bash

cd /src/scrapy/stock

#一般都是每天六點後才有新值
scrapy crawl price

scrapy crawl legalperson

scrapy crawl financing

cd /src/scrapy/stock/jobs

python legalperson_price.py

python financing.py