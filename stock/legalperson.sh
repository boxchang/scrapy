#!/usr/bin/env bash

cd /src/scrapy/stock

scrapy crawl legalperson

scrapy crawl financing

cd /src/scrapy/stock/jobs

python legalperson_price.py

python financing.py