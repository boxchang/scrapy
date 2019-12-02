#!/usr/bin/env bash

cd /src/scrapy/stock

scrapy crawl legalperson

cd /src/scrapy/stock/jobs

python legalperson_price.py