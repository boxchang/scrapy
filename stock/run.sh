#!/usr/bin/env bash


cd /src/scrapy/stock

scrapy crawl holder
scrapy crawl price

cd /src/scrapy/stock/jobs

python policy.py

cd /src/scrapy/stock

sh clean.sh