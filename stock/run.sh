#!/usr/bin/env bash


cd /src/scrapy/stock

scrapy crawl holder

cd /src/scrapy/stock/stock

python policy.py

cd /src/scrapy/stock

sh clean.sh