#!/usr/bin/env bash


cd /src/scrapy/stock

scrapy crawl holder

cd /src/scrapy/stock/jobs

python policy.py

cd /src/scrapy/stock

sh clean.sh