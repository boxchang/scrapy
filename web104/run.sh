#!/usr/bin/env bash


cd /src/scrapy/web104

scrapy crawl web104

cd /src/scrapy/web104/jobs

python fetchdata.py

cd /src/scrapy/web104

sh clean.sh
