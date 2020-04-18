#!/usr/bin/env bash


#cd /src/scrapy/web104

#scrapy crawl web104

cd /src/scrapy/web104/jobs

python getweb104_s.py

python getweb104_w.py

python fetchdata.py

cd /src/scrapy/web104

sh clean.sh
