#!/usr/bin/env bash

cd /src/scrapy/stock

scrapy crawl legalperson  #外資買賣超


cd /src/scrapy/stock/jobs

python legalperson_price.py  #整理三大法人持股及股票金額，要計算三大法人平均成本使用

python legalperson_daily.py  #監控每日三大法人是否有大量買進情況