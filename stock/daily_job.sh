#!/usr/bin/env bash

cd /src/scrapy/stock

#一般都是每天六點後才有新值
scrapy crawl taiex  #大盤指數

scrapy crawl price  #股票每日價格

scrapy crawl financing  #融資融券

scrapy crawl legalperson  #外資買賣超



cd /src/scrapy/stock/jobs

python financing.py  #抓出融資融券異常股票

python legalperson_price.py  #整理三大法人持股及股票金額，要計算三大法人平均成本使用

python legalperson_daily.py  #監控每日三大法人是否有大量買進情況

