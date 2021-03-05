#!/usr/bin/env bash

cd /src/scrapy/stock

scrapy crawl legalperson  #外資買賣超


cd /src/scrapy/stock/jobs
python broker_daily.py #監控券商交易量

python legalperson_price.py  #整理三大法人持股及股票金額，要計算三大法人平均成本使用

python legalperson_daily.py  #監控每日三大法人是否有大量買進情況

python flag_monitor_daily.py #監控旗標股票加碼或減碼

cd /src/scrapy/stock/jobs/predict    #到7月就可以先關掉
python q3q4_predict.py today 2020 Q4 #取2020Q4的EPS 再取今日股價更新殖息率
python q3q4_monitor.py