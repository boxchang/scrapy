#!/usr/bin/env bash

cd /src/scrapy/stock/jobs/predict    #到7月就可以先關掉
python q3q4_predict.py today 2020 Q4 #取2020Q4的EPS 再取今日股價更新殖息率
python q3q4_monitor.py