#!/usr/bin/env bash

j=20221114
while [ $j -le 20221209 ] ;do
    echo $j

    cd /src/scrapy/stock

    scrapy crawl legalperson -a opt_date=$j  #外資買賣超

    scrapy crawl financing -a opt_date=$j  #融資融券

    cd /src/scrapy/stock/jobs

    python stockpricebyD.py $j

    python legalperson_price.py $j  #整理三大法人持股及股票金額，要計算三大法人平均成本使用

    python legalperson_daily.py $j  #監控每日三大法人是否有大量買進情況


    j=`TZ=UTC date -d "$j +1 day" +%Y%m%d`
done