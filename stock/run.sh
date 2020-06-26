#!/usr/bin/env bash

cd /src/scrapy/stock

#基本上星期六早上才有新值
scrapy crawl holder

cd /src/scrapy/stock/jobs

#要跑完Holder後再跑policy才有意義
python policy.py

python dividend_notice.py

cd /src/scrapy/stock

sh clean.sh