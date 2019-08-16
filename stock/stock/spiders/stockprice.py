# -*- coding: utf-8 -*
import datetime

import scrapy
import pandas as pd
from io import StringIO

from stock.items import StockPriceItem


def clean(str):
    return str.replace(",", "").replace("--", "0")


class StockPrice(scrapy.Spider):
    name = 'price'
    date = datetime.date.today().strftime('%Y%m%d')
    date = "20190815"
    start_urls = ['https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date='+date+'&type=ALL']

    def parse(self, response):

        df = pd.read_csv(StringIO(response.text.replace("=", "")),
                           header=["證券代號" in l for l in response.text.split("\n")].index(True)-1)

        for index, row in df.iterrows():
            if len(clean(row['證券代號'])) == 4:
                item = StockPriceItem()
                item['stock_no'] = clean(row['證券代號']).zfill(6)
                item['stock_name'] = clean(row['證券名稱'])
                item['stock_buy'] = clean(row['成交股數'])
                item['stock_num'] = clean(row['成交筆數'])
                item['stock_amount'] = clean(row['成交金額'])
                item['stock_sprice'] = clean(row['開盤價'])
                item['stock_hprice'] = clean(row['最高價'])
                item['stock_lprice'] = clean(row['最低價'])
                item['stock_eprice'] = clean(row['收盤價'])
                item['stock_status'] = clean(row['漲跌(+/-)'])
                item['stock_gap'] = row['漲跌價差']
                item['stock_last_buy'] = clean(row['最後揭示買價'])
                item['stock_last_bnum'] = clean(row['最後揭示買量'])
                item['stock_last_sell'] = clean(row['最後揭示賣價'])
                item['stock_last_snum'] = clean(row['最後揭示賣量'])
                item['stock_value'] = clean(row['本益比'])
                yield item


