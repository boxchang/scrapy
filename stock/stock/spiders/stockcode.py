# -*- coding: utf-8 -*
import scrapy
import requests
from bs4 import BeautifulSoup

from stock.items import StockCodeItem

TWSE_URL = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
TPEX_URL = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=4'

columns = ['dtype', 'code', 'name', '國際證券辨識號碼', '上市日', '市場別', '產業別', 'CFI']


class StockCodeSpider(scrapy.Spider):
    name = 'stock_code'

    custom_settings = {
        'DOWNLOAD_DELAY': 30,
        'CONCURRENT_REQUESTS': 1,
        'ITEM_PIPELINES': {
            'stock.pipelines.StockCodePipeline': 100
        }
    }

    start_urls = [TWSE_URL, TPEX_URL]


    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find("table", {"class": "h4"})
        c = 0
        for row in table.find_all("tr"):
            data = [col.text for col in row.find_all('td')]
            if data[0].find('\u3000') > 0:
                code, name = data[0].split('\u3000')
                #yield dict(zip(columns, [dtype, code, name, *row[1: -1]]))
                #print([code, name, *data[1: -1]])
                item = StockCodeItem()
                item['stock_no'] = code  # 證券代號
                item['stock_name'] = name  # 證券名稱
                item['stock_isin'] = data[1]  # 國際證券辨識號碼(ISIN Code)
                item['stock_createdate'] = data[2]  # 上市日
                item['stock_type'] = data[3]  # 市場別
                item['stock_industry'] = data[4]  # 產業別
                item['stock_cficode'] = data[5]  # CFICode
                yield item

            c+=1
