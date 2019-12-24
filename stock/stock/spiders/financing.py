# -*- coding: utf-8 -*
import datetime
import scrapy
import pandas as pd
from io import StringIO
from stock.database import database
from stock.items import FinancingItem


def clean(str):
    return str.replace(",", "").replace("--", "0").replace("=\"", "").replace("\"", "")

class StockCodeSpider(scrapy.Spider):
    name = 'financing'

    custom_settings = {
        'DOWNLOAD_DELAY': 30,
        'CONCURRENT_REQUESTS': 1,
        'ITEM_PIPELINES': {
            'stock.pipelines.FinancingPipeline': 100
        }
    }



    data_date = datetime.date.today().strftime('%Y%m%d')
    #data_date = "20191223"
    url = 'https://www.twse.com.tw/exchangeReport/MI_MARGN?response=csv&date='+data_date+'&selectType=ALL'
    start_urls = [url]
                  #https://www.twse.com.tw/exchangeReport/MI_MARGN?response=csv&date=20191218&selectType=ALL
    def parse(self, response):
        if response.text != '':  # 有資料才跑，不然會遇到假日全都沒資料
            df = pd.read_csv(StringIO(response.text), header=7).dropna(how='all', axis=1).dropna(how='any')

            try:
                r = 0
                for index, row in df.iterrows():
                    if len(clean(row['股票代號'])) == 4:
                        if r == 0:  #有資料才刪除
                            db = database()
                            sql = "delete from financing"
                            db.execute_sql(sql)

                        item = FinancingItem()
                        item['data_date'] = self.data_date
                        item['stock_no'] = clean(row['股票代號']).zfill(6)  # 證券代號
                        item['stock_name'] = clean(row['股票名稱'])  # 股票名稱
                        item['today_borrow_money'] = float(clean(str(row[6])))  # 融資今日餘額
                        item['today_borrow_stock'] = float(clean(str(row[12])))  # 融券今日餘額
                        yield item
                        r += 1
            except:
                pass