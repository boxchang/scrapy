# -*- coding: utf-8 -*
import datetime
import sys

import scrapy
import pandas as pd
from io import StringIO

from stock.database import database
from stock.items import LegalPersonItem


def clean(str):
    return str.replace(",", "").replace("--", "0")


class LegalPerson(scrapy.Spider):
    name = 'legalperson'

    custom_settings = {
        'ITEM_PIPELINES': {
            'stock.pipelines.LegalPersonPipeline': 300
        }
    }

    data_date = datetime.date.today().strftime('%Y%m%d')
    #data_date = "20220112"

    start_urls = ['http://www.twse.com.tw/fund/T86?response=csv&date='+data_date+'&selectType=ALLBUT0999']

    def parse(self, response):

        if response.text != '\r\n':  # 有資料才跑，不然會遇到假日全都沒資料
            db = database()
            sql = "delete from legalperson"
            db.execute_sql(sql)

            # 製作三大法人的DataFrame
            try:
                df = pd.read_csv(StringIO(response.text), header=1).dropna(how='all', axis=1).dropna(how='any')

                for index, row in df.iterrows():
                    if len(clean(row['證券代號'])) == 4:
                        item = LegalPersonItem()
                        item['data_date'] = self.data_date
                        item['stock_no'] = clean(row['證券代號']).zfill(6)
                        item['stock_name'] = clean(row['證券名稱'])
                        item['china_buy'] = float(clean(str(row['外陸資買進股數(不含外資自營商)'])))
                        item['china_sell'] = float(clean(str(row['外陸資賣出股數(不含外資自營商)'])))
                        item['china_sum'] = float(clean(str(row['外陸資買賣超股數(不含外資自營商)'])))
                        item['foreign_buy'] = float(clean(str(row['外資自營商買進股數'])))
                        item['foreign_sell'] = float(clean(str(row['外資自營商賣出股數'])))
                        item['foreign_sum'] = float(clean(str(row['外資自營商買賣超股數'])))
                        item['invest_buy'] = float(clean(str(row['投信買進股數'])))
                        item['invest_sell'] = float(clean(str(row['投信賣出股數'])))
                        item['invest_sum'] = float(clean(str(row['投信買賣超股數'])))
                        item['com_sum'] = float(clean(str(row['自營商買賣超股數'])))
                        item['hedge_buy'] = float(clean(str(row['自營商買進股數(避險)'])))
                        item['hedge_sell'] = float(clean(str(row['自營商賣出股數(避險)'])))
                        item['hedge_sum'] = float(clean(str(row['自營商買賣超股數(避險)'])))
                        item['legalperson'] = float(clean(str(row['三大法人買賣超股數'])))
                        yield item
            except:
                pass




