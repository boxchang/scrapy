# -*- coding: utf-8 -*
import datetime
import scrapy
import json
from stock.database import database
from stock.items import TaiexItem


def clean(str):
    return str.replace(",", "").replace("--", "0").replace("=\"", "").replace("\"", "")

#將數字字串裡的千分符','用''取代，例如123,456轉成123456
def num_comma_clear(arg):
	return str(arg.replace(",",""))

class StockCodeSpider(scrapy.Spider):
    name = 'taiex'

    custom_settings = {
        'DOWNLOAD_DELAY': 30,
        'CONCURRENT_REQUESTS': 1,
        'ITEM_PIPELINES': {
            'stock.pipelines.TaiexPipeline': 100
        }
    }


    data_date = datetime.date.today().strftime('%Y%m%d')
    #data_date = "20191223"
    url = 'https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date='+data_date
    start_urls = [url]

    def parse(self, response):
        if response.text != '':  # 有資料才跑，不然會遇到假日全都沒資料
            json_data = json.loads(response.text)

            for j in json_data['data']:
                tmp = str(j[0]).replace(" ", "").split("/")
                date_tmp = str(int(tmp[0])+1911) + tmp[1] + tmp[2]
                if self.validate(date_tmp):
                    item = TaiexItem()
                    item['data_date'] = date_tmp  #日期
                    item['open_index'] = num_comma_clear(j[1])   #開盤指數
                    item['high_index'] = num_comma_clear(j[2])   #最高指數
                    item['low_index'] = num_comma_clear(j[3])    #最低指數
                    item['close_index'] = num_comma_clear(j[4])  #收盤指數
                    yield item
                    print(str(j[0]).replace(" ", ""), num_comma_clear(j[1]), num_comma_clear(j[2]), num_comma_clear(j[3]),num_comma_clear(j[4]))


    def validate(self, data_date):
        db = database()
        conn = db.create_connection()
        cur = conn.cursor()
        sql = "SELECT * FROM taiex where data_date = '{data_date}'"
        sql = sql.format(data_date = data_date)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True