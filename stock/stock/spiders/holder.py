# -*- coding: utf-8 -*
import scrapy
import pandas as pd
import io

from stock.database import database
from stock.items import StockItem


class Holder(scrapy.Spider):
    name = 'holder'

    custom_settings = {
        'ITEM_PIPELINES': {
            'stock.pipelines.StockPipeline': 200
        }
    }

    start_urls = ['https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5']
    # def start_requests(self):
    #     pass
    db = database()
    conn = db.create_connection()

    def copy2Hist(self):
        db = database()
        sql = "insert into stockholder_hist " \
              " (select * from stockholder a where not exists (select * from stockholder_hist b where a.stock_no = b.stock_no and a.created_date = b.created_date))"
        db.execute_sql(sql)

    def clearTable(self):
        db = database()
        sql = "delete from stockholder"
        db.execute_sql(sql)



    def parse(self, response):
        url = 'https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5'
        data = pd.read_csv(url)
        # print(data['持股分級'])
        # print(data[data['持股分級'] == 15][:17])
        # print(data[:17])
        check_data = data['資料日期'][:1][0]
        print(check_data)

        if self.validate(check_data):
            self.copy2Hist()  # 跑程式前，先將資料Copy到歷史區
            self.clearTable() # 清除目前的要運算的表
            # csv = data[data['資料日期'] == 15][:1]
            for index, row in data.iterrows():
                item = StockItem()
                item['stock_no'] = row['證券代號'].zfill(6)
                item['stock_num'] = row['股數']
                item['level'] = row['持股分級']
                item['holder_num'] = row['人數']
                item['percent'] = row['占集保庫存數比例%']
                item['data_date'] = row['資料日期']
                yield item

    def validate(self, data_date):
        # file = "D:\\0)SourceCode\\scrapy\\web104\\web104.sqlite"
        # # create a database connection
        # db = database()
        # conn = db.create_sqlite_connection(file)

        self.cur = self.conn.cursor()
        sql = "SELECT * FROM stockholder_hist where data_date = str_to_date('{data_date}', '%Y%m%d')"
        sql = sql.format(data_date = data_date)
        self.cur.execute(sql)

        rows = self.cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True