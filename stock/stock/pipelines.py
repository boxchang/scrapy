# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from stock.database import database


class StockPipeline(object):
    def open_spider(self, spider):
        db = database()
        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作
        self.cur.execute('create table if not exists stockholder(stock_no varchar(10), level int, '
                         'stock_num bigint, holder_num bigint, percent float, data_date date, '
                         'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')
        self.cur.execute('create table if not exists stockholder_hist(stock_no varchar(10), level int, '
                         'stock_num bigint, holder_num bigint, percent float, data_date date, '
                         'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')
        self.conn.commit()

    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()



    def process_item(self, item, spider):
        col = ','.join(item.keys())
        # placeholders = ','.join(len(item) * '?')
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into stockholder({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        self.cur.execute(sql.format(col, placeholders), tuple(item.values()))
        return item
