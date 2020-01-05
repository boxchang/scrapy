# -*- coding: utf-8 -*
#!/usr/bin/python
import sys
import datetime

sys.path.append("..")
from stock.database import database


class Price_ma5(object):
    PRICE_MA5_TABLE = (
        'CREATE TABLE if not exists stockprice_ma5 (stock_no varchar(10),avg_price float,created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')

    def execute(self):
        self.create_stockprice_ma5_table()
        self.delete_stockprice_ma5()
        self.insert_stockprice_ma5()

    def delete_stockprice_ma5(self):
        sql = 'delete from stockprice_ma5'
        db = database()
        db.execute_sql(sql)

    def insert_stockprice_ma5(self):
        sql = 'insert into stockprice_ma5 (stock_no,avg_price) (select a.stock_no,round(avg(stock_eprice),2) from stockprice a where a.batch_no in ( select * from (select data_date from taiex order by data_date desc limit 5) b) group by a.stock_no)'
        db = database()
        db.execute_sql(sql)

    def create_stockprice_ma5_table(self):
        sql = self.PRICE_MA5_TABLE
        db = database()
        db.execute_sql(sql)

price_ma5 = Price_ma5()
price_ma5.execute()