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

class Price_ma10(object):
    PRICE_MA10_TABLE = (
        'CREATE TABLE if not exists stockprice_ma10 (stock_no varchar(10),avg_price float,created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')

    def execute(self):
        self.create_stockprice_ma10_table()
        self.delete_stockprice_ma10()
        self.insert_stockprice_ma10()

    def delete_stockprice_ma10(self):
        sql = 'delete from stockprice_ma10'
        db = database()
        db.execute_sql(sql)

    def insert_stockprice_ma10(self):
        sql = 'insert into stockprice_ma10 (stock_no,avg_price) (select a.stock_no,round(avg(stock_eprice),2) from stockprice a where a.batch_no in ( select * from (select data_date from taiex order by data_date desc limit 10) b) group by a.stock_no)'
        db = database()
        db.execute_sql(sql)

    def create_stockprice_ma10_table(self):
        sql = self.PRICE_MA10_TABLE
        db = database()
        db.execute_sql(sql)

class Price_ma20(object):
    PRICE_MA20_TABLE = (
        'CREATE TABLE if not exists stockprice_ma20 (stock_no varchar(10),avg_price float,created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')

    def execute(self):
        self.create_stockprice_ma20_table()
        self.delete_stockprice_ma20()
        self.insert_stockprice_ma20()

    def delete_stockprice_ma20(self):
        sql = 'delete from stockprice_ma20'
        db = database()
        db.execute_sql(sql)

    def insert_stockprice_ma20(self):
        sql = 'insert into stockprice_ma20 (stock_no,avg_price) (select a.stock_no,round(avg(stock_eprice),2) from stockprice a where a.batch_no in ( select * from (select data_date from taiex order by data_date desc limit 20) b) group by a.stock_no)'
        db = database()
        db.execute_sql(sql)

    def create_stockprice_ma20_table(self):
        sql = self.PRICE_MA20_TABLE
        db = database()
        db.execute_sql(sql)

class Price_ma60(object):
    PRICE_MA60_TABLE = (
        'CREATE TABLE if not exists stockprice_ma60 (stock_no varchar(10),avg_price float,created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')

    def execute(self):
        self.create_stockprice_ma60_table()
        self.delete_stockprice_ma60()
        self.insert_stockprice_ma60()

    def delete_stockprice_ma60(self):
        sql = 'delete from stockprice_ma60'
        db = database()
        db.execute_sql(sql)

    def insert_stockprice_ma60(self):
        sql = 'insert into stockprice_ma60 (stock_no,avg_price) (select a.stock_no,round(avg(stock_eprice),2) from stockprice a where a.batch_no in ( select * from (select data_date from taiex order by data_date desc limit 60) b) group by a.stock_no)'
        db = database()
        db.execute_sql(sql)

    def create_stockprice_ma60_table(self):
        sql = self.PRICE_MA60_TABLE
        db = database()
        db.execute_sql(sql)


class Price_ma240(object):
    PRICE_MA240_TABLE = (
        'CREATE TABLE if not exists stockprice_ma240 (stock_no varchar(10),avg_price float,created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')

    def execute(self):
        self.create_stockprice_ma240_table()
        self.delete_stockprice_ma240()
        self.insert_stockprice_ma240()

    def delete_stockprice_ma240(self):
        sql = 'delete from stockprice_ma240'
        db = database()
        db.execute_sql(sql)

    def insert_stockprice_ma240(self):
        sql = 'insert into stockprice_ma240 (stock_no,avg_price) (select a.stock_no,round(avg(stock_eprice),2) from stockprice a where a.batch_no in ( select * from (select data_date from taiex order by data_date desc limit 20) b) group by a.stock_no)'
        db = database()
        db.execute_sql(sql)

    def create_stockprice_ma240_table(self):
        sql = self.PRICE_MA240_TABLE
        db = database()
        db.execute_sql(sql)


price_ma5 = Price_ma5()
price_ma5.execute()

price_ma10 = Price_ma10()
price_ma10.execute()

price_ma20 = Price_ma20()
price_ma20.execute()

price_ma60 = Price_ma60()
price_ma60.execute()

price_ma240 = Price_ma240()
price_ma240.execute()