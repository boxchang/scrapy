# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from stock.database import database

class FinancingPipeline(object):
    def open_spider(self, spider):
        db = database()
        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作
        self.cur.execute('CREATE TABLE if not exists financing (data_date varchar(10) NOT NULL,stock_no varchar(10) NOT NULL,stock_name varchar(60) NOT NULL,today_borrow_money double NULL,today_borrow_stock double NULL)')


    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()


    def process_item(self, item, spider):
        if spider.name == 'financing':
            col = ','.join(item.keys())
            placeholders = ("%s," * len(item))[:-1]
            sql = 'insert into financing({}) values({})'
            print(sql.format(col, placeholders), tuple(item.values()))
            self.cur.execute(sql.format(col, placeholders), tuple(item.values()))
            return item


class LegalPersonPipeline(object):


    def open_spider(self, spider):
        db = database()
        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作
        self.cur.execute('CREATE TABLE if not exists legalperson (data_date varchar(10) NOT NULL,stock_no varchar(10) NOT NULL,stock_name varchar(60) NOT NULL,china_buy double NULL,china_sell double NULL,china_sum double NULL, foreign_buy double NULL,foreign_sell double NULL,foreign_sum double NULL,invest_buy double NULL,invest_sell double NULL,invest_sum double NULL,com_sum double NULL,legalperson double NULL,created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')


    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()


    def process_item(self, item, spider):
        if spider.name == 'legalperson':
            col = ','.join(item.keys())
            # placeholders = ','.join(len(item) * '?')
            placeholders = ("%s," * len(item))[:-1]
            sql = 'insert into legalperson({}) values({})'
            print(sql.format(col, placeholders), tuple(item.values()))
            self.cur.execute(sql.format(col, placeholders), tuple(item.values()))
            return item


class MoneyReportPipeline(object):
    def open_spider(self, spider):
        db = database()
        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作
        self.cur.execute('create table if not exists moneyreport(myear varchar(4), season varchar(2), stock_no varchar(10), revenue bigint, '
                         ' grossprofit float, operprofit float, netprofit float, aftertaxprofit float, '
                         'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')


    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()


    def process_item(self, item, spider):
        if spider.name == 'money_report':
            col = ','.join(item.keys())
            # placeholders = ','.join(len(item) * '?')
            placeholders = ("%s," * len(item))[:-1]
            sql = 'insert into moneyreport({}) values({})'
            print(sql.format(col, placeholders), tuple(item.values()))
            self.cur.execute(sql.format(col, placeholders), tuple(item.values()))
            return item


class PricePipeline(object):
    def open_spider(self, spider):
        db = database()
        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作
        self.cur.execute('create table if not exists stockprice(batch_no varchar(10) NOT NULL,stock_no varchar(10), stock_name varchar(100), '
                         ' stock_buy bigint, stock_num bigint, stock_amount float, stock_sprice float, stock_hprice float, '
                         ' stock_lprice float, stock_eprice float, stock_status varchar(10), stock_gap float, '
                         ' stock_last_buy float, stock_last_bnum bigint, stock_last_sell float, stock_last_snum bigint, stock_value int,'
                         'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')
        # self.cur.execute('create table if not exists stockprice_hist(stock_no varchar(10), stock_name varchar(100), '
        #                  ' stock_buy bigint, stock_num bigint, stock_amount float, stock_sprice float, stock_hprice float, '
        #                  ' stock_lprice float, stock_eprice float, stock_status varchar(10), stock_gap float, '
        #                  ' stock_last_buy float, stock_last_bnum bigint, stock_last_sell float, stock_last_snum bigint, stock_value int,'
        #                  'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')


    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()


    def process_item(self, item, spider):
        if spider.name == 'price':
            col = ','.join(item.keys())
            # placeholders = ','.join(len(item) * '?')
            placeholders = ("%s," * len(item))[:-1]
            sql = 'insert into stockprice({}) values({})'
            print(sql.format(col, placeholders), tuple(item.values()))
            self.cur.execute(sql.format(col, placeholders), tuple(item.values()))
            return item


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

    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()



    def process_item(self, item, spider):
        if spider.name == 'holder':
            col = ','.join(item.keys())
            # placeholders = ','.join(len(item) * '?')
            placeholders = ("%s," * len(item))[:-1]
            sql = 'insert into stockholder({}) values({})'
            print(sql.format(col, placeholders), tuple(item.values()))
            self.cur.execute(sql.format(col, placeholders), tuple(item.values()))
            return item


class StockCodePipeline(object):
    def open_spider(self, spider):
        db = database()
        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作
        self.cur.execute('create table if not exists stockcode(stock_no varchar(10), stock_name varchar(60), '
                         'stock_isin varchar(14), stock_createdate varchar(10), stock_type varchar(10),'
                         'stock_industry varchar(14), stock_cficode varchar(10),'
                         'created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')
        sql = "delete from stockcode"
        db.execute_sql(sql)

    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()


    def process_item(self, item, spider):
        if spider.name == 'stock_code':
            col = ','.join(item.keys())
            # placeholders = ','.join(len(item) * '?')
            placeholders = ("%s," * len(item))[:-1]
            sql = 'insert into stockcode({}) values({})'
            print(sql.format(col, placeholders), tuple(item.values()))
            self.cur.execute(sql.format(col, placeholders), tuple(item.values()))
            return item
