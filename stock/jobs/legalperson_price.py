# -*- coding: utf-8 -*
#!/usr/bin/python
import sys

sys.path.append("..")
from stock.database import database
from stock.line import lineNotifyMessage

class LegalPerson(object):
    LEGALPERSON_PRICE_TABLE = ('CREATE TABLE if not exists legalperson_price (batch_no varchar(10), stock_no varchar(10) NOT NULL, stock_name varchar(60) NOT NULL,china_buy double NULL,china_sell double NULL,china_sum double NULL, foreign_buy double NULL,foreign_sell double NULL,foreign_sum double NULL,invest_buy double NULL,invest_sell double NULL,invest_sum double NULL,com_sum double NULL,legalperson double NULL,stock_price float,created_date TimeStamp DEFAULT CURRENT_TIMESTAMP)')

    def execute(self):
        self.open_conn()
        self.create_legalpersonPrice_table()
        self.delete_legalperson_price()
        self.combine_legalperson_price()


    def open_conn(self):
        db = database()
        self.conn = db.create_connection()

    def create_legalpersonPrice_table(self):
        sql = self.LEGALPERSON_PRICE_TABLE
        db = database()
        db.execute_sql(sql)

    def delete_legalperson_price(self):
        db = database()
        sql = "delete from legalperson_price where batch_no = Date_format(now(),'%Y%m%d')"
        db.execute_sql(sql)


    def combine_legalperson_price(self):
        db = database()
        sql = "insert into legalperson_price (" \
              "select Date_format(now(),'%Y%m%d') nowdate,a.stock_no,a.stock_name,a.china_buy,a.china_sell,a.china_sum," \
              "a.foreign_buy,a.foreign_sell,a.foreign_sum,a.invest_buy,a.invest_sell,a.invest_sum,a.com_sum,a.legalperson,b.stock_last_buy,now()  " \
              "from legalperson a, stockprice b, robert_stock_list c " \
              "where a.stock_no = b.stock_no and a.stock_no = c.stock_no)"
        db.execute_sql(sql)


legal_person = LegalPerson()
legal_person.execute()