# -*- coding: utf-8 -*
#!/usr/bin/python
import sys

sys.path.append("..")
from stock.database import database
from stock.line import lineNotifyMessage
import datetime

#日排程
#只盯特定的股票，沒有全盯
class LegalPerson(object):
    LEGALPERSON_PRICE_TABLE = ('CREATE TABLE if not exists legalperson_price (batch_no varchar(10), stock_no varchar(10) NOT NULL, stock_name varchar(60) NOT NULL,china_buy double NULL,china_sell double NULL,china_sum double NULL, foreign_buy double NULL,foreign_sell double NULL,foreign_sum double NULL,invest_buy double NULL,invest_sell double NULL,invest_sum double NULL,com_sum double NULL,legalperson double NULL,stock_price float,created_date TimeStamp DEFAULT CURRENT_TIMESTAMP,stock_num double NULL,percent float NULL)')

    def execute(self):
        data_date = datetime.date.today().strftime('%Y%m%d')
        #data_date = "20220113"
        self.open_conn()
        self.create_legalpersonPrice_table()
        self.delete_legalperson_price(data_date)
        self.combine_legalperson_price(data_date)


    def open_conn(self):
        db = database()
        self.conn = db.create_connection()

    def create_legalpersonPrice_table(self):
        sql = self.LEGALPERSON_PRICE_TABLE
        db = database()
        db.execute_sql(sql)

    def delete_legalperson_price(self,data_date):
        db = database()
        sql = "delete from legalperson_price where batch_no = '{data_date}'"
        sql = sql.format(data_date=data_date)
        db.execute_sql(sql)


    def combine_legalperson_price(self,data_date):
        db = database()
        sql = "insert into legalperson_price (" \
              "select '{data_date}' nowdate,a.stock_no,a.stock_name,a.china_buy,a.china_sell,a.china_sum," \
              "a.foreign_buy,a.foreign_sell,a.foreign_sum,a.invest_buy,a.invest_sell,a.invest_sum,a.com_sum,a.hedge_buy,a.hedge_sell,a.hedge_sum,a.legalperson,b.stock_last_buy,now(),d.stock_num,round((legalperson/d.stock_num*100),2) percent  " \
              "from legalperson a, stockprice b, robert_stock_list c, stockholder d " \
              "where a.stock_no = b.stock_no and a.stock_no = c.stock_no and b.batch_no = a.data_date and d.level=17 and d.stock_no = a.stock_no and a.data_date='{data_date}')"
        sql = sql.format(data_date=data_date)
        db.execute_sql(sql)


legal_person = LegalPerson()
legal_person.execute()