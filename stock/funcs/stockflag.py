# -*- coding: utf-8 -*
import sys
sys.path.append("..")

from stock.database import database

class stockflag(object):
    def create_stock_flag_table(self):
        sql = self.CREATE_STOCK_FLAG_TABLE
        db = database()
        db.execute_sql(sql)

    def saveFlagDate(self ,data_date ,stock_no):
        self.create_stock_flag_table()
        db = database()
        sql = "insert into stockflag(data_date,stock_no,stock_name,stock_lprice,close_index) " \
              " (select batch_no,stock_no,stock_name,stock_lprice,close_index from stockprice a, taiex b where a.stock_no = '{stock_no}' and batch_no = '{data_date}' and a.batch_no = b.data_date)"
        sql = sql.format(stock_no=stock_no, data_date=data_date)
        db.execute_sql(sql)

    def delFlagDate(self,stock_no):
        db = database()
        sql = "update stockflag set enable = 'N' where stock_no={stock_no} and enable is null"
        sql = sql.format(stock_no=stock_no)
        db.execute_sql(sql)