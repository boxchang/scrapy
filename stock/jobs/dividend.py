# -*- coding: utf-8 -*
import datetime
import pandas as pd
from stock.database import database
import html5lib
import time

def clean(str):
    return str.replace(",", "").replace("--", "0").replace("=\"", "").replace("\"", "")

# 手動執行JOB，一年執行一次，等九月股利都公佈後再執行
class Dividend(object):

    CREATE_DIVIDEND_TABLE = "CREATE TABLE if not exists dividend (stock_no VARCHAR(6) NOT NULL,year INT NOT NULL,cash FLOAT NOT NULL,stock_earn FLOAT NOT NULL,stock_capital FLOAT NOT NULL,stock FLOAT NOT NULL,total FLOAT NOT NULL,create_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    CREATE_CALCULATING_PRICE_TABLE = "CREATE TABLE if not exists calculating_price (stock_no VARCHAR(6) NOT NULL,cheap FLOAT NOT NULL,normal FLOAT NOT NULL,expensive FLOAT NOT NULL,create_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"

    def execute(self):
        self.create_dividend_table()
        self.create_calculating_price_table()
        self.save_dividend_data()
        self.calculating_price()


    def calculating_price(self):
        sql = "insert into calculating_price(stock_no,cheap,normal,expensive) (select a.stock_no,round(avg(a.total)*15,2) cheap,round(avg(a.total)*20,2) normal,round(avg(a.total)*25,2) expensive from dividend a group by a.stock_no)"
        db = database()
        db.execute_sql(sql)

    def delete_dividend(self):
        sql = "delete from dividend"
        db = database()
        db.execute_sql(sql)

    def save_dividend_data(self):
        db = database()
        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作

        sql = "select stock_no from stockcode a where stock_cficode = 'ESVUFR' and not exists (select * from dividend b where a.stock_no = b.stock_no)"
        self.cur.execute(sql)
        rows = self.cur.fetchall()

        for row in rows:
            stock_no = row[0][2:6]
            self.get_stock_dividend(stock_no) # 右取四碼
            # time.sleep(2)


    def get_stock_dividend(self, stock_no):
        print(stock_no)
        url = 'https://tw.stock.yahoo.com/d/s/dividend_{stock_no}.html'
        ## url = 'https://tw.stock.yahoo.com/d/s/dividend_1504.html'
        url =url.format(stock_no=stock_no)
        df = pd.read_html(url, encoding="Big5")[3]

        ## 去除中文欄位名稱，程式中盡量避免出現中文
        df = df.drop(0).reset_index(drop=True)

        for index, row in df.iterrows():
            col1 = row[0]  # 年度
            col2 = row[1]  # 現金股利
            col3 = row[2]  # 盈餘配股
            col4 = row[3]  # 公積配股
            col5 = row[4]  # 股票股利
            col6 = row[5]  # 合計
            print(stock_no,col1)
            self.insert_dividend(stock_no,col1,col2,col3,col4,col5,col6)


    def insert_dividend(self, stock_no, col1, col2, col3, col4, col5, col6):
        sql = "insert into dividend(stock_no,year,cash,stock_earn,stock_capital,stock,total) " \
              "values('{stock_no}',{year},{cash},{stock_earn},{stock_capital},{stock},{total})"
        sql = sql.format(stock_no=stock_no.zfill(6),year=col1,cash=col2,stock_earn=col3,stock_capital=col4,stock=col5,total=col6)
        db = database()
        db.execute_sql(sql)


    def create_calculating_price_table(self):
        sql = self.CREATE_CALCULATING_PRICE_TABLE
        db = database()
        db.execute_sql(sql)


    def create_dividend_table(self):
        sql = self.CREATE_DIVIDEND_TABLE
        db = database()
        db.execute_sql(sql)

dividend = Dividend()
dividend.execute()