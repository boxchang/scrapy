# -*- coding: utf-8 -*
import MySQLdb
import sys
sys.path.append("..")
import datetime
from stock.database import database
import requests
import pandas as pd
from io import StringIO
import re, time

class DividendNotice(object):
    DIVIDEND_NOTICE_TABLE = "CREATE TABLE if not exists dividend_notice (dividend_date VARCHAR(8) NOT NULL," \
                            "stock_no VARCHAR(8) NOT NULL,stock_name VARCHAR(50) NOT NULL," \
                            "dividend_type VARCHAR(10) NOT NULL,stock FLOAT NOT NULL DEFAULT 0," \
                            "increase_rate FLOAT NOT NULL DEFAULT 0,buy_price FLOAT NOT NULL DEFAULT 0," \
                            "money FLOAT NOT NULL DEFAULT 0,season VARCHAR(50) NOT NULL DEFAULT '0'," \
                            "networth FLOAT NOT NULL DEFAULT 0,eps FLOAT NOT NULL DEFAULT 0," \
                            "create_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"

    def clean(self, str):
        result = "0"
        try:
            result = str.replace(",", "").replace("--", "0")
        except:
            pass
        return result

    def conn_close(self):
        self.conn.close()

    def __init__(self):
        self.db = database()
        self.conn = self.db.create_connection()

    def DelDividendNotice(self):
        cur = self.conn.cursor()
        sql = "delete from dividend_notice"
        cur.execute(sql)

    def create_dividendnotice_table(self):
        sql = self.DIVIDEND_NOTICE_TABLE
        db = database()
        db.execute_sql(sql)

    def InsDividendNotice(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into dividend_notice({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))

    def execute(self):
        self.create_dividendnotice_table()

        url = 'https://www.twse.com.tw/exchangeReport/TWT48U?response=csv'
        res = requests.get(url)

        if len(res.text) > 0:
            self.DelDividendNotice()

            df = pd.read_csv(StringIO(res.text), header=1).dropna(how='all', axis=1).dropna(how='any')
            for index, row in df.iterrows():
                if len(row['股票代號']) == 4:
                    item = {}
                    item['dividend_date'] = int(row['除權除息日期'].replace('年','').replace('月','').replace('日',''))+19110000
                    item['stock_no'] = row['股票代號'].zfill(6)
                    item['stock_name'] = row['名稱']
                    item['dividend_type'] = row['除權息']
                    item['stock'] = float(self.clean(str(row['無償配股率'])))
                    item['increase_rate'] = float(self.clean(str(row['現金增資配股率'])))
                    item['buy_price'] = float(self.clean(str(row['現金增資認購價'])))
                    item['money'] = float(self.clean(str(row['現金股利'])))
                    item['season'] = row['最近一次申報資料 季別/日期']
                    item['networth'] = float(self.clean(str(row['最近一次申報每股 (單位)淨值'])))
                    item['eps'] = float(self.clean(str(row['最近一次申報每股 (單位)盈餘'])))
                    self.InsDividendNotice(item)

            self.conn.commit()
            time.sleep(1)

dn = DividendNotice()
dn.execute()