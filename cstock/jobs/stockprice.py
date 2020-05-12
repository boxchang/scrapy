# -*- coding: utf-8 -*
import sys
sys.path.append("..")
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
import MySQLdb
from database import database


class StockPrice(object):
    page = ""

    def clean(self,str):
        result = "0"
        try:
            result = str.replace(",", "").replace("--", "0")
        except:
            pass
        return result

    def conn_close(self):
        self.conn.close()

    def __init__(self,page):
        self.db = database()
        self.conn = self.db.create_connection()
        self.page = page

    def delete_stockprice(self,stock_no):
        sql = 'delete from stockprice where stock_no ={stock_no}'
        sql = sql.format(stock_no=stock_no)
        db = database()
        db.execute_sql(sql)
        self.conn.commit()
    def validate(self, stock_no):
        cur = self.conn.cursor()
        sql = "SELECT * FROM stockprice where stock_no = '{stock_no}' "
        sql = sql.format(stock_no=stock_no)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True

    def execute(self):
        send_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
            "Connection": "keep-alive",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8"}

        #url = "https://app.finance.ifeng.com/list/stock.php?t=ha&f=chg_pct&o=desc&p="+str(self.page)  #滬市A
        url = "https://app.finance.ifeng.com/list/stock.php?t=hs&f=chg_pct&o=desc&p="+str(self.page) #滬深A
        print(url)
        res = requests.get(url, headers=send_headers)

        if res.text.find(u'最新价') >0:


            soup = BeautifulSoup(res.text, 'html.parser')

            print(soup)

            dividend_table = soup.find('table')

            inx = 1
            for tr in dividend_table.findAll('tr'):
                if len(tr.findAll('td')) == 11:
                    item = {}

                    value = [td.getText().encode('utf-8') for td in tr.findAll('td')]
                    #text = td.getText().encode('cp936') + '!'
                    print(value)
                    item['stock_no'] = value[0]
                    item['stock_name'] = value[1]
                    item['cprice'] = self.clean(value[2])
                    self.delete_stockprice(value[0])
                    self.InsStockPriceData(item)
                inx+=1


    def InsStockPriceData(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into stockprice({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))
        self.conn.commit()
class Stock(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        self.db = database()
        self.conn = self.db.create_connection()

    def getAllStock(self):
        sql = "select * from stocklist where stock_no not in (select distinct stock_no from dividend)"

        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        lists = cur.fetchall()

        return lists



for page in range(75,76):
    scf = StockPrice(page)
    scf.execute()
    time.sleep(40)