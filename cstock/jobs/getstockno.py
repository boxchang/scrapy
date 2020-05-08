# -*- coding: utf-8 -*
# coding=utf8
import sys
sys.path.append("..")
import requests
from bs4 import BeautifulSoup
import pandas as pd

from database import database


class StockList(object):

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

    def delete_stocklist(self, stock_no):
        sql = 'delete from stocklist where stock_no ={stock_no}'
        sql = sql.format(stock_no=stock_no)
        db = database()
        db.execute_sql(sql)

    def execute(self):


        url = "http://quote.eastmoney.com/stock_list.html"

        res = requests.get(url)
        print(res.encoding)  # 查看网页返回的字符集类型
        print(res.apparent_encoding)  # 自动判断字符集类型
        res.encoding = "gbk"

        if len(res.text) > 0:
            #self.delete_stocklist()

            soup = BeautifulSoup(res.text, 'html.parser')



            uls = soup.find(id='quotesearch').findAll('ul')


            for ul in uls:
                print(ul)
                inx = 1
                for a in ul.findAll('a'):
                    stock_no = a.text[a.text.find('(')+1:a.text.find('(')+7]
                    stock_name = a.text[:a.text.find('(')]
                    item = {}
                    item['stock_no'] = stock_no
                    item['stock_name'] = stock_name
                    self.InsStockData(item)
                    inx += 1
                    print(stock_no+'   '+stock_name)
            self.conn.commit()

    def InsStockData(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into stocklist({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))


scf = StockList()
scf.execute()