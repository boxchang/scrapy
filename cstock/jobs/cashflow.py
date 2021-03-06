# -*- coding: utf-8 -*
import sys
sys.path.append("..")
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
import MySQLdb
from database import database


class StockCashFlow(object):
    stock_no = ""

    def clean(self,str):
        result = "0"
        try:
            result = str.replace(",", "").replace("--", "0")
        except:
            pass
        return result

    def conn_close(self):
        self.conn.close()

    def __init__(self, stock_no):
        self.db = database()
        self.conn = self.db.create_connection()
        self.stock_no = stock_no

    def delete_cashflow(self,stock_no):
        sql = 'delete from cashflow where stock_no ={stock_no}'
        sql = sql.format(stock_no=stock_no)
        db = database()
        db.execute_sql(sql)

    def validate(self, stock_no):
        cur = self.conn.cursor()
        sql = "SELECT * FROM cashflow where stock_no = '{stock_no}' "
        sql = sql.format(stock_no=stock_no)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True

    def execute(self):
        if self.validate(self.stock_no):
            send_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
                "Connection": "keep-alive",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.8"}

            url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/"+self.stock_no+"/ctrl/part/displaytype/4.phtml"
            #url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/002916/ctrl/part/displaytype/4.phtml"
            print(url)
            res = requests.get(url, headers=send_headers)

            if res.text.find(u'报表日期') >0:
                self.delete_cashflow(self.stock_no)

                soup = BeautifulSoup(res.text, 'html.parser')

                print(soup)

                tables = soup.findAll('table')

                print(tables[13])
                tab = tables[13]
                inx = 1
                for tr in tab.tbody.findAll('tr')[2:]:
                    if len(tr.findAll('td')) == 6:
                        item = {}

                        value = [td.getText().encode('utf-8') for td in tr.findAll('td')]
                        #text = td.getText().encode('cp936') + '!'
                        print(value)
                        item['stock_no'] = self.stock_no
                        item['acc_name'] = value[0]
                        item['acc_no'] = 'A'+str(inx)
                        item['qa'] = self.clean(value[1])
                        item['qb'] = self.clean(value[2])
                        item['qc'] = self.clean(value[3])
                        item['qd'] = self.clean(value[4])
                        item['qe'] = self.clean(value[5])
                        self.InsStockData(item)
                    inx+=1
                self.conn.commit()

    def InsStockData(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into cashflow({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))

class Stock(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        self.db = database()
        self.conn = self.db.create_connection()

    def getAllStock(self):
        sql = "select * from stocklist where stock_no not in (select distinct stock_no from cashflow)"

        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        lists = cur.fetchall()

        return lists



sk = Stock()
lists = sk.getAllStock()

for stock in lists:
    scf = StockCashFlow(stock['stock_no'])
    scf.execute()
    time.sleep(10)