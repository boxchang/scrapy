# -*- coding: utf-8 -*
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


    def execute(self):
        #url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/"+self.stock_no+"/ctrl/part/displaytype/4.phtml"
        url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/002916/ctrl/part/displaytype/4.phtml"
        res = requests.get(url)

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
        sql = "select * from stocklist"

        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        lists = cur.fetchall()

        return lists



sk = Stock()
lists = sk.getAllStock()

for stock in lists:
    scf = StockCashFlow(stock['stock_no'])
    scf.execute()
    time.sleep(2)