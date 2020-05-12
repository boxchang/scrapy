# -*- coding: utf-8 -*
import sys
sys.path.append("..")
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
import MySQLdb
from database import database


class Dividend(object):
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

    def delete_dividend(self,stock_no):
        sql = 'delete from dividend where stock_no ={stock_no}'
        sql = sql.format(stock_no=stock_no)
        db = database()
        db.execute_sql(sql)

    def validate(self, stock_no):
        cur = self.conn.cursor()
        sql = "SELECT * FROM dividend where stock_no = '{stock_no}' "
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

            #url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/600019.phtml"
            url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/"+self.stock_no+".phtml"
            print(url)
            res = requests.get(url, headers=send_headers)

            if res.text.find(u'分红') >0:
                self.delete_dividend(self.stock_no)

                soup = BeautifulSoup(res.text, 'html.parser')

                print(soup)

                dividend_table = soup.find('tbody')

                inx = 1
                for tr in dividend_table.findAll('tr'):
                    if len(tr.findAll('td')) == 9:
                        item = {}

                        value = [td.getText().encode('utf-8') for td in tr.findAll('td')]
                        #text = td.getText().encode('cp936') + '!'
                        print(value)
                        item['stock_no'] = self.stock_no
                        item['pdate'] = value[0]
                        item['stockg'] = self.clean(value[1])
                        item['stocka'] = self.clean(value[2])
                        item['money'] = self.clean(value[3])
                        item['gdate'] = value[5]
                        item['sdate'] = value[6]
                        self.InsDividendData(item)
                    inx+=1
                self.conn.commit()

    def InsDividendData(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into dividend({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))

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



sk = Stock()
lists = sk.getAllStock()

for stock in lists:
    scf = Dividend(stock['stock_no'])
    scf.execute()
    time.sleep(10)