# -*- coding: utf-8 -*
import MySQLdb
import sys
sys.path.append("..")
import datetime
from stock.database import database
import requests
import pandas as pd
from io import StringIO


class LegalPersonDay(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        self.db = database()
        self.conn = self.db.create_connection()

    def clean(self, str):
        result = "0"
        try:
            result = str.replace(",", "").replace("--", "0")
        except:
            pass
        return result

    def validate(self, data_date):
        cur = self.conn.cursor()
        sql = "SELECT * FROM legalperson_hist where data_date = '{data_date}' "
        sql = sql.format(data_date=data_date)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True

    def InsLegalPersonByDate(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into legalperson_hist({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))

    def DelLegalPersonByDate(self, data_date):
        cur = self.conn.cursor()
        sql = "delete from legalperson_hist where data_date = '" + data_date + "'"
        cur.execute(sql)

    def GetLegalPersonByPeriod(self, fromDate, toDate):

        for data_date in range(int(fromDate), int(toDate)):
            self.GetLegalPersonByDate(str(data_date))

    def GetLegalPersonByDate(self, data_date):

        #data_date = '20200422'
        url = 'http://www.tse.com.tw/fund/T86?response=csv&date='+data_date+'&selectType=ALLBUT0999'
        res = requests.get(url)

        if len(res.text) > 0 and self.validate(data_date):
            self.DelLegalPersonByDate(data_date)

            df = pd.read_csv(StringIO(res.text), header=1).dropna(how='all', axis=1).dropna(how='any')
            for index, row in df.iterrows():
                if len(self.clean(row['證券代號'])) == 4:
                    item = {}
                    item['data_date'] = data_date
                    item['stock_no'] = self.clean(row['證券代號']).zfill(6)
                    item['stock_name'] = self.clean(row['證券名稱'])
                    item['china_buy'] = float(self.clean(str(row['外陸資買進股數(不含外資自營商)'])))
                    item['china_sell'] = float(self.clean(str(row['外陸資賣出股數(不含外資自營商)'])))
                    item['china_sum'] = float(self.clean(str(row['外陸資買賣超股數(不含外資自營商)'])))
                    item['foreign_buy'] = float(self.clean(str(row['外資自營商買進股數'])))
                    item['foreign_sell'] = float(self.clean(str(row['外資自營商賣出股數'])))
                    item['foreign_sum'] = float(self.clean(str(row['外資自營商買賣超股數'])))
                    item['invest_buy'] = float(self.clean(str(row['投信買進股數'])))
                    item['invest_sell'] = float(self.clean(str(row['投信賣出股數'])))
                    item['invest_sum'] = float(self.clean(str(row['投信買賣超股數'])))
                    item['com_sum'] = float(self.clean(str(row['自營商買賣超股數'])))
                    item['legalperson'] = float(self.clean(str(row['三大法人買賣超股數'])))
                    self.InsLegalPersonByDate(item)

            self.conn.commit()

    def execute(self):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "SELECT data_date FROM taiex order by data_date"
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            for row in rows:
                data_date = row['data_date']

                if self.validate(data_date):
                    self.GetLegalPersonByDate(data_date)


lp = LegalPersonDay()
lp.execute()