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
import sys
reload(sys)
sys.setdefaultencoding('utf8')

class PolicyTest(object):

    def conn_close(self):
        self.conn.close()

    def __init__(self,table):
        db = database()
        self.conn = db.create_connection()
        self.create_table(table)

    def build_dict(self, seq, key):
        return dict((d[key], dict(d, index=i)) for (i, d) in enumerate(seq))

    def create_table(self,table):
        TABLE = "CREATE TABLE if not exists {table} (ikey int ,data_date VARCHAR(10) NOT NULL,stock_no VARCHAR(10) NOT NULL,stock_name VARCHAR(60) NOT NULL,stock_lprice FLOAT NOT NULL,close_index FLOAT NOT NULL,actual_price FLOAT,price90 FLOAT,price80 FLOAT,price70 FLOAT,price50 FLOAT,final FLOAT,final_datadate VARCHAR(10),sellout FLOAT,mgap FLOAT,enable VARCHAR(1), created_date TimeStamp DEFAULT CURRENT_TIMESTAMP, updated_date TimeStamp)"
        TABLE = TABLE.format(table=table)
        db = database()
        db.execute_sql(TABLE)

    def isStockExisted(self,table, stock_no):
        cur = self.conn.cursor()
        sql = "SELECT * FROM {table} where stock_no = '{stock_no}'"
        sql = sql.format(table=table, stock_no=stock_no)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return True
        else:
            return False

    def isFlagExisted(self, table, stock_no):
        cur = self.conn.cursor()
        sql = "SELECT * FROM {table} where stock_no = '{stock_no}' and enable is null"
        sql = sql.format(table=table, stock_no=stock_no)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return True
        else:
            return False

    #參數一 股票代號
    #參數二 多少買超比例觸發旗標
    #參數三 只盯外資或三大法人
    #總股數是取stockholder最新資料，會有點失真，但不影響整體推演
    def analyze(self, stock_no, percent, type, table):
        price = self.getPrice(stock_no)
        price = self.build_dict(price, key="batch_no")
        stock_lprice = 0
        ikey = 1
        count = 0
        sql = "select a.data_date,a.stock_no,a.stock_name,china_sum,invest_sum,com_sum,stock_num from legalperson_hist a,stockholder b " \
              "where a.stock_no={stock_no} and a.stock_no = b.stock_no and b.level = 17 order by data_date"
        sql = sql.format(stock_no=stock_no)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            for row in rows:
                data_date = row['data_date']
                stock_no = row['stock_no']
                stock_name = str(row['stock_name']).encode('utf-8').strip()
                foreign = row['china_sum']
                invest = row['invest_sum']
                com = row['com_sum']
                total = row['stock_num']

                if foreign>0:
                    count += round(foreign / total * 100,2)
                else:
                    count = 0

                if count > percent:
                    if not self.isFlagExisted(table,stock_no):
                        stock_lprice = price[data_date]['stock_lprice']
                        self.insFlag(table,ikey,data_date, stock_no, stock_name, count, stock_lprice)
                        ikey+=1
                else:
                    if count == 0:
                        self.closeFlag(table,stock_no)

                # else:
                #     if self.isFlagExisted(table, stock_no):
                #         self.updFlag(table, stock_no, count, data_date, stock_lprice)

        self.conn_close()


    def insFlag(self, table,ikey,data_date, stock_no, stock_name,count,lprice):
        cur = self.conn.cursor()
        sql = "insert into {table} (ikey,data_date,stock_no,stock_name,stock_lprice,final,close_index) values({ikey},{data_date},'{stock_no}','{stock_name}',{lprice},{final},0)"
        sql = sql.format(table=table,ikey=ikey,stock_no=stock_no,stock_name=stock_name,data_date=data_date,final=count,lprice=lprice)
        print(sql)
        cur.execute(sql)
        self.conn.commit()

    def updFlag(self, table,stock_no,count,final_datadate,stock_lprice):
        cur = self.conn.cursor()
        sql = "update {table} set final={count},final_datadate={final_datadate},sellout={stock_lprice},mgap=stock_lprice-{stock_lprice},enable='N' where stock_no = '{stock_no}' and enable is null"
        sql = sql.format(table=table,stock_no=stock_no,count=count,final_datadate=final_datadate,stock_lprice=stock_lprice)
        cur.execute(sql)
        self.conn.commit()

    def closeFlag(self,table,stock_no):
        cur = self.conn.cursor()
        sql = "update {table} set enable='N' where stock_no = '{stock_no}' and enable is null"
        sql = sql.format(table=table, stock_no=stock_no)
        cur.execute(sql)
        self.conn.commit()

    def getPrice(self, stock_no):
        result = 0

        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select batch_no,stock_lprice from stockprice where stock_no={stock_no}"
        sql = sql.format(stock_no=stock_no)
        #print(sql)
        cur.execute(sql)
        result = cur.fetchall()
       #print(result)

        return result

class FlagPolicyTest(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        db = database()
        self.conn = db.create_connection()

    def isFlagExisted(self, table, stock_no, ikey):
        cur = self.conn.cursor()
        sql = "SELECT * FROM {table} where stock_no = '{stock_no}' and ikey={ikey}"
        sql = sql.format(table=table, stock_no=stock_no,ikey=ikey)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return True
        else:
            return False

    def updFlag(self, table,stock_no,count,final_datadate,stock_lprice,ikey):
        cur = self.conn.cursor()
        sql = "update {table} set final={count},final_datadate={final_datadate},sellout={stock_lprice},mgap={stock_lprice}-stock_lprice where stock_no = '{stock_no}' and ikey={ikey}"
        sql = sql.format(table=table,stock_no=stock_no,count=count,final_datadate=final_datadate,stock_lprice=stock_lprice,ikey=ikey)
        cur.execute(sql)
        self.conn.commit()


    def getPrice(self, stock_no):
        result = 0

        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select batch_no,stock_lprice from stockprice where stock_no={stock_no}"
        sql = sql.format(stock_no=stock_no)
        #print(sql)
        cur.execute(sql)
        result = cur.fetchall()
       #print(result)

        return result

    def build_dict(self, seq, key):
        return dict((d[key], dict(d, index=i)) for (i, d) in enumerate(seq))

    def getFlagData(self,table,stock_no):
        price = self.getPrice(stock_no)
        price = self.build_dict(price, key="batch_no")

        sql = "select ikey,data_date,stock_lprice from {table} where stock_no={stock_no}"
        sql = sql.format(table=table,stock_no=stock_no)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            for row in rows:
                print('---------------------------------------------')
                count_day = 0
                sql2 = "select a.data_date,a.stock_no,a.stock_name,china_sum,invest_sum,com_sum,stock_num from legalperson_hist a,stockholder b " \
                      "where a.stock_no={stock_no} and a.stock_no = b.stock_no and b.level = 17 and a.data_date > {data_date} order by data_date"
                sql2 = sql2.format(stock_no=stock_no,data_date=row['data_date'])
                cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
                cur.execute(sql2)

                rows2 = cur.fetchall()

                count = 0
                if len(rows2) > 0:
                    for row2 in rows2:
                        ikey = row['ikey']
                        data_date = row2['data_date']
                        stock_no = row2['stock_no']
                        stock_name = str(row2['stock_name']).encode('utf-8').strip()
                        foreign = row2['china_sum']
                        invest = row2['invest_sum']
                        com = row2['com_sum']
                        total = row2['stock_num']
                        lprice = row['stock_lprice']


                        cprice = price[data_date]['stock_lprice']

                        count += foreign / total * 100

                        percent = round(((cprice/lprice)-1)*100,2)

                        count_day+=1

                        if count_day > 30:
                            isMon = "Over one Month"
                        else:
                            isMon = "less one Month"

                        if lprice < cprice:
                            k = "Win"
                        else:
                            k = "Lose"

                        if count < 0:
                            if self.isFlagExisted(table, stock_no, ikey):
                                self.updFlag(table, stock_no, count, data_date, cprice,ikey)
                            print(data_date,"   ",round(count,2),"    ","Over")
                            print('---------------------------------------------')
                            break
                        else:
                            print(data_date, "   holder", round(count,2),"%  ",lprice,"    ",cprice, "   ",k, " ",percent,"%  "+isMon)
        self.conn_close()

table = 't1p5f'
stock_no = '002317'
percent = 1.5
pt = PolicyTest(table)
if not pt.isFlagExisted(table,stock_no):
    pt.analyze(stock_no,percent,'f',table) #外資比例超過1.5觸發旗標

    fp = FlagPolicyTest()
    fp.getFlagData(table,stock_no)


# table = 't3p0f'
# stock_no = '002345'
# percent = 3
# pt = PolicyTest(table)
# if not pt.isFlagExisted(table,stock_no):
#     pt.analyze(stock_no,percent,'f',table) #外資比例超過0.5觸發旗標
#
#     fp = FlagPolicyTest()
#     fp.getFlagData(table,stock_no)


# table = 't0p1f'
# stock_no = '002330'
# percent = 0.1
# pt = PolicyTest(table)
# if not pt.isStockExisted(table,stock_no):
#     pt.analyze(stock_no,percent,'f',table) #外資比例超過0.1觸發旗標
#
# fp = FlagPolicyTest()
# fp.getFlagData(table,stock_no)