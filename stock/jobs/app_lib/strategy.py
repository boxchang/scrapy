# -*- coding: utf-8 -*
import sys
sys.path.append("..")
import numpy
from stock.database import database
import MySQLdb

class StaticStrategy(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        db = database()
        self.conn = db.create_connection()

    # 靜態因素(第二階段再做，靜態因素主要是判斷旗標觸發後何時買進，現在先用人為判斷)
    # 市值<300億
    def CompanyStockValue(self, data_date, stock_no):
        result = ""
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select * from legalperson_price a where (a.stock_price * a.stock_num) < 30000000000 and batch_no = {data_date} and stock_no = {stock_no}"
        sql = sql.format(data_date=data_date, stock_no=stock_no)
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) > 0:
            result = "Y"
        else:
            result = "N"

        return result

    #年線乖離率
    #(旗標日收盤價 / 年線 -1 ) X 100%
    def Ma240_Flag_Gap(self, data_date, stock_no):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select distinct a.stock_name,(c.stock_eprice/b.avg_price-1)*100 result from stockflag a, stockprice_ma240 b, stockprice c " \
              "where a.stock_no = b.stock_no and a.stock_no ={stock_no} and a.stock_no = c.stock_no and c.batch_no = {data_date}"
        sql = sql.format(data_date=data_date, stock_no=stock_no)
        cur.execute(sql)
        result = round(cur.fetchone()['result'],2)
        return result

    #20日均線乖離率
    def Ma20_Flag_Gap(self, data_date, stock_no):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select distinct c.stock_name,(c.stock_eprice/b.avg_price-1)*100 result from stockprice_ma20 b, stockprice c " \
              "where b.stock_no ={stock_no} and b.stock_no = c.stock_no and c.batch_no = {data_date}"
        sql = sql.format(data_date=data_date, stock_no=stock_no)
        cur.execute(sql)
        result = round(cur.fetchone()['result'],2)
        return result

    #20日均線值
    def getMa20Value(self, stock_no):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select avg_price from stockprice_ma20 where stock_no = {stock_no}"
        sql = sql.format(stock_no=stock_no)
        cur.execute(sql)
        result = round(cur.fetchone()['avg_price'], 2)
        return result

    # 5日均線值
    def getMa5Value(self, stock_no):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select avg_price from stockprice_ma5 where stock_no = {stock_no}"
        sql = sql.format(stock_no=stock_no)
        cur.execute(sql)
        result = round(cur.fetchone()['avg_price'], 2)
        return result


    def calculate_SD(self, stock_no, MAt):

        price_sum = 0

        sql = "select stock_eprice from stockprice a where a.stock_no = {stock_no} order by batch_no desc limit 20"
        sql = sql.format(stock_no=stock_no)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        #print(sql)
        cur.execute(sql)
        lists = cur.fetchall()
        for item in lists:
            price_sum += numpy.square(float(item['stock_eprice'])-MAt)
        SDt = numpy.sqrt(price_sum/len(lists))

        return SDt

    #1年最大跌幅
    #資料量不足，尚未有年資料
    def Year_Flag_Gap(self):
        pass

class DynamicStrategy(object):
    today = ""

    def conn_close(self):
        self.conn.close()

    def __init__(self, today):
        db = database()
        self.conn = db.create_connection()
        self.today = today
    #券資比
    def Today_Financing_Percent(self, stock_no):
        sql = "select round((a.today_borrow_stock / a.today_borrow_money)*100,2) percent from financing a where a.stock_no = {stock_no} and a.data_date = {today}"
        sql = sql.format(stock_no=stock_no, today=self.today)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        result = cur.fetchone()['percent']
        return result

    #動態檢核表(何時賣出，何時加碼)
    #今日外資買超比率
    def Today_Foreign_Percent(self, stock_no):
        sql = "select percent from legalperson_price a where a.stock_no = {stock_no} and batch_no = {today}"
        sql = sql.format(stock_no=stock_no, today=self.today)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        result = cur.fetchone()['percent']
        return result

    #外資買超比率
    def Foreign_Percent(self, stock_no, flagDate):
        sql = "select round(sum(china_sum/stock_num*100),2) result from legalperson_price a where a.stock_no = {stock_no} and batch_no between {flagDate} and {today}"
        sql = sql.format(stock_no=stock_no, flagDate=flagDate, today=self.today)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()['result']
        return result

    #大盤漲幅
    #大盤漲幅 = (今日加權指數 / 旗標日的加權指數 - 1) X 100%
    def Taiex_Percent(self, flagDate):

        #取得旗標日加權
        sql = "select close_index from taiex a where a.data_date = {data_date}"
        sql = sql.format(data_date=flagDate)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        flag_value = cur.fetchone()['close_index']

        # 取得今日加權
        sql = "select close_index from taiex a order by data_date desc limit 0,1"
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        today_value = cur.fetchone()['close_index']

        result = round(((today_value/flag_value)-1)*100,2)

        return result

    #投信買超比率
    #投信買超比率 = 投資張數 / 公司股票張數 X 100%
    def Invest_Percent(self, stock_no, flagDate):
        sql = "select round(sum(invest_sum/stock_num*100),2) result from legalperson_price a where a.stock_no = {stock_no} and batch_no between {flagDate} and {today}"
        sql = sql.format(stock_no=stock_no, flagDate=flagDate, today=self.today)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()['result']
        return result

    #外資買超時間
    #買超時間>=一個月
    #旗標日開始計算外資買超一個月
    def Foreign_Day_Over1M(self, stock_no, flagDate):
        sum_value = 0
        total_value = 0
        sql = "select china_sum,stock_num from legalperson_price a where a.stock_no = {stock_no} and batch_no >= {flagDate}"
        sql = sql.format(stock_no=stock_no, flagDate=flagDate)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        lists = cur.fetchall()
        print(len(lists))

        for list in lists:
            sum_value += list['china_sum']
            total_value = list['stock_num']

        value = round(sum_value / total_value*100, 2)
        if len(lists) > 30 and value > 0:
            result = "超一個月"
        else:
            result = "未滿一個月"

        return result


    #創新高價(資料不足晚點做)
    # 創1年新高 = 旗標日至今日的最高價 > 前240日最高價
    def MostPrice(self, stock_no):
        sql = "select max(stock_eprice) max_price from stockprice a where a.stock_no = '{stock_no}' order by batch_no desc limit 240"
        sql = sql.format(stock_no=stock_no)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()['max_price']
        return result


    def CurrentPrice(self, data_date, stock_no):
        sql = "select stock_eprice from stockprice a where a.stock_no = '{stock_no}' and a.batch_no = {data_date}"
        sql = sql.format(stock_no=stock_no, data_date=data_date)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()['stock_eprice']
        return result


    #基本面(這部分看財報狗，關聯財報狗網址)
    #1.毛利上升
    #2.轉虧為盈
    #3.營收成長
    def BasicInformation(self):
        pass


    #編製合理漲幅區間機率表
    #基準股價 = 旗標日最低買價