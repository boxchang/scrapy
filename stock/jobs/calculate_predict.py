# -*- coding: utf-8 -*
import csv
import os
import sys
import time
sys.path.append("..")
import MySQLdb
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO

from stock.database import database

if sys.version_info < (3, 0):
    reload(sys)
    sys.setdefaultencoding('utf-8')

class PreDividend(object):
    stock_no = ""
    stock_name = ""
    price = 0
    season = ""
    cur_eps = 0
    cpr_eps = 0
    year = ""
    last_eps = 0
    last_moeny = 0
    last_stock = 0
    last_rate = 0
    near_eps = 0
    pre_div = 0
    pre_rate = 0

class stock_info(object):
    data_date = ""

    def __init__(self, data_date):
        self.data_date = data_date
        self.db = database()
        self.conn = self.db.create_connection()

    def clean(self, str):
        result = "0"
        try:
            result = str.replace(",", "").replace("--", "0")
        except:
            pass
        return result

    # 取得昨日上市收盤價
    def setStockPrice1(self, stockprice):
        url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + self.data_date + '&type=ALL'
        res = requests.get(url)

        if len(res.text) > 0:

            df = pd.read_csv(StringIO(res.text.replace("=", "")),
                             header=["證券代號" in l for l in res.text.split("\n")].index(True) - 1)

            for index, row in df.iterrows():
                if len(self.clean(row['證券代號'])) == 4:
                    stockprice[self.clean(row['證券代號']).zfill(6)] = (row[1], float(self.clean(row['收盤價'])))

        return stockprice

    # 取得昨日上櫃收盤價
    def setStockPrice2(self, stockprice):
        data_date = str(int(self.data_date[0:4])-1911) + "/" + str(self.data_date[4:6]) + "/" + str(self.data_date[6:8])
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_download.php?l=zh-tw&d={data_date}&s=0,asc,0'
        url = url.format(data_date = data_date)
        res = requests.get(url)

        if len(res.text) > 0:

            df = pd.read_csv(StringIO(res.text), header=2)

            for index, row in df.iterrows():
                if len(self.clean(row['代號'])) == 4:
                    if str(row[2]).strip() != "---":
                        stockprice[self.clean(row['代號']).zfill(6)] = (row[1], float(row[2]))

        return stockprice

    def getStockPrice(self):
        stockprice = {}
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = """SELECT * FROM (
                SELECT a.stock_no,a.stock_name,a.stock_eprice,b.season xx FROM 
                (SELECT * FROM stockprice a WHERE a.batch_no = {data_date}) a LEFT OUTER JOIN predividend b ON a.stock_no = b.stock_no ) aa
                WHERE xx IS null"""
        sql = sql.format(data_date=self.data_date)
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            stockprice[row["stock_no"]] = (row["stock_name"], float(row["stock_eprice"]))

        return stockprice

    def chkDataExisted(self, stock_no):
        result = False
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = """select * from predividend where stock_no='{stock_no}'"""
        sql = sql.format(stock_no=stock_no)
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) > 0:
            result = True

        return result

    def updateData(self, item):
        pass

    def insertData(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into predividend({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))
        self.conn.commit()


class dividend_predict(object):
    stock_no = ""


    def __init__(self, stock_no):
        self.stock_no = stock_no

    #去年同期EPS
    def getPredictEPS(self):
        url = 'https://histock.tw/stock/{stock_no}/%E6%AF%8F%E8%82%A1%E7%9B%88%E9%A4%98'
        url = url.format(stock_no = self.stock_no)

        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

        resp = requests.get(url, headers=headers)

        # 設定編碼為 utf-8 避免中文亂碼問題
        resp.encoding = 'utf-8'

        # 根據 HTTP header 的編碼解碼後的內容資料（ex. UTF-8），若該網站沒設定可能會有中文亂碼問題。所以通常會使用 resp.encoding 設定
        raw_html = resp.text

        # 將 HTML 轉成 BeautifulSoup 物件
        soup = BeautifulSoup(raw_html, 'html.parser')

        eps_list = []
        count = 0
        session = ""
        cQ4 = soup.select('.tb-outline > table > tr:nth-child(5) > td:nth-child(10)')[0].text
        if cQ4 != "-" and count < 4:
            eps_list.append(float(cQ4))
            session = "Q1"
            count += 1
        cQ3 = soup.select('.tb-outline > table > tr:nth-child(4) > td:nth-child(10)')[0].text
        if cQ3 != "-" and count < 4:
            eps_list.append(float(cQ3))
            session = "Q2"
            count += 1
        cQ2 = soup.select('.tb-outline > table > tr:nth-child(3) > td:nth-child(10)')[0].text
        if cQ2 != "-" and count < 4:
            eps_list.append(float(cQ2))
            session = "Q3"
            count += 1
        cQ1 = soup.select('.tb-outline > table > tr:nth-child(2) > td:nth-child(10)')[0].text
        if cQ1 != "-" and count < 4:
            eps_list.append(float(cQ1))
            session = "Q4"
            count += 1
        lQ4 = soup.select('.tb-outline > table > tr:nth-child(5) > td:nth-child(9)')[0].text
        if lQ4 != "-" and count < 4:
            eps_list.append(float(lQ4))
            session = "Q3"
            count += 1
        lQ3 = soup.select('.tb-outline > table > tr:nth-child(4) > td:nth-child(9)')[0].text
        if lQ3 != "-" and count < 4:
            eps_list.append(float(lQ3))
            session = "Q2"
            count += 1
        lQ2 = soup.select('.tb-outline > table > tr:nth-child(3) > td:nth-child(9)')[0].text
        if lQ2 != "-" and count < 4:
            eps_list.append(float(lQ2))
            session = "Q1"
            count += 1

        count_4Q = round(sum(eps_list),2)

        return session, count_4Q, count

    #今年累計EPS
    def getThisYearEPS(self, session):
        pass

    def validate(self, value):
        result = False
        if value != "-" and value != "0":
            result = True
        return result

    # 去年配息率
    def getLastYearDividendRate2(self):
        url = 'https://histock.tw/stock/{stock_no}/除權除息'
        url = url.format(stock_no=self.stock_no)

        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

        resp = requests.get(url, headers=headers)

        # 設定編碼為 utf-8 避免中文亂碼問題
        resp.encoding = 'utf-8'

        # 根據 HTTP header 的編碼解碼後的內容資料（ex. UTF-8），若該網站沒設定可能會有中文亂碼問題。所以通常會使用 resp.encoding 設定
        raw_html = resp.text

        # 將 HTML 轉成 BeautifulSoup 物件
        soup = BeautifulSoup(raw_html, 'html.parser')
        # soup.select('#divDetail > table > tr > td:nth-child(1) > nobr > b')[0].text

        # tr:nth-child(1) 第一列
        year = 0
        row_index = 2
        last_eps = 0
        money = 0
        stock = 0
        rate = 0
        try:
            while year == 0 and row_index <= 5:
                rate_tmp = soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(9)')[0].text.replace('%', '')
                if self.validate(rate_tmp):
                    year = soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(1)')[0].text
                    last_eps = soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(8)')[0].text
                    money = float(soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(7)')[0].text)
                    stock = float(soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(6)')[0].text)
                    rate = float(rate_tmp)
                else:
                    row_index += 1
        except:
            pass

        return year, last_eps, money, stock, rate


    #去年配息率
    def getLastYearDividendRate(self):
        url = 'https://goodinfo.tw/StockInfo/StockDividendPolicy.asp?STOCK_ID={stock_no}'
        url = url.format(stock_no=self.stock_no)

        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

        resp = requests.get(url, headers=headers)

        # 設定編碼為 utf-8 避免中文亂碼問題
        resp.encoding = 'utf-8'

        # 根據 HTTP header 的編碼解碼後的內容資料（ex. UTF-8），若該網站沒設定可能會有中文亂碼問題。所以通常會使用 resp.encoding 設定
        raw_html = resp.text

        # 將 HTML 轉成 BeautifulSoup 物件
        soup = BeautifulSoup(raw_html, 'html.parser')
        #soup.select('#divDetail > table > tr > td:nth-child(1) > nobr > b')[0].text

        #tr:nth-child(1) 第一列
        year = 0
        row_index = 2
        last_eps= 0
        money = 0
        stock = 0
        rate = 0
        while year == 0 and row_index <= 2:
            if soup.find('異常') > 0:
                print('您的瀏覽量異常, 已影響網站速度, 目前暫時關閉服務, 請稍後再重新使用, 若您是使用程式大量下載本網站資料, 請適當調降程式查詢頻率, 以維護一般使用者的權益')

            rate_tmp = soup.select('#divDetail > table > tr:nth-child('+str(row_index)+') > td:nth-child(24)')[0].text
            if self.validate(rate_tmp):
                year = soup.select('#divDetail > table > tr:nth-child('+str(row_index)+') > td:nth-child(1)')[0].text
                last_eps = soup.select('#divDetail > table > tr:nth-child('+str(row_index)+') > td:nth-child(21)')[0].text
                money = float(soup.select('#divDetail > table > tr:nth-child('+str(row_index)+') > td:nth-child(4)')[0].text)
                stock = float(soup.select('#divDetail > table > tr:nth-child('+str(row_index)+') > td:nth-child(7)')[0].text)
                rate = float(rate_tmp)
            else:
                row_index += 1

        return year, last_eps, money, stock, rate

    #預估今年配息
    def getPredictDividend(self):
        pass

    #目前股價配息率
    def getPreDividendRate(self):
        pass

# dp = dividend_predict("2206")
# year, last_eps, money, stock, rate = dp.getLastYearDividendRate2()


if sys.argv[1] > "":

    data_date = sys.argv[1]
    file_name = data_date + '_' + time.strftime("%H%M%S")
    header = ['代碼', '公司', '股價', '季', '配息年份', '去年EPS', '去年配息', '去年配股', '去年配息比例', '預估EPS', '預估今年配息', '目前股價配息率']

    #with open('predict/dividend_' + file_name + '.csv', 'w') as csvfile:
    # with open('predict/dividend_' + file_name + '.csv', 'w', newline='', encoding="utf-8") as csvfile:
    #     # 建立 CSV 檔寫入器
    #     writer = csv.writer(csvfile)
    #     # 寫入一列資料
    #     writer.writerow(header)

    si = stock_info(data_date)
    # si.setStockPrice1(stockprice)
    # print("上市公司筆數:" + str(len(stockprice)))
    # si.setStockPrice2(stockprice)
    # print("stock count:" + str(len(stockprice)))
    stockprice = si.getStockPrice()

    for stock_no in stockprice:
        item = {}
        prediv = PreDividend()
        # if stock_no != "001477":
        #     continue
        dp = dividend_predict(stock_no[2:])
        stock_name = stockprice[stock_no][0]
        stock_price = stockprice[stock_no][1]
        print("stock info:" + stock_no + " " + stock_name)
        year, last_eps, money, stock, rate = dp.getLastYearDividendRate2()

        item['stock_no'] = stock_no
        item['stock_name'] = stock_name
        item['price'] = stock_price
        item['year'] = year
        item['last_eps'] = last_eps
        item['last_money'] = money
        item['last_stock'] = stock
        item['last_rate'] = rate

        if float(rate) > 0: #分配率大於0的才收集
            season, near_eps, count = dp.getPredictEPS()
            if count == 4 and near_eps > 0:
                pre_dividend = round(near_eps * rate / 100,2)
                price_rate = round((pre_dividend / stock_price)*100, 2)
                #writer.writerow([stock_no[2:], stock_name, stock_price, session, year, last_eps, money, stock, rate, pre_eps, pre_dividend, price_rate])
                print(price_rate)
                item['season'] = season
                item['near_eps'] = near_eps
                item['pre_div'] = pre_dividend
                item['pre_rate'] = price_rate

        if si.chkDataExisted(stock_no):
            si.updateData(item)
        else:
            si.insertData(item)

        time.sleep(60)

        # csvfile.close()




