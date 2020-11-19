# -*- coding: utf-8 -*
import csv
import datetime
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
                SELECT a.stock_no,a.stock_name,a.stock_eprice,b.price xx FROM 
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

    def clean(self, str):
        result = "0"
        try:
            result = str.replace(",", "").replace("-", "0").replace("--", "0")
        except:
            pass
        return result

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
        season = ""
        cur_eps = 0
        cpr_eps = 0
        cpr_rate = 0
        count_4Q = 0

        try:
            index = 10
            l_index = 9
            while(soup.select('.tb-outline > table > tr:nth-child(2) > td:nth-child('+str(index)+')')[0].text == "-"):
                index-=1
                l_index-=1

            cQ4 = soup.select('.tb-outline > table > tr:nth-child(5) > td:nth-child('+str(index)+')')[0].text
            if cQ4 != "-" and count < 4:
                eps_list.append(float(cQ4))
                season = "Q1"
                count += 1
            cQ3 = soup.select('.tb-outline > table > tr:nth-child(4) > td:nth-child('+str(index)+')')[0].text
            if cQ3 != "-" and count < 4:
                eps_list.append(float(cQ3))
                season = "Q2"
                count += 1
            cQ2 = soup.select('.tb-outline > table > tr:nth-child(3) > td:nth-child('+str(index)+')')[0].text
            if cQ2 != "-" and count < 4:
                eps_list.append(float(cQ2))
                season = "Q3"
                count += 1
            cQ1 = soup.select('.tb-outline > table > tr:nth-child(2) > td:nth-child('+str(index)+')')[0].text
            if cQ1 != "-" and count < 4:
                eps_list.append(float(cQ1))
                season = "Q4"
                count += 1
            lQ4 = soup.select('.tb-outline > table > tr:nth-child(5) > td:nth-child('+str(l_index)+')')[0].text
            if lQ4 != "-" and count < 4:
                eps_list.append(float(lQ4))
                season = "Q3"
                count += 1
            lQ3 = soup.select('.tb-outline > table > tr:nth-child(4) > td:nth-child('+str(l_index)+')')[0].text
            if lQ3 != "-" and count < 4:
                eps_list.append(float(lQ3))
                season = "Q2"
                count += 1
            lQ2 = soup.select('.tb-outline > table > tr:nth-child(3) > td:nth-child('+str(l_index)+')')[0].text
            if lQ2 != "-" and count < 4:
                eps_list.append(float(lQ2))
                season = "Q1"
                count += 1
            lQ1 = soup.select('.tb-outline > table > tr:nth-child(2) > td:nth-child('+str(l_index)+')')[0].text
            if lQ1 != "-" and count < 4:
                eps_list.append(float(lQ1))
                season = "Q4"
                count += 1

            if season == "Q1":
                cpr_eps = round(float(lQ1), 2)
                cur_eps = round(float(cQ1), 2)
                cpr_rate = round(((cur_eps/cpr_eps)-1)*100, 2)
            if season == "Q2":
                cpr_eps = round(float(lQ1)+float(lQ2), 2)
                cur_eps = round(float(cQ1)+float(cQ2), 2)
                cpr_rate = round(((cur_eps/cpr_eps)-1)*100, 2)
            if season == "Q3":
                cpr_eps = round(float(lQ1)+float(lQ2)+float(lQ3), 2)
                cur_eps = round(float(cQ1)+float(cQ2)+float(cQ3), 2)
                cpr_rate = round(((cur_eps/cpr_eps)-1)*100, 2)
            if season == "Q4":
                cpr_eps = round(float(lQ1)+float(lQ2)+float(lQ3)+float(lQ4), 2)
                cur_eps = round(float(cQ1)+float(cQ2)+float(cQ3)+float(cQ4), 2)
                cpr_rate = round(((cur_eps/cpr_eps)-1)*100, 2)


            count_4Q = round(sum(eps_list),2)
        except:
            pass

        return season, count_4Q, count, cur_eps, cpr_eps, cpr_rate

    #今年累計EPS
    def getThisYearEPS(self, season):
        pass

    def validate(self, value):
        result = False
        if value != "-" and value != "0":
            result = True
        return result

    # 取得三年平均配息率
    def getAveDividendRate(self, soup):
        row_index = 1
        rate = 0
        data = []
        total = 0

        # try:
        year = soup.select(
            '.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(row_index) + ') > td:nth-child(1)')[
            0].text
        print(year)

        last_year = str(int(datetime.datetime.now().strftime('%Y'))-1)
        if year.find(last_year) >= 0:
            rate_tmp = soup.select(
                '.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(row_index) + ') > td:nth-child(9)')[
                0].text.replace('%', '')
            total = float(rate_tmp)

            while (True):
                row_index += 1
                try:
                    nyear = soup.select('.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(
                        row_index) + ') > td:nth-child(1)')[0].text
                    nrate_tmp = soup.select('.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(
                        row_index) + ') > td:nth-child(9)')[0].text.replace('%', '')
                except:
                    nyear = 0
                    nrate_tmp = 0

                if year == nyear:
                    total += float(nrate_tmp)
                else:
                    data.append(total)
                    if nyear == 0:
                        break
                    else:
                        year = nyear

                    if len(data) == 3:
                        break

                    if nrate_tmp == "-":
                        break
                    else:
                        total = float(nrate_tmp)

            # except:
            #     pass

            rate = round(sum(data) / len(data), 2)

        return rate

    # 取得去年配息
    def getLastDividend(self, soup):
        row_index = 1

        # try:
        year = soup.select('.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(row_index) + ') > td:nth-child(1)')[0].text

        last_year = str(int(datetime.datetime.now().strftime('%Y')) - 1)
        if year.find(last_year) >= 0:
            ndiv_stock = soup.select('.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(
                row_index) + ') > td:nth-child(6)')[0].text.replace('%', '')
            ndiv_money = soup.select('.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(
                row_index) + ') > td:nth-child(7)')[0].text.replace('%', '')

            stock_total = float(ndiv_stock)
            money_total = float(ndiv_money)

            while (True):
                row_index += 1
                try:
                    nyear = soup.select('.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(
                        row_index) + ') > td:nth-child(1)')[0].text
                    ndiv_stock = soup.select('.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(
                        row_index) + ') > td:nth-child(6)')[0].text.replace('%', '')
                    ndiv_money = soup.select('.tb-outline > table > tr:nth-child(2) > tr:nth-child(' + str(
                        row_index) + ') > td:nth-child(7)')[0].text.replace('%', '')

                except:
                    nyear = 0

                if year == nyear:
                    money_total += float(ndiv_money)
                    stock_total += float(ndiv_stock)
                else:
                    break

            # except:
            #     pass
        return money_total, stock_total


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

        if resp.text.find("無配發現金股利與股票股利資料") == -1:
            while year == 0 and row_index <= 5:
                rate_tmp = self.getAveDividendRate(soup)
                #rate_tmp = soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(9)')[0].text.replace('%', '')
                year = soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(1)')[0].text
                last_eps = self.clean(soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(8)')[0].text)
                #money = float(soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(7)')[0].text)
                #stock = float(soup.select('.tb-outline > table > tr:nth-child('+str(row_index)+') > tr > td:nth-child(6)')[0].text)
                money, stock = self.getLastDividend(soup)
                rate = float(rate_tmp)

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


        # if stock_no != "003188":
        #     continue

        dp = dividend_predict(stock_no[2:])
        stock_name = stockprice[stock_no][0]
        stock_price = stockprice[stock_no][1]
        print("stock info:" + stock_no + " " + stock_name)
        year, last_eps, money, stock, rate = dp.getLastYearDividendRate2()

        #只看今年有配息的，太久沒配息的就不看了
        last_year = str(int(datetime.datetime.now().strftime('%Y')) - 1)
        if year.find(last_year) >= 0:

            item['stock_no'] = stock_no
            item['stock_name'] = stock_name
            item['price'] = stock_price
            item['year'] = year
            item['last_eps'] = last_eps
            item['last_money'] = money
            item['last_stock'] = stock
            item['last_rate'] = rate

            if float(rate) > 0 and stock_price > 0: #分配率大於0的才收集
                season, near_eps, count, cur_eps, cpr_eps, cpr_rate = dp.getPredictEPS()
                if count == 4 and near_eps > 0:
                    pre_dividend = round(near_eps * rate / 100,2)
                    price_rate = round((pre_dividend / stock_price)*100, 2)
                    #writer.writerow([stock_no[2:], stock_name, stock_price, season, year, last_eps, money, stock, rate, pre_eps, pre_dividend, price_rate])
                    print(price_rate)
                    item['season'] = season
                    item['near_eps'] = near_eps
                    item['pre_div'] = pre_dividend
                    item['pre_rate'] = price_rate
                    item['cur_eps'] = cur_eps
                    item['cpr_eps'] = cpr_eps
                    item['cpr_rate'] = cpr_rate

            if si.chkDataExisted(stock_no):
                si.updateData(item)
            else:
                si.insertData(item)

        time.sleep(30)

        # csvfile.close()




