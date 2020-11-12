# -*- coding: utf-8 -*
import requests
from bs4 import BeautifulSoup

class dividend_predict(object):

    #去年同期EPS
    def getLastYearEPS(self, year, session):
        url = 'https://histock.tw/stock/2330/%E6%AF%8F%E8%82%A1%E7%9B%88%E9%A4%98'

        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

        resp = requests.get(url, headers=headers)

        # 設定編碼為 utf-8 避免中文亂碼問題
        resp.encoding = 'utf-8'

        # 根據 HTTP header 的編碼解碼後的內容資料（ex. UTF-8），若該網站沒設定可能會有中文亂碼問題。所以通常會使用 resp.encoding 設定
        raw_html = resp.text

        # 將 HTML 轉成 BeautifulSoup 物件
        soup = BeautifulSoup(raw_html, 'html.parser')
        print(soup.select('.tb-outline > table > tr:nth-child(2) > td:nth-child(2)'))
        pass

    #今年累計EPS
    def getThisYearEPS(self, session):
        pass

    #去年配息率
    def getLastYearDividendRate(self):
        url = 'https://goodinfo.tw/StockInfo/StockDividendPolicy.asp?STOCK_ID=2330'

        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

        resp = requests.get(url, headers=headers)

        # 設定編碼為 utf-8 避免中文亂碼問題
        resp.encoding = 'utf-8'

        # 根據 HTTP header 的編碼解碼後的內容資料（ex. UTF-8），若該網站沒設定可能會有中文亂碼問題。所以通常會使用 resp.encoding 設定
        raw_html = resp.text

        # 將 HTML 轉成 BeautifulSoup 物件
        soup = BeautifulSoup(raw_html, 'html.parser')
        soup.select('#divDetail > table > tr > td:nth-child(1) > nobr > b')[0].text

        #tr:nth-child(1) 第一列
        print(soup.select('#divDetail > table > tr:nth-child(2) > td:nth-child(24)')[0].text)

        for i in range(1, 40):
            print(i, soup.select('#divDetail > table > tr:nth-child({i}) > td:nth-child(1) > nobr > b'))

    #預估今年配息
    def getPredictDividend(self):
        pass

    #目前股價配息率
    def getPreDividendRate(self):
        pass



dp = dividend_predict()
dp.getLastYearEPS("2020", "Q3")