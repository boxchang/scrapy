# -*- coding: utf-8 -*
import requests
from bs4 import BeautifulSoup
import pandas as pd

url = 'https://goodinfo.tw/StockInfo/StockDividendPolicy.asp?STOCK_ID=3029'
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}

resp = requests.get(url, headers=headers)

# 設定編碼為 utf-8 避免中文亂碼問題
resp.encoding = 'utf-8'

# 根據 HTTP header 的編碼解碼後的內容資料（ex. UTF-8），若該網站沒設定可能會有中文亂碼問題。所以通常會使用 resp.encoding 設定
raw_html = resp.text

# 將 HTML 轉成 BeautifulSoup 物件
soup = BeautifulSoup(raw_html, 'html.parser')

print(soup.select('#divDetail > table > tr:nth-child(2) > td:nth-child(24)'))
