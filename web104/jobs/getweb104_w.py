#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append("..")
import pandas as pd
import re, time, requests
from selenium import webdriver
from bs4 import BeautifulSoup
import json
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from web104.database import database

# 加入使用者資訊(如使用什麼瀏覽器、作業系統...等資訊)模擬真實瀏覽網頁的情況
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}

# 2007000000  資訊軟體系統類
# 2001000000  經營/人資類
# 2001002000  人力資源類人員

# 6001016000	高雄市
# 6001008000	台中市
# 6001001000	台北市
# 6001002000	新北市
# 6001006000	新竹縣市
# 6003000000    其他亞洲
# 6002000000	大陸地區

# scmin=50000 最低薪資50000

# isnew=3 三日最新 isnew=0 本日最新

my_params = {'ro': '0',  # 限定全職的工作，如果不限定則輸入0
             'jobcat': '2007000000',  # 想要查詢的關鍵字
             'area': '6001016000,6001002000,6001001000,6001008000,6001006000',
             'isnew': '0',
             'mode': 'l'}  # 清單的瀏覽模式
#https://www.104.com.tw/jobs/search/list?ro=0&jobcat=2007000000&isnew=0&area=6001016000%2C6001002000%2C6001001000%2C6001008000%2C6001006000&order=11&asc=0&sctp=M&scmin=50000&scstrict=1&scneg=0&page=1&mode=s&jobsource=2018indexpoc

url = requests.get('https://www.104.com.tw/jobs/search/?', my_params, headers=headers).url

#開啟Chrome於前端顯示
#driver = webdriver.Chrome()

#在背景執行
chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--headless')
driver = webdriver.Chrome(chrome_options=chrome_options) #若這行有問題去下載chromedriver放置/usr/local/bin
driver.get(url)

# 網頁的設計方式是滑動到下方時，會自動加載新資料，在這裡透過程式送出Java語法幫我們執行「滑到下方」的動作
for i in range(20):
    driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
    time.sleep(0.6)

# 自動加載只會加載15次，超過之後必須要點選「手動載入」的按鈕才會繼續載入新資料（可能是防止爬蟲）
k = 1
while k != 0:
    try:
        # 手動載入新資料之後會出現新的more page，舊的就無法再使用，所以要使用最後一個物件
        driver.find_elements_by_class_name("js-more-page", )[-1].click()
        # 如果真的找不到，也可以直接找中文!
        # driver.find_element_by_xpath("//*[contains(text(),'手動載入')]").click()
        print('Click 手動載入，' + '載入第' + str(15 + k) + '頁')
        k = k + 1
        time.sleep(1)  # 時間設定太短的話，來不及載入新資料就會跳錯誤
    except:
        k = 0
        print('No more Job')

# 透過BeautifulSoup解析資料
soup = BeautifulSoup(driver.page_source, 'html.parser')
List = soup.findAll('a', {'class': 'js-job-link'})
print('共有 ' + str(len(List)) + ' 筆資料')


def bind(cate):
    k = []
    for i in cate:
        if len(i.text) > 0:
            k.append(i.text)
    return str(k)

db = database()
conn = db.create_connection()
i = 0


def insertTable(item, conn):
    cursor = conn.cursor()

    col = ','.join(item.keys())
    # placeholders = ','.join(len(item) * '?')
    placeholders = ("%s," * len(item))[:-1]
    sql = 'insert into web104({}) values({})'
    print(sql.format(col, placeholders), tuple(item.values()))
    cursor.execute(sql.format(col, placeholders), tuple(item.values()))
    conn.commit()

def validate(jobNo,conn):

    cur = conn.cursor()
    cur.execute("SELECT * FROM web104 where jobNo = '" + jobNo + "'")

    rows = cur.fetchall()

    if len(rows) > 0:
        return False
    else:
        return True


while i < len(List):
    # print('正在處理第' + str(i) + '筆，共 ' + str(len(List)) + ' 筆資料')
    content = List[i]
    # 這裡用Try的原因是，有時候爬太快會遭到系統阻擋導致失敗。因此透過這個方式，當我們遇到錯誤時，會重新再爬一次資料！
    #content.attrs['href'].strip('//') => www.104.com.tw/job/5cmwg?jobsource=hotjob_chr
    #www.104.com.tw/job/ajax/similarJobs/5cmwg
    batchNo = time.strftime("%Y%m%d%H%M%S", time.localtime())
    url = 'https://' + content.attrs['href'].strip('//')
    if url.find("www.104.com.tw") > 0:
        try:
            jobNo = url[url.rfind("/") + 1:url.rfind("?")]
            ajax_url = "https://www.104.com.tw/job/ajax/content/" + jobNo
            res = requests.get(ajax_url)
            job = json.loads(res.text)
            item = {}
            item['custName'] = job['data']['header']['custName']
            item['jobNo'] = jobNo
            item['jobName'] = job['data']['header']['jobName']
            item['description'] = job['data']['jobDetail']['jobDescription']
            item['jobAddrNoDesc'] = job['data']['jobDetail']['addressRegion']
            item['jobLink'] = url
            item['addr'] = job['data']['jobDetail']['addressDetail']
            item['history'] = job['data']['condition']['workExp']
            item['tool'] = ''
            item['other'] = job['data']['condition']['other']
            item['benefit'] = job['data']['welfare']['welfare']
            item['update_date'] = job['data']['header']['appearDate']
            item['batchNo'] = batchNo
            print(item)

            if validate(jobNo,conn):
                insertTable(item,conn)
        except:
            pass

        i += 1
        print("Success and Crawl Next 目前正在爬第" + str(i) + "個職缺資訊")
    else:
        i += 1
        continue

    time.sleep(0.5)  # 執行完休息0.5秒，避免造成對方主機負擔


conn.close()

