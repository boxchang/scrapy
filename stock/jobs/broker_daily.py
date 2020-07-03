import json
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
ua = UserAgent()

def getStockInfo(col1):
    stock_no = ""
    stock_name = ""
    if str(col1).find('GenLink2stk')>0:
        s = str(col1).find('GenLink2stk') + 15
        e = str(col1).find('GenLink2stk') + 19
        stock_no = str(col1)[s:e]
        s = str(col1).find('GenLink2stk') + 22
        e = str(col1).find('\');')
        stock_name = str(col1)[s:e]
    return stock_no,stock_name

def clean(str):
    result = "0"
    try:
        result = str.replace(",", "").replace("--", "0")
    except:
        pass
    return result

# headers = {
#     'User-Agent': ua.random
# }

headers = {
    'Connection': 'Keep-Alive',
    'Accept': 'text/html, application/xhtml+xml, */*',
    'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'
}

#url = 'https://www.nvesto.com/tpe/2345/majorForce#!/fromdate/2020-06-30/todate/2020-06-30/view/summary'
url = 'http://jsjustweb.jihsun.com.tw/z/zg/zgb/zgb0.djhtm?a=1030&b=1030&c=B&e=2020-7-2&f=2020-7-2'

res = requests.get(url,headers=headers,allow_redirects=True)

if len(res.text) > 0 :
    soup = BeautifulSoup(res.text, 'html.parser')
    table = soup.find("table", {"class": "t0"})
    rows = table.find_all('tr')
    data = []
    stock = []
    for row in rows[2:]:
        cols = row.find_all('td')
        stock_no, stock_name = getStockInfo(cols[0])
        cols[0] = stock_no
        cols[1] = clean(cols[1].text)
        cols[2] = clean(cols[2].text)
        cols[3] = clean(cols[3].text)
        cols.append(stock_name)

        #cols = [clean(ele.text) for ele in cols]
        data.append([ele for ele in cols if ele])  # Get rid of empty values
