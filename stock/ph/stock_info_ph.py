import requests
from bs4 import BeautifulSoup

class Equity(object):
    def getStockInfo(self):
        headers = {
            'Connection': 'Keep-Alive',
            'Accept': 'text/html, application/xhtml+xml, */*',
            'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'
        }

        url = 'https://www.marketwatch.com/investing/Stock/GLO?countryCode=PH'

        res = requests.get(url, headers=headers, allow_redirects=True)

        if len(res.text) > 0:
            soup = BeautifulSoup(res.text, 'html.parser')
            price_info = soup.find_all("div", {"class": "intraday__data"})
            price = price_info[0].find_all("span", {"class": "value"})[0].text
            print(price)

eq = Equity()
eq.getStockInfo()