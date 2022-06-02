import requests
from bs4 import BeautifulSoup
from stock.database import database

class Stock(object):
    stock_no = ""
    stock_name = ""
    stock_eps = 0

class EPS(object):
    year = 2021

    def execute(self):
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
        #url = 'https://mops.twse.com.tw/mops/web/t163sb04'
        request_headers = {
            'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36' \
                         '(KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36',
            'referer':'https://mops.twse.com.tw/',
        }
        form_data = {
            'TYPEK':'sii',
            'year': str(self.year-1911),
            'season':'04',
            'step':'1',
            'firstin':'true',
            'off':'1',
            'isQuery':'Y',
        }
        r = requests.post(url, headers=request_headers, data=form_data)
        soup = BeautifulSoup(r.text)
        tables = soup.find_all("table", {"class": "hasBorder"})
        for table in tables:
            ths = table.find_all('th')
            eps_index = len(ths)-1  # 最後一欄為EPS
            trs = table.find_all('tr')
            for tr in trs:
                tds = tr.find_all('td')
                if (len(tds)-1) == eps_index:
                    stock = Stock()
                    stock.stock_no = "00"+tds[0].text
                    stock.stock_name = tds[1].text
                    stock.stock_eps = tds[eps_index].text
                    self.insert_data(self.year, stock)

    def insert_data(self, year, stock):
        sql = """insert into EPS_2021(year,stock_no,stock_name,eps) 
                 Values('{year}', '{stock_no}', '{stock_name}', {eps})"""
        sql = sql.format(year=self.year, stock_no=stock.stock_no, stock_name=stock.stock_name, eps=stock.stock_eps)
        db = database()
        db.execute_sql(sql)

    def delete_data(self):
        sql = """delete from EPS_2021"""
        db = database()
        db.execute_sql(sql)


eps = EPS()
eps.delete_data()
eps.execute()