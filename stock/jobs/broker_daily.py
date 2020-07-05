# -*- coding: utf-8 -*
import json
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from stock.database import database
import datetime
import MySQLdb
import time
ua = UserAgent()


class Broker(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        self.db = database()
        self.conn = self.db.create_connection()

    def DelBrokerData(self, data_date, broker_no):
        cur = self.conn.cursor()
        sql = "delete from broker_data where data_date = '{data_date}' and broker_no = '{broker_no}'"
        sql = sql.format(data_date=data_date, broker_no=broker_no)
        cur.execute(sql)


    def InsBrokerData(self, item):
        cur = self.conn.cursor()
        col = ','.join(item.keys())
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into broker_data({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        cur.execute(sql.format(col, placeholders), tuple(item.values()))

    def validate(self, data_date, broker_no):
        cur = self.conn.cursor()
        sql = "SELECT * FROM broker_data where data_date = '{data_date}' and broker_no = '{broker_no}' "
        sql = sql.format(data_date=data_date, broker_no=broker_no)
        cur.execute(sql)

        rows = cur.fetchall()

        if len(rows) > 0:
            return False
        else:
            return True

    def getBrokerData(self, broker_head, broker_no):
        data_date = datetime.date.today().strftime('%Y%m%d')
        now = datetime.date.today()
        year = str(int(now.strftime('%Y')))
        month = str(int(now.strftime('%m')))
        day = str(int(now.strftime('%d')))
        today = year + "-" + month + "-" + day
        today = '2020-7-3'
        data_date = '20200703'

        if self.validate(data_date,broker_no):
            #self.DelBrokerData(data_date, broker_no)

            # headers = {
            #     'User-Agent': ua.random
            # }

            headers = {
                'Connection': 'Keep-Alive',
                'Accept': 'text/html, application/xhtml+xml, */*',
                'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'
            }

            # url = 'https://www.nvesto.com/tpe/2345/majorForce#!/fromdate/2020-06-30/todate/2020-06-30/view/summary'
            url = 'http://jsjustweb.jihsun.com.tw/z/zg/zgb/zgb0.djhtm?a={broker_head}&b={broker_no}&c=B&e={today}&f={today}'
            url = url.format(broker_head=broker_head, broker_no=broker_no, today=today)

            res = requests.get(url, headers=headers, allow_redirects=True)

            if len(res.text) > 0:
                soup = BeautifulSoup(res.text, 'html.parser')
                table = soup.find_all("table", {"class": "t0"})
                buy_table = table[0]
                sell_table = table[1]

                #買超
                rows = buy_table.find_all('tr')
                data = []
                stock = []
                for row in rows[2:]:
                    cols = row.find_all('td')
                    stock_no, stock_name = self.getStockInfo(cols[0])
                    # cols[0] = stock_no
                    # cols[1] = self.clean(cols[1].text)
                    # cols[2] = self.clean(cols[2].text)
                    # cols[3] = self.clean(cols[3].text)
                    # cols.append(stock_name)

                    item = {}
                    item['data_date'] = data_date
                    item['broker_no'] = broker_no
                    item['stock_no'] = stock_no.zfill(6)
                    item['buy'] = self.clean(cols[1].text)
                    item['sell'] = self.clean(cols[2].text)
                    item['diff'] = self.clean(cols[3].text)
                    self.InsBrokerData(item)

                #賣超
                rows = sell_table.find_all('tr')
                data = []
                stock = []
                for row in rows[2:]:
                    cols = row.find_all('td')
                    stock_no, stock_name = self.getStockInfo(cols[0])
                    # cols[0] = stock_no
                    # cols[1] = self.clean(cols[1].text)
                    # cols[2] = self.clean(cols[2].text)
                    # cols[3] = self.clean(cols[3].text)
                    # cols.append(stock_name)

                    item = {}
                    item['data_date'] = data_date
                    item['broker_no'] = broker_no
                    item['stock_no'] = stock_no.zfill(6)
                    item['buy'] = self.clean(cols[1].text)
                    item['sell'] = self.clean(cols[2].text)
                    item['diff'] = self.clean(cols[3].text)
                    self.InsBrokerData(item)

                    # cols = [clean(ele.text) for ele in cols]
                    # data.append([ele for ele in cols if ele])  # Get rid of empty values

    def getStockInfo(self, col1):
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

    def clean(self, str):
        result = "0"
        try:
            result = str.replace(",", "").replace("--", "0")
        except:
            pass
        return result

    def getBorkerList(self):

        db = database()
        self.conn = db.create_connection()
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)

        sql = "select broker_head,broker_no,broker_name from broker_list a where enable is null"
        cur.execute(sql)
        rows = cur.fetchall()

        return rows

    def execute(self):
        brokers = self.getBorkerList()
        for broker in brokers:
            broker_head = broker['broker_head']
            broker_no = broker['broker_no']
            self.getBrokerData(broker_head,broker_no)
            self.conn.commit()
            time.sleep(5)
        self.conn_close()


br = Broker()
br.execute()

