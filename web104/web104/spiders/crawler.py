# -*- coding:utf-8 -*-
import json
import logging
import sqlite3
import time

import requests
import scrapy
from bs4 import BeautifulSoup
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule

from web104.database import database
from web104.items import Web104Item



class Web104(scrapy.Spider):
    name = 'web104'
    allowed_domain = ['www.104.com.tw']
    batchNo = time.strftime("%Y%m%d%H%M%S", time.localtime())

    # 2007000000  資訊軟體系統類    6002000000  大陸地區
    # 2001000000  經營/人資類
    # 2001002000  人力資源類人員

    start_urls = [
        'https://www.104.com.tw/jobs/search/list?ro=0&jobcat=2007000000&area=6003000000%2C6002000000&order=11&asc=0&page=1&mode=s&jobsource=2018indexpoc&isnew=3&sctp=M&scmin=50000&scstrict=1&scneg=0',
        'https://www.104.com.tw/jobs/search/list?ro=0&jobcat=2007000000&area=6001016000%2C6001018000%2C6001011000%2C6001012000%2C6001016000&order=11&asc=0&page=1&mode=s&jobsource=2018indexpoc&isnew=3&sctp=M&scmin=50000&scstrict=1&scneg=0',
    ]
    # start_urls = [
    #     'https://www.104.com.tw']

    # 獲取匹配分頁頁碼的鏈接的正則表達式
    # page_link = LinkExtractor(canonicalize=True, unique=True)
    #
    # rules = (
    #     Rule(page_link, callback='parse_items', follow=True),
    # )

    # 使用了rules，這段就省略了
    def start_requests(self):
        for url in self.start_urls:
            page = 1 # 變數初始化
            res = requests.get(url)
            data = res.json()
            totalPage = data['data']['totalPage']  # 取得總分頁數量

            # 取得每個分頁內容
            for page in range(1, int(totalPage)):
                url = url.replace('page=1', 'page='+str(page))
                yield scrapy.Request(url, callback=self.parse, dont_filter=False)

    def parse(self, response):
        jobs = json.loads(response.body_as_unicode())
        items = []
        #print(jobs['data'])
        for job in jobs['data']['list']:
            item = Web104Item()
            item['custName'] = job['custName']
            item['jobNo'] = job['jobNo']
            item['jobName'] = job['jobName']
            item['description'] = job['description']
            item['jobAddrNoDesc'] = job['jobAddrNoDesc']
            item['jobLink'] = job['link']['job'][2:]
            job_url = 'http://'+job['link']['job'][2:]
            print(job_url)
            logging.info('job_url:'+job_url)

            if self.validate(item['jobNo']):
                yield scrapy.Request(job_url, meta={'item': item}, callback=self.parse_detail)


    def parse_detail(self, response):
        item = response.meta['item']
        res = BeautifulSoup(response.xpath('//*[@id="job"]/article').extract()[0])

        # 工作地點
        tag = res.select('.addr')[0]
        item['addr'] = str(tag.text).replace('地圖找工作', '').strip()

        # 工作經歷
        tag = res.select('.content')[1].select('dd')[1]
        item['history'] = tag.text

        # 擅長工具
        tag = res.select('.content')[1].select('dd')[5]
        item['tool'] = tag.text

        # 其他
        tag = res.select('.content')[1].select('dd')[7]
        item['other'] = tag.text

        # 福利
        tag = res.select('.content')[2]
        item['benefit'] = tag.text

        # 更新日期
        tag = res.select('time')[0]
        item['update_date'] = tag.text

        # 批次No
        item['batchNo'] = self.batchNo

        return item


    def validate(self, jobNo):
        try:
            # file = "D:\\0)SourceCode\\scrapy\\web104\\web104.sqlite"
            # # create a database connection
            # db = database()
            # conn = db.create_sqlite_connection(file)

            db = database()
            conn = db.create_connection()

            with conn:
                self.cur = conn.cursor()
                self.cur.execute("SELECT * FROM web104 where jobNo = '" + jobNo + "'")

                rows = self.cur.fetchall()

                if len(rows) > 0:
                    return False
                else:
                    return True
        except:
            print("DB error")


