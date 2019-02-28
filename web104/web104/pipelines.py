# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3
import time

from web104.database import database


class Web104Pipeline(object):
    def open_spider(self, spider):
        db = database()
        self.conn = db.create_connection()
        self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作
        self.cur.execute('create table if not exists web104(custName varchar(100), jobNo varchar(50), '
                         'jobName varchar(100), description text, history varchar(100), tool text, other text, '
                         'jobAddrNoDesc varchar(50), benefit text, addr text,update_date varchar(50), jobLink varchar(100), '
                         'is_read varchar(1), ts TimeStamp DEFAULT CURRENT_TIMESTAMP, batchNo varchar(14))')

        # self.conn = sqlite3.connect('web104.sqlite')
        # self.cur = self.conn.cursor()  # 建立cursor對資料庫做操作
        # self.cur.execute('create table if not exists web104(custName varchar(100), jobNo varchar(50), '
        #                  'jobName varchar(100), description text, history varchar(100), tool varchar(100), other text, '
        #                  'jobAddrNoDesc varchar(50), addr text,update_date varchar(50), jobLink varchar(100), '
        #                  'is_read varchar(1), TimeStamp NOT NULL DEFAULT (datetime(\'now\',\'localtime\')), batcnNo varchar(14))')

    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()

    def process_item(self, item, spider):
        col = ','.join(item.keys())
        # placeholders = ','.join(len(item) * '?')
        placeholders = ("%s," * len(item))[:-1]
        sql = 'insert into web104({}) values({})'
        print(sql.format(col, placeholders), tuple(item.values()))
        self.cur.execute(sql.format(col, placeholders), tuple(item.values()))
        return item
