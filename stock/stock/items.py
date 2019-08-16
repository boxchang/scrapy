# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class StockItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    stock_no = scrapy.Field()
    level = scrapy.Field()
    stock_num = scrapy.Field()
    holder_num = scrapy.Field()
    percent = scrapy.Field()
    data_date = scrapy.Field()
    # pass


class StockPriceItem(scrapy.Item):
    stock_no = scrapy.Field()    # 證券代號
    stock_name = scrapy.Field()  # 證券名稱
    stock_buy = scrapy.Field()   # 成交股數
    stock_num = scrapy.Field()   # 成交筆數
    stock_amount = scrapy.Field()  # 成交金額
    stock_sprice = scrapy.Field()  # 開盤價
    stock_hprice = scrapy.Field()  # 最高價
    stock_lprice = scrapy.Field()  # 最低價
    stock_eprice = scrapy.Field()  # 收盤價
    stock_status = scrapy.Field()  # 漲跌
    stock_gap = scrapy.Field()        # 漲跌價差
    stock_last_buy = scrapy.Field()   # 最後揭示買價
    stock_last_bnum = scrapy.Field()  # 最後揭示買量
    stock_last_sell = scrapy.Field()  # 最後揭示賣價
    stock_last_snum = scrapy.Field()  # 最後揭示賣量
    stock_value = scrapy.Field()      # 本益比


