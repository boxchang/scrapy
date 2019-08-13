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


class stockholder(object):
    stock_no = ""
    level = 0
    stock_num = 0
    holder_num = 0
    percent = 0.0
    data_date = ""
