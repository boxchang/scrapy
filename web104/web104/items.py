# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class Web104Item(scrapy.Item):
    custName = scrapy.Field()
    jobNo = scrapy.Field()
    jobName = scrapy.Field()
    description = scrapy.Field()
    history = scrapy.Field()
    tool = scrapy.Field()
    other = scrapy.Field()
    jobAddrNoDesc = scrapy.Field()
    addr = scrapy.Field()
    update_date = scrapy.Field()
    jobLink = scrapy.Field()
    batchNo = scrapy.Field()
    #pass
