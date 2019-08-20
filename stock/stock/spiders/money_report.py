# -*- coding: utf-8 -*
import requests
import scrapy
import pandas as pd

from stock.items import MoneyReportItem


class MoneyReportSpider(scrapy.Spider):
    name = 'money_report'

    custom_settings = {
        'DOWNLOAD_DELAY': 30,
        'CONCURRENT_REQUESTS': 1,
        'ITEM_PIPELINES': {
            'stock.pipelines.MoneyReportPipeline': 100
        }
    }

    year = 108
    season = 2

    def start_requests(self):
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb06'

        payload ={
                    'encodeURIComponent': '1',
                    'step': '1',
                    'firstin': '1',
                    'off': '1',
                    'TYPEK':'sii',
                    'year':str(self.year),
                    'season':str(self.season),
                }

        yield scrapy.FormRequest(url, formdata=payload, dont_filter=True)

    def parse(self, response):
        #response.encoding = 'utf8'
        df = translate_dataFrame(response.text)
        for index, row in df.iterrows():
            item = MoneyReportItem()
            item['myear'] = self.year
            item['season'] = self.season
            item['stock_no'] = row['公司代號'].zfill(6)
            item['revenue'] = row['營業收入']
            item['grossprofit'] = row['毛利率(%)']
            item['operprofit'] = row['營業利益率(%)']
            item['netprofit'] = row['稅前純益率(%)']
            item['aftertaxprofit'] = row['稅後純益率(%)']

            yield item


def remove_td(column):
    remove_one = column.split('<')
    remove_two = remove_one[0].split('>')
    return remove_two[1].replace(",", "")


def is_number(str):
  try:
    # 因為使用float有一個例外是'NaN'
    if str=='NaN':
      return False
    float(str)
    return True
  except ValueError:
    return False

def translate_dataFrame(response):
    # 拆解內容
    table_array = response.split('<table')
    tr_array = table_array[1].split('<tr')

    # 拆解td
    data = []
    index = []
    column = []
    for i in range(len(tr_array)):
        td_array = tr_array[i].split('<td')
        if (len(td_array) > 1):
            code = remove_td(td_array[1])
            name = remove_td(td_array[2])
            revenue = remove_td(td_array[3])
            profitRatio = remove_td(td_array[4])
            profitMargin = remove_td(td_array[5])
            preTaxIncomeMargin = remove_td(td_array[6])
            afterTaxIncomeMargin = remove_td(td_array[7])
            if is_number(code):
                data.append([code, revenue, profitRatio, profitMargin, preTaxIncomeMargin, afterTaxIncomeMargin])
                index.append(name)
            if (i == 1):
                column.append(code)
                column.append(revenue)
                column.append(profitRatio)
                column.append(profitMargin)
                column.append(preTaxIncomeMargin)
                column.append(afterTaxIncomeMargin)

    return pd.DataFrame(data=data, index=index, columns=column)


def financial_statement(year, season, type='營益分析彙總表'):

    if year >= 1000:
        year -= 1911

    if type == '綜合損益彙總表':
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    elif type == '資產負債彙總表':
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
    elif type == '營益分析彙總表':
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb06'
    else:
        print('type does not match')

    r = requests.post(url, {
        'encodeURIComponent':1,
        'step':1,
        'firstin':1,
        'off':1,
        'TYPEK':'sii',
        'year':str(year),
        'season':str(season),
    })

    r.encoding = 'utf8'

    df = translate_dataFrame(r.text)
    print(r.text)
    return df

# stock = financial_statement(108,1)
# print(stock.loc['一零四'])