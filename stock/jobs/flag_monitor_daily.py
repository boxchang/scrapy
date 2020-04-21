# -*- coding: utf-8 -*
import datetime
from stock.database import database
import MySQLdb
from stock.line import lineNotifyMessage
import sys
sys.path.append("..")
reload(sys)
sys.setdefaultencoding("utf-8")
#監控旗標日的股票
#該程式只能跑在有開市的日期
class FlagMonitorDaily(object):


    def conn_close(self):
        self.conn.close()

    def __init__(self):
        db = database()
        self.conn = db.create_connection()

    def getFlagStock(self):

        sql = "select * from stockflag where enable is null"

        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        lists = cur.fetchall()

        return lists

    def Message(self):
        msg = ""

        lists = self.getFlagStock()
        ds = DynamicStrategy()

        for list in lists:
            msg = ""
            stock_no = list['stock_no']
            stock_name = list['stock_name']
            flagDate = list['data_date']

            msg = msg + "Stock No : " + stock_no + "(" + stock_name +")\n"

            #條件一
            forePercent = ds.Foreign_Percent(stock_no, flagDate)
            msg = msg + "外資買超比率 :" + str(forePercent) + "\n"

            #用外資買超比率計算預期股價
            if forePercent > 0:
                msg = msg + self.calculate_stock_price(stock_no, forePercent)

            #條件二
            taiexPercent = ds.Taiex_Percent(flagDate)
            if taiexPercent >= 5:
                result_2 = "Yes"
            else:
                result_2 = "No"
            msg = msg + "大盤漲幅>=5% :" + str(taiexPercent) + " ("+result_2+")\n"

            #條件三
            investPercent = ds.Invest_Percent(stock_no, flagDate)
            if investPercent >= 0.1:
                result_3 = "Yes"
            else:
                result_3 = "No"
            msg = msg + "投信買超比率 >= 0.1% :" + str(investPercent) + " ("+result_3+")\n"

            #條件四
            Over1M = ds.Foreign_Day_Over1M(stock_no, flagDate)
            if Over1M == "超一個月":
                result_4 = "Yes"
            else:
                result_4 = "No"

            msg = msg + "外資持有時間 :" + Over1M + " (" + result_4 + ")\n"

            token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
            lineNotifyMessage(token, msg)
        ds.conn_close()

    def calculate_stock_price(self,stock_no, foreign_rate):
        msg = ""
        times = 0
        if foreign_rate <=1 :
            times = 15
        elif foreign_rate > 1 and foreign_rate <= 3:
            times = 9
        elif foreign_rate > 3 and foreign_rate <= 5:
            times = 8
        elif foreign_rate > 5 and foreign_rate <= 10:
            times = 6
        elif foreign_rate > 10 and foreign_rate <= 20:
            times = 5
        elif foreign_rate > 20:
            times = 4


        percent = foreign_rate * times
        percent50 = percent+5
        percent80 = percent-5
        percent90 = percent-10

        db = database()
        conn = db.create_connection()
        cur = conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "SELECT stock_lprice FROM stockflag where stock_no ='{stock_no}' and enable is null"
        sql = sql.format(stock_no=stock_no)
        cur.execute(sql)

        if cur.rowcount > 0:
            row = cur.fetchone()
            stock_lprice = row['stock_lprice']
            percent_price70 = round(stock_lprice*(1+(percent/100)),1)
            percent_price50 = round(stock_lprice * (1 + (percent50/100)),1)
            percent_price80 = round(stock_lprice * (1 + (percent80/100)),1)
            percent_price90 = round(stock_lprice * (1 + (percent90/100)),1)

            # sql = "update stockflag set price90 ={percent_price90},price80={percent_price80},price70={percent_price70},price50={percent_price50} where stock_no ='{stock_no}' and enable is null"
            # sql = sql.format(percent_price90=percent_price90,percent_price80=percent_price80,percent_price70=percent_price70,percent_price50=percent_price50,stock_no=stock_no)
            # db.execute_sql(sql)
            msg += "機率值90% : " + str(percent_price90) + "\n"
            msg += "機率值80% : " + str(percent_price80) + "\n"
            msg += "機率值70% : " + str(percent_price70) + "\n"
            msg += "機率值50% : " + str(percent_price50) + "\n"

        return msg

class StaticStrategy(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        db = database()
        self.conn = db.create_connection()

    # 靜態因素(第二階段再做，靜態因素主要是判斷旗標觸發後何時買進，現在先用人為判斷)
    # 市值<300億
    def CompanyStockValue(slef):
        pass



    #年線乖離率
    #(旗標日收盤價 / 年線 -1 ) X 100%
    #資料量不足，尚未有年線
    def Ma240_Flag_Gap(self):
        pass


    #1年最大跌幅
    #資料量不足，尚未有年資料
    def Year_Flag_Gap(self):
        pass

class DynamicStrategy(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        db = database()
        self.conn = db.create_connection()

    #動態檢核表(何時賣出，何時加碼)
    #外資買超比率
    def Foreign_Percent(self, stock_no, flagDate):
        today = datetime.date.today().strftime('%Y%m%d')

        sql = "select round(sum(china_sum/stock_num*100),2) result from legalperson_price a where a.stock_no = {stock_no} and batch_no between {flagDate} and {today}"
        sql = sql.format(stock_no=stock_no, flagDate=flagDate, today=today)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()['result']
        return result

    #大盤漲幅
    #大盤漲幅 = (今日加權指數 / 旗標日的加權指數 - 1) X 100%
    def Taiex_Percent(self, flagDate):
        today = datetime.date.today().strftime('%Y%m%d')

        #取得旗標日加權
        sql = "select close_index from taiex a where a.data_date = {data_date}"
        sql = sql.format(data_date=flagDate)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        flag_value = cur.fetchone()['close_index']

        # 取得今日加權
        sql = "select close_index from taiex a order by data_date desc limit 0,1"
        sql = sql.format(data_date=flagDate)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        today_value = cur.fetchone()['close_index']

        result = round(((today_value/flag_value)-1)*100,2)

        return result

    #投信買超比率
    #投信買超比率 = 投資張數 / 公司股票張數 X 100%
    def Invest_Percent(self, stock_no, flagDate):
        today = datetime.date.today().strftime('%Y%m%d')

        sql = "select round(sum(invest_sum/stock_num*100),2) result from legalperson_price a where a.stock_no = {stock_no} and batch_no between {flagDate} and {today}"
        sql = sql.format(stock_no=stock_no, flagDate=flagDate, today=today)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()['result']
        return result

    #外資買超時間
    #買超時間>=一個月
    #旗標日開始計算外資買超一個月
    def Foreign_Day_Over1M(self, stock_no, flagDate):
        sum_value = 0
        total_value = 0
        sql = "select china_sum,stock_num from legalperson_price a where a.stock_no = {stock_no} and batch_no >= {flagDate}"
        sql = sql.format(stock_no=stock_no, flagDate=flagDate)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        lists = cur.fetchall()
        print(len(lists))

        for list in lists:
            sum_value += list['china_sum']
            total_value = list['stock_num']

        value = round(sum_value / total_value*100, 2)
        if len(lists) > 30 and value > 0:
            result = "超一個月"
        else:
            result = "未滿一個月"

        return result


    #創新高價(資料不足晚點做)
    # 創1年新高 = 旗標日至今日的最高價 > 前240日最高價
    def MostPrice(self):
        pass


    #基本面(這部分看財報狗，關聯財報狗網址)
    #1.毛利上升
    #2.轉虧為盈
    #3.營收成長
    def BasicInformation(self):
        pass


    #編製合理漲幅區間機率表
    #基準股價 = 旗標日最低買價




fmd = FlagMonitorDaily()
fmd.Message()

