# -*- coding: utf-8 -*
import numpy
import sys
sys.path.append("..")
from funcs.stockflag import stockflag
import datetime
from stock.database import database
import MySQLdb
from stock.line import lineNotifyMessage

reload(sys)
sys.setdefaultencoding("utf-8")
#監控旗標日的股票
#該程式只能跑在有開市的日期
class FlagMonitorDaily(object):
    today = ""

    def conn_close(self):
        self.conn.close()

    def __init__(self):
        db = database()
        self.conn = db.create_connection()
        self.today = datetime.date.today().strftime('%Y%m%d')
        #self.today = "20200807"

    def getFlagStock(self):

        sql = "select * from stockflag where enable is null"

        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        lists = cur.fetchall()

        return lists

    def Message(self):
        msg = ""
        lists = self.getFlagStock()
        ds = DynamicStrategy(self.today)
        ss = StaticStrategy()

        for list in lists:
            msg = ""
            stock_no = list['stock_no']
            stock_name = list['stock_name']
            flagDate = list['data_date']

            msg = msg + "Stock No : " + stock_no + "(" + stock_name +")\n"

            #條件一
            forePercent = ds.Foreign_Percent(stock_no, flagDate)
            todayPercent = ds.Today_Foreign_Percent(stock_no)
            msg = msg + "外資買超比率 :" + str(forePercent) + "(" + str(todayPercent) + ")\n"

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

            #條件五 市值<300億
            s300 = ss.CompanyStockValue(flagDate, stock_no)
            if s300 == "Y":
                result_5 = "Yes，漲幅高"
            else:
                result_5 = "No，漲幅低"
            msg = msg + "市值小於300億 :" + result_5 + "\n"

            #資券比
            financing = ds.Today_Financing_Percent(stock_no)
            if financing > 20:
                result_7 = "資券比 : " + str(financing) + "%，注意！散戶進場\n"
            else:
                result_7 = "資券比 : " + str(financing) + "\n"
            msg = msg + result_7

            #超過240天最高價
            mostPrice = ds.MostPrice(stock_no)
            currentPrice = ds.CurrentPrice(self.today, stock_no)
            msg += "---中長期指標-----------------\n"
            msg += "240天最高價 : " + str(mostPrice) + "\n"

            #年線乖離率
            ma240 = ss.Ma240_Flag_Gap(self.today, stock_no)
            if ma240 < -20:
                result_6 = "年線乖離率 :" + str(ma240) + "，偏離年線很大，股價剛經過一段時間急跌\n"
            elif ma240 > -20 and ma240 < 0:
                result_6 = "年線乖離率 :" + str(ma240) + "，等待突破年壓力線\n"
            elif ma240 > 0 and ma240 < 20:
                result_6 = "年線乖離率 :" + str(ma240) + "，股價具備年線支撐\n"
            elif ma240 > 20:
                result_6 = "年線乖離率 :" + str(ma240) + "，股價已經上漲了一段時間\n"
            msg = msg + result_6
            # 用外資買超比率計算預期股價
            if forePercent > 0:
                msg += "外資比例推算\n"
                msg += self.calculate_stock_price(stock_no, forePercent)

            #20日均線買賣訊號
            result_8 = ""
            MAt = ss.getMa20Value(stock_no)
            ma20 = ss.Ma20_Flag_Gap(self.today, stock_no)

            # 布林通道
            SDt = self.calculate_SD(stock_no, MAt)
            UBt = round(MAt + (SDt * 2), 2)
            LBt = round(MAt - (SDt * 2), 2)
            PB = round((currentPrice - LBt) / (UBt - LBt) * 100, 2)

            result_8 = "---短期指標-------------------\n"
            result_8 += "20日均線為" + str(MAt) + "，%B為"+str(PB) + "\n"
            if ma20 >= 0 and ma20 <= 3:
                result_8 += "，突破20日均線，可買進\n"
            elif ma20 < 0 and ma20 >= -3:
                result_8 += "，跌破20日均線，要賣出\n"

            if PB > 100 :
                result_8 += "%B>100 建議出脫"
            elif PB >= 80 :
                result_8 += "%B>80 多頭行情加碼"
            elif PB < 0 :
                result_8 += "%B<0 可以考慮買進"
            elif PB < 20 :
                result_8 += "%B<20空頭行情減碼"

            if len(result_8) > 0 :
                msg = msg + result_8 + "\n"

            result_9 = "壓力線為" + str(UBt) + "\n"
            result_9 += "今日價:" + str(currentPrice) + "\n"
            result_9 += "支撐線為"+str(LBt)+"\n"
            result_9 += "------------------------------\n"

            if len(result_9) > 0 :
                msg = msg + result_9

            # 關閉旗標日
            if forePercent <= 1 and todayPercent < 0:
                sf = stockflag()
                sf.delFlagDate(stock_no)
                msg = msg + "不符合預期，該旗標日關閉\n"

            token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
            print(msg)
            lineNotifyMessage(token, msg)
        ds.conn_close()

    def calculate_SD(self, stock_no, MAt):

        price_sum = 0

        sql = "select stock_price from legalperson_price a where a.stock_no = {stock_no} order by batch_no desc limit 20"
        sql = sql.format(stock_no=stock_no)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        lists = cur.fetchall()
        for item in lists:
            price_sum += numpy.square(float(item['stock_price'])-MAt)
        SDt = numpy.sqrt(price_sum/len(lists))

        return SDt

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

        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
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
    def CompanyStockValue(self, data_date, stock_no):
        result = ""
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select * from legalperson_price a where (a.stock_price * a.stock_num)/10 < 30000000000 and batch_no = {data_date} and stock_no = {stock_no}"
        sql = sql.format(data_date=data_date, stock_no=stock_no)
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) > 0:
            result = "Y"
        else:
            result = "N"

        return result

    #年線乖離率
    #(旗標日收盤價 / 年線 -1 ) X 100%
    def Ma240_Flag_Gap(self, data_date, stock_no):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select distinct a.stock_name,(c.stock_eprice/b.avg_price-1)*100 result from stockflag a, stockprice_ma240 b, stockprice c " \
              "where a.stock_no = b.stock_no and a.stock_no ={stock_no} and a.stock_no = c.stock_no and c.batch_no = {data_date}"
        sql = sql.format(data_date=data_date, stock_no=stock_no)
        cur.execute(sql)
        result = round(cur.fetchone()['result'],2)
        return result

    #20日均線乖離率
    def Ma20_Flag_Gap(self, data_date, stock_no):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select distinct a.stock_name,(c.stock_eprice/b.avg_price-1)*100 result from stockflag a, stockprice_ma20 b, stockprice c " \
              "where a.stock_no = b.stock_no and a.stock_no ={stock_no} and a.stock_no = c.stock_no and c.batch_no = {data_date}"
        sql = sql.format(data_date=data_date, stock_no=stock_no)
        cur.execute(sql)
        result = round(cur.fetchone()['result'],2)
        return result

    #20日均線值
    def getMa20Value(self, stock_no):
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select avg_price from stockprice_ma20 where stock_no = {stock_no}"
        sql = sql.format(stock_no=stock_no)
        cur.execute(sql)
        result = round(cur.fetchone()['avg_price'], 2)
        return result

    #1年最大跌幅
    #資料量不足，尚未有年資料
    def Year_Flag_Gap(self):
        pass

class DynamicStrategy(object):
    today = ""

    def conn_close(self):
        self.conn.close()

    def __init__(self, today):
        db = database()
        self.conn = db.create_connection()
        self.today = today
    #券資比
    def Today_Financing_Percent(self, stock_no):
        sql = "select round((a.today_borrow_stock / a.today_borrow_money)*100,2) percent from financing a where a.stock_no = {stock_no} and a.data_date = {today}"
        sql = sql.format(stock_no=stock_no, today=self.today)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        result = cur.fetchone()['percent']
        return result

    #動態檢核表(何時賣出，何時加碼)
    #今日外資買超比率
    def Today_Foreign_Percent(self, stock_no):
        sql = "select percent from legalperson_price a where a.stock_no = {stock_no} and batch_no = {today}"
        sql = sql.format(stock_no=stock_no, today=self.today)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        result = cur.fetchone()['percent']
        return result

    #外資買超比率
    def Foreign_Percent(self, stock_no, flagDate):
        sql = "select round(sum(china_sum/stock_num*100),2) result from legalperson_price a where a.stock_no = {stock_no} and batch_no between {flagDate} and {today}"
        sql = sql.format(stock_no=stock_no, flagDate=flagDate, today=self.today)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()['result']
        return result

    #大盤漲幅
    #大盤漲幅 = (今日加權指數 / 旗標日的加權指數 - 1) X 100%
    def Taiex_Percent(self, flagDate):

        #取得旗標日加權
        sql = "select close_index from taiex a where a.data_date = {data_date}"
        sql = sql.format(data_date=flagDate)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        flag_value = cur.fetchone()['close_index']

        # 取得今日加權
        sql = "select close_index from taiex a order by data_date desc limit 0,1"
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        today_value = cur.fetchone()['close_index']

        result = round(((today_value/flag_value)-1)*100,2)

        return result

    #投信買超比率
    #投信買超比率 = 投資張數 / 公司股票張數 X 100%
    def Invest_Percent(self, stock_no, flagDate):
        sql = "select round(sum(invest_sum/stock_num*100),2) result from legalperson_price a where a.stock_no = {stock_no} and batch_no between {flagDate} and {today}"
        sql = sql.format(stock_no=stock_no, flagDate=flagDate, today=self.today)
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
    def MostPrice(self, stock_no):
        sql = "select max(stock_eprice) max_price from stockprice a where a.stock_no = '{stock_no}' order by batch_no desc limit 240"
        sql = sql.format(stock_no=stock_no)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()['max_price']
        return result


    def CurrentPrice(self, data_date, stock_no):
        sql = "select stock_eprice from stockprice a where a.stock_no = '{stock_no}' and a.batch_no = {data_date}"
        sql = sql.format(stock_no=stock_no, data_date=data_date)
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()['stock_eprice']
        return result


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

