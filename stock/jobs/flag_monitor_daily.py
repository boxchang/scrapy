# -*- coding: utf-8 -*
import sys
sys.path.append("..")
from jobs.app_lib.strategy import DynamicStrategy, StaticStrategy
from funcs.stockflag import stockflag
import datetime
from stock.database import database
import MySQLdb
from stock.line import lineNotifyMessage
from bases.setting import token

if sys.version_info < (3, 0):
    reload(sys)
    sys.setdefaultencoding('utf-8')


class Stock(object):
    stock_no = ""
    percent_price90 = 0
    percent_price80 = 0
    percent_price70 = 0
    percent_price50 = 0
    stock_no = ""
    stock_name = ""
    flagDate = ""
    forePercent = 0 # 外資累積買超比例
    todayPercent = 0 # 外資今日買超比例
    taiexPercent = 0 #大盤漲幅
    investPercent = 0 # 投信買超比例
    mostPrice = 0 # 240天內最高價
    currentPrice = 0 # 目前價格

    def __init__(self, stock_no, stock_name, flagDate):
        self.stock_no =stock_no
        self.stock_name = stock_name
        self.flagDate = flagDate

#監控旗標日的股票
#該程式只能跑在有開市的日期
class FlagMonitorDaily(object):
    today = ""

    def conn_close(self):
        self.conn.close()

    def __init__(self, data_date):
        db = database()
        self.conn = db.create_connection()
        self.today = data_date

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
            stock = Stock(stock_no, stock_name, flagDate)

            msg = msg + "Stock No : " + stock_no + "(" + stock_name +")\n"
            msg = msg + "旗標日：" + flagDate + "\n"

            #條件一
            stock.forePercent = ds.Foreign_Percent(stock_no, flagDate)
            stock.todayPercent = ds.Today_Foreign_Percent(stock_no)
            msg = msg + "外資買超比率 :" + str(stock.forePercent) + "(" + str(stock.todayPercent) + ")\n"

            #條件二
            stock.taiexPercent = ds.Taiex_Percent(flagDate)
            if stock.taiexPercent >= 5:
                result_2 = "Yes"
            else:
                result_2 = "No"
            msg = msg + "大盤漲幅>=5% :" + str(stock.taiexPercent) + " ("+result_2+")\n"

            #條件三
            stock.investPercent = ds.Invest_Percent(stock_no, flagDate)
            if stock.investPercent >= 0.1:
                result_3 = "Yes"
            else:
                result_3 = "No"
            msg = msg + "投信買超比率 >= 0.1% :" + str(stock.investPercent) + " ("+result_3+")\n"

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
            stock.mostPrice = ds.MostPrice(stock_no)
            stock.currentPrice = ds.CurrentPrice(self.today, stock_no)
            msg += "---中長期指標-----------------\n"
            msg += "240天最高價 : " + str(stock.mostPrice) + "\n"

            #年線乖離率
            result_6 = ""
            ma240 = ss.Ma240_Flag_Gap(self.today, stock_no)
            if ma240 < -20:
                result_6 = "年線乖離率 :" + str(ma240) + "，偏離年線很大，股價剛經過一段時間急跌\n"
            elif ma240 > -20 and ma240 <= 0:
                result_6 = "年線乖離率 :" + str(ma240) + "，等待突破年壓力線\n"
            elif ma240 > 0 and ma240 < 20:
                result_6 = "年線乖離率 :" + str(ma240) + "，股價具備年線支撐\n"
            elif ma240 > 20:
                result_6 = "年線乖離率 :" + str(ma240) + "，股價已經上漲了一段時間\n"
            msg = msg + result_6
            # 用外資買超比率計算預期股價
            if stock.forePercent > 0:
                stock = self.calculate_stock_price(stock)

                msg += "外資比例推算\n"
                msg += "機率值90% : " + str(stock.percent_price90) + "\n"
                msg += "機率值80% : " + str(stock.percent_price80) + "\n"
                msg += "機率值70% : " + str(stock.percent_price70) + "\n"
                msg += "機率值50% : " + str(stock.percent_price50) + "\n"

            #20日均線買賣訊號
            result_8 = ""
            MAt = ss.getMa20Value(stock_no)
            ma20 = ss.Ma20_Flag_Gap(self.today, stock_no)

            # 布林通道
            SDt = ss.calculate_SD(stock_no, MAt)
            UBt = round(MAt + (SDt * 2), 2)
            LBt = round(MAt - (SDt * 2), 2)
            PB = round((stock.currentPrice - LBt) / (UBt - LBt) * 100, 2)

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
            result_9 += "今日價:" + str(stock.currentPrice) + "\n"
            result_9 += "支撐線為"+str(LBt)+"\n"
            result_9 += "------------------------------\n"

            if len(result_9) > 0 :
                msg = msg + result_9

            # 外資加權平均收盤價
            avg_price = ds.Foreign_Avg_Price(stock_no, flagDate)
            msg = msg + "外資平均加權價格 :" + str(avg_price) + "\n"

            # 關閉旗標日
            if stock.forePercent <= 1 and stock.todayPercent < 0:
                msg = msg + "不符合預期，該旗標日關閉\n"
                self.close_flag(stock_no)
                print(msg)
                lineNotifyMessage(token, msg)
            elif stock.currentPrice < stock.percent_price70 and result_4 == "Yes" and result_3 == "Yes":
                print(msg)
                lineNotifyMessage(token, msg)
        ds.conn_close()

    def calculate_stock_price(self,stock):
        msg = ""
        times = 0
        foreign_rate = stock.forePercent
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
        sql = sql.format(stock_no=stock.stock_no)
        cur.execute(sql)

        if cur.rowcount > 0:
            row = cur.fetchone()
            stock_lprice = row['stock_lprice']
            stock.percent_price70 = round(stock_lprice*(1+(percent/100)),1)
            stock.percent_price50 = round(stock_lprice * (1 + (percent50/100)),1)
            stock.percent_price80 = round(stock_lprice * (1 + (percent80/100)),1)
            stock.percent_price90 = round(stock_lprice * (1 + (percent90/100)),1)

            # sql = "update stockflag set price90 ={percent_price90},price80={percent_price80},price70={percent_price70},price50={percent_price50} where stock_no ='{stock_no}' and enable is null"
            # sql = sql.format(percent_price90=percent_price90,percent_price80=percent_price80,percent_price70=percent_price70,percent_price50=percent_price50,stock_no=stock_no)
            # db.execute_sql(sql)
        return stock


    def close_flag(self, stock_no):
        sf = stockflag()
        sf.delFlagDate(stock_no)


if len(sys.argv) > 1:
    data_date = sys.argv[1]
else:
    data_date = datetime.date.today().strftime('%Y%m%d')

fmd = FlagMonitorDaily(data_date)
fmd.Message()

