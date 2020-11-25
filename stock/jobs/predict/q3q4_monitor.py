# -*- coding: utf-8 -*
import numpy
import sys
sys.path.append("../../")
from jobs.app_lib.strategy import StaticStrategy
import datetime
from stock.database import database
import MySQLdb
from stock.line import lineNotifyMessage

#  q3q4_predict.py 要先跑完才能跑這支monitor


#  取得要Monitor的範圍
#  條件一、三年配息率平均 > 60%
#  條件二、預估殖利率 > 7%
#  條件三、過去20日平均交易量 > 1000張   成交張數 = round(stock_buy/1000,0)
#  條件四、總市值 > 100億
#  條件五、配息率大於100%的刪除
#  條件六、排除不要監控的公司
class q3q4Monitor(object):
    data_date = datetime.date.today().strftime('%Y%m%d')

    def conn_close(self):
        self.conn.close()

    def __init__(self, data_date):
        self.data_date = data_date
        self.db = database()
        self.conn = self.db.create_connection()

    #  更新今日股價跟預估殖息率
    def updatePreDividend(self):
        print("updatePreDividend Start")
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "SELECT a.batch_no,a.stock_no,stock_eprice,b.pre_div,b.pre_rate,round(b.pre_div/stock_eprice*100,2) pre_rate2 " \
              "FROM stockprice a, predividend b WHERE a.batch_no = '{data_date}' " \
              "AND b.stock_no = a.stock_no AND a.stock_eprice>0 AND b.pre_div>0"
        sql = sql.format(data_date=self.data_date)
        cur.execute(sql)
        lists = cur.fetchall()

        cur = self.conn.cursor()
        for data in lists:
            update_sql = """update predividend set data_date='{data_date}',price={price},pre_rate={pre_rate},updated_date=now() where stock_no='{stock_no}'"""
            update_sql = update_sql.format(data_date=data['batch_no'], price=data['stock_eprice'], pre_rate=data['pre_rate2'], stock_no=data['stock_no'])
            print(update_sql)
            cur.execute(update_sql)
        self.conn.commit()
        print("updatePreDividend End")


    def getStockList(self):
        results = ""
        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = """SELECT * FROM (
                    SELECT a.stock_no,a.stock_name,a.stock_eprice,d.pre_rate,round(AVG(stock_buy)/1000,0) amount 
                    FROM stockprice a ,(SELECT data_date FROM taiex ORDER BY data_date desc LIMIT 20) b,
                    (select stock_no,pre_rate from predividend WHERE pre_rate > 7 AND last_rate < 100 AND last_rate > 60 AND cpr_rate > 0 ) d 
                    WHERE a.batch_no = b.data_date AND a.stock_no = d.stock_no
                    GROUP BY a.stock_no,a.stock_name HAVING round(AVG(stock_buy)/1000,0) > 1000) aa, 
                    (SELECT k.stock_no,k.stock_num*stock_eprice total FROM stockholder k,stockprice j WHERE LEVEL = 17 AND k.stock_no = j.stock_no AND j.batch_no = '{data_date}' 
                    and k.stock_num*stock_eprice > 10000000000 ) bb WHERE aa.stock_no = bb.stock_no """
        sql = sql.format(data_date=self.data_date)
        cur.execute(sql)
        lists = cur.fetchall()

        for data in lists:
            self.chkBooleanChannel(data['stock_no'], data['stock_name'], data['stock_eprice'], data['pre_rate'])

        return results

    #  用布林通道判斷是否買進
    def chkBooleanChannel(self, stock_no, stock_name, currentPrice, pre_rate):
        # 20日均線買賣訊號
        ss = StaticStrategy()
        MAt = ss.getMa20Value(stock_no)
        MAt5 = ss.getMa5Value(stock_no)
        ma20 = ss.Ma20_Flag_Gap(self.data_date, stock_no)

        # 布林通道
        SDt = ss.calculate_SD(stock_no, MAt)
        UBt = round(MAt + (SDt * 2), 2)
        LBt = round(MAt - (SDt * 2), 2)
        PB = round((currentPrice - LBt) / (UBt - LBt) * 100, 2)

        result = stock_name+"("+stock_no+") 預測殖利率"+str(pre_rate)+"\n"
        result += "---高殖息率股布林通道觸發---------\n"
        result += "20日均線為" + str(MAt) + "，%B為" + str(PB) + "\n"
        print(stock_name+" ma20:"+str(ma20)+" PB:"+str(PB))
        ma_gap = MAt5/MAt-1
        if ma20 >= 0 and ma20 <= 2 and PB > 50 and PB <=60 and ma_gap < -0.001:
        #if ma20 < 0 and PB <= 50 and MAt5 > MAt:
            result += "5日線快突破20日線上，可觀注\n"
            result += "壓力線為" + str(UBt) + "\n"
            result += "今日價:" + str(currentPrice) + "\n"
            result += "支撐線為" + str(LBt) + "\n"
            result += "------------------------------\n"
            print(result)
            token = "zoQSmKALUqpEt9E7Yod14K9MmozBC4dvrW1sRCRUMOU"
            lineNotifyMessage(token, result)


if len(sys.argv) > 1:
    data_date = sys.argv[1]
else:
    data_date = datetime.date.today().strftime('%Y%m%d')

qm = q3q4Monitor(data_date)
qm.updatePreDividend()
qm.getStockList()