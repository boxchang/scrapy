# -*- coding: utf-8 -*
import MySQLdb
import sys
sys.path.append("..")
import datetime
from stock.database import database
# 成交量可视化
# 绘制K线图+移动平均线+成交量
if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding('utf-8')
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec  # 分割子图
import pandas_datareader.data as web
import datetime
import mpl_finance as mpf  # 替换 import matplotlib.finance as mpf
import MySQLdb
import pandas as pd
from datetime import datetime as ddt
import matplotlib
#解決負號'-'顯示為方塊的問題
matplotlib.rcParams['axes.unicode_minus']=False
plt.rcParams['font.family'] = ['Microsoft YaHei']
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
#zhfont1 = matplotlib.font_manager.FontProperties(fname='C:\Windows\Fonts\msjh.ttc')

class PolicyResult(object):
    def conn_close(self):
        self.conn.close()

    def __init__(self):
        db = database()
        self.conn = db.create_connection()

    def build_dict(seq, key):
        return dict((d[key], dict(d, index=i)) for (i, d) in enumerate(seq))

    def getDrawList(self,table):
        cur = self.conn.cursor()
        sql = "SELECT distinct stock_no,stock_name FROM {table}"
        sql = sql.format(table=table)
        cur.execute(sql)

        rows = cur.fetchall()
        return rows

    def draw(self,stock_no,stock_name,from_date,to_date):
        title = stock_no + "    " + stock_name

        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

        df_stockload = web.DataReader(stock_no+".TW", "yahoo", datetime.datetime(2019, 1, 1), datetime.datetime(2019, 12, 31))
        print(df_stockload.info())

        fig = plt.figure(figsize=(8, 6), dpi=100, facecolor="white")  # 创建fig对象

        gs = gridspec.GridSpec(2, 1, left=0.06, bottom=0.15, right=0.96, top=0.96, wspace=None, hspace=0,
                               height_ratios=[3.5, 1])
        graph_KAV = fig.add_subplot(gs[0, :])
        graph_VOL = fig.add_subplot(gs[1, :])

        # 绘制K线图
        mpf.candlestick2_ochl(graph_KAV, df_stockload.Open, df_stockload.Close, df_stockload.High, df_stockload.Low, width=0.5,
                              colorup='r', colordown='g')  # 绘制K线走势

        # 绘制移动平均线图
        df_stockload['Ma20'] = df_stockload.Close.rolling(window=20).mean()  # pd.rolling_mean(df_stockload.Close,window=20)
        df_stockload['Ma30'] = df_stockload.Close.rolling(window=30).mean()  # pd.rolling_mean(df_stockload.Close,window=30)
        df_stockload['Ma60'] = df_stockload.Close.rolling(window=60).mean()  # pd.rolling_mean(df_stockload.Close,window=60)

        graph_KAV.plot(np.arange(0, len(df_stockload.index)), df_stockload['Ma20'], 'black', label='M20', lw=1.0)
        graph_KAV.plot(np.arange(0, len(df_stockload.index)), df_stockload['Ma30'], 'green', label='M30', lw=1.0)
        graph_KAV.plot(np.arange(0, len(df_stockload.index)), df_stockload['Ma60'], 'blue', label='M60', lw=1.0)

        graph_KAV.legend(loc='best')
        graph_KAV.set_title(title)
        graph_KAV.set_ylabel(u"價格")
        graph_KAV.set_xlim(0, len(df_stockload.index))  # 设置一下x轴的范围
        graph_KAV.set_xticks(range(0, len(df_stockload.index), 15))  # X轴刻度设定 每15天标一个日期

        print('df_stockload.index')
        print(df_stockload.index)

        cur = self.conn.cursor(MySQLdb.cursors.DictCursor)

        # 畫買進點位
        sql = """
        select data_date,stock_lprice from t1p5f
        where stock_no = '{stock_no}' and data_date between '{from_date}' and '{to_date}' order by data_date;
        """
        sql = sql.format(stock_no="00"+stock_no,from_date=from_date,to_date=to_date)
        print(sql)
        cur.execute(sql)
        rows = cur.fetchall()

        tmp_price_tuple = []
        tmp_posi_tuple = []

        for row in rows:
            try:
                tmp1 = row['stock_lprice']
                tmp2 = df_stockload.index.get_loc(row['data_date'])
                tmp_price_tuple.append(tmp1)
                tmp_posi_tuple.append(tmp2)
            except:
                print(df_stockload.index)
                pass

        data = {
            'Close': tmp_price_tuple,
            'Date': tmp_posi_tuple
        }

        df = pd.DataFrame(data=data)

        graph_KAV.scatter(df['Date'], df['Close'], color='blue')

        # 改寫成交量
        cur2 = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = """
        select data_date,legalperson from legalperson_hist
        where stock_no = '{stock_no}' and data_date between '{from_date}' and '{to_date}' order by data_date;
        """
        sql = sql.format(stock_no="00"+stock_no,from_date=from_date,to_date=to_date)
        print(sql)
        cur2.execute(sql)
        rows = cur2.fetchall()

        count_posi = 0
        count_negi = 0
        tmp_dict = {}
        index_dict = {}
        index = 0
        for row in rows:
            if row['legalperson'] > 0:
                count_posi += row['legalperson']
                count_negi = 0
                tmp_dict[row['data_date']] = count_posi
            else:
                count_negi += row['legalperson']
                count_posi = 0
                tmp_dict[row['data_date']] = count_negi
            index_dict[index] = row['data_date']
            index += 1

        # 绘制成交量图
        graph_VOL.bar(np.arange(0, len(tmp_dict)), tmp_dict.values())
        graph_VOL.set_ylabel(u"外資連續買超成交量")
        graph_VOL.set_xlabel(u"日期")
        graph_VOL.set_xlim(0, len(tmp_dict))  # 设置一下x轴的范围
        graph_VOL.set_xticks(range(0, len(index_dict.keys()), 15))  # X轴刻度设定 每15天标一个日期
        graph_VOL.set_xticklabels([index_dict[index] for index in graph_VOL.get_xticks()])  # 标签设置为日期

        # X-轴每个ticker标签都向右倾斜45度
        for label in graph_KAV.xaxis.get_ticklabels():
            label.set_visible(False)  # 隐藏标注 避免重叠

        for label in graph_VOL.xaxis.get_ticklabels():
            label.set_rotation(45)
            label.set_fontsize(10)  # 设置标签字体

        #plt.show()
        #plt.legend(prop=zhfont1)
        fig.savefig(stock_no+'.png')


# pr = PolicyResult()
# items = pr.getDrawList('t1p5f')
#
# for item in items:
#     stock_no = item[0][2:6]
#     pr.draw(stock_no,item[1],'20190101','20191231')

pr = PolicyResult()
stock_no = "2313"
pr.draw(stock_no,u"華通",'20190101','20191231')