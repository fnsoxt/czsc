import akshare as ak
import math
import pandas as pd
import datetime
from typing import List, Callable
from urllib.parse import quote

from czsc各个版本.czsc0819.czsc.objects import RawBar, Event
from czsc各个版本.czsc0819.czsc.enum import Freq
from czsc各个版本.czsc0819.czsc.utils.bar_generator import freq_end_time
from czsc各个版本.czsc0819.czsc.signals.signals import get_default_signals
from czsc各个版本.czsc0819.czsc.analyze_mm import CZSC

import baostock as bs
from pytdx.hq import TdxHq_API

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.min_rows', 500)  # 最多显示数据的行数


freq_map = {'1min': Freq.F1, '5min': Freq.F5, '15min': Freq.F15, '30min': Freq.F30,
            '60min': Freq.F60, 'D': Freq.D, 'W': Freq.W, 'M': Freq.M}


def get_kline_period_from_bs(symbol: str, start_date: str,
                             end_date: str, freq: str, fq=True) -> List[RawBar]:
    """获取指定时间段的行情数据

    :param symbol: 通达信标的代码
    :param start_date: 开始日期，格式：'2021-01-01'
    :param end_date: 截止日期，格式：'2021-01-01'
    :param freq: K线级别，可选值 ['1min', '5min', '30min', '60min', 'D', 'W', 'M']
    :param fq: 是否进行复权，默认后复权
    :return:
    """

    # 将输入股票代码转化为字符串格式
    stock_code = str(symbol)

    if fq:
        adjustflag = '1'  # 后复权
    else:
        adjustflag = '3'  # 不复权

    if "min" in freq:
        if len(freq) == 4:
            freq_bs = freq[0]
        else:
            freq_bs = freq[0:2]
        file = "time,open,high,low,close,volume,amount,adjustflag"
    else:
        freq_bs = freq.lower()
        file = "date,open,high,low,close,volume,amount,adjustflag"

    # 给输入代码更改为本地代码命名形式
    if stock_code[0] == '6':
        stock_code = 'sh.' + stock_code
    else:
        stock_code = 'sz.' + stock_code

    #### 登陆系统 ####
    lg = bs.login()
    #### 获取沪深A股历史K线数据 ####
    rs = bs.query_history_k_data_plus(stock_code,
                                      file,
                                      start_date=start_date, end_date=end_date,
                                      frequency=freq_bs, adjustflag=adjustflag)

    if rs.error_code == '0':
        print(stock_code, '  处理完成')
    else:
        print(rs.error_code, rs.error_msg)

    #### 打印结果集 ####
    data_list = []
    bars = []
    i = -1

    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())

    for row in data_list:
        row[0] = row[0][0:12]
        dt = pd.to_datetime(row[0])

        if int(row[5]) > 0:
            i += 1
            bars.append(RawBar(symbol=symbol, dt=dt, id=i, freq=freq_map[freq],
                               open=round(float(row[1]), 2),
                               close=round(float(row[4]), 2),
                               high=round(float(row[2]), 2),
                               low=round(float(row[3]), 2),
                               vol=int(row[5])))

    if start_date:
        bars = [x for x in bars if x.dt >= pd.to_datetime(start_date)]
    if "min" in freq and bars:
        bars[-1].dt = freq_end_time(bars[-1].dt, freq=freq_map[freq])
    bars = [x for x in bars if x.dt <= pd.to_datetime(end_date)]

    bs.logout()
    return bars


def get_kline_preiod_from_tdx(symbol: str, start_date: str,
                              end_date: str, freq: str, fq=True) -> List[RawBar]:
    api = TdxHq_API()
    with api.connect('119.147.212.81', 7709):
        data = []
        # data = api.get_security_bars(9, 0, '000001', 0, 10)

        if symbol[0] == '6':
            market = 1
        else:
            market = 0

        freq_dict = {'1min': 7, '5min': 0, '15min': 1, '30min': 2,
                     '60min': 3, 'D': 9, 'W': 5, 'M': 6}

        min_dict = {'1min': 240, '5min': 48, '15min': 16, '30min': 8, '60min': 4, 'D': 1}
        '''
        trade_date = []
        tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
        for i in tool_trade_date_hist_sina_df.values:
            trade_date.append(str(i[0]))
        # print(trade_date)

        index_0 = str(datetime.date.today())
        index_of_index_0 = trade_date.index(index_0)

        index_of_index_end = trade_date.index(end_date)
        index_of_index_start = trade_date.index(start_date)
        # print('k线开始时间: ', index_of_index_start)
        # print('k线结束时间: ', index_of_index_end)

        index_of_end = index_of_index_0 - index_of_index_end
        index_length = index_of_index_end + 1 - index_of_index_start

        print('第一根K线的位置：', index_of_end + index_length)
        print('总共K线个数: ',index_length)
        print('最后一根K线位置: ',index_of_end)
        k = (index_length * min_dict[freq] - 1) % 800
        print('i循环最后一次剩余K线个数', k + 1)
        print('*'*30)
        data = []
        n = math.ceil((index_length * min_dict[freq] - 1) / 800)
        print(n,'++++++++++++++')
        m = 0
        for i in range(index_length * min_dict[freq] - 1, index_of_end * min_dict[freq] - 1, -800):
            print('单次获取K线的范围起始点：', i)
            print('单次获取K线范围的终止点：', i - 800)
            print('取数开始点：', (index_length * min_dict[freq] - 1))
            print('取数终止点：', (index_of_end * min_dict[freq] - 1))
            # n = ( i - k ) / 800

            print('第几次循环',n)
            print('i - k 等于：', i - k )
            if k != 0:
                # data += api.get_security_bars(freq_dict[freq], market, symbol, i - k + , k + 1)
                # data += api.get_security_bars(freq_dict[freq], market, symbol, index_of_end + 1 + k - 1, k + 1)
                # data += api.get_security_bars(freq_dict[freq], market, symbol, 3 - k - 2, k + 1)
                if m != 0 :  #解决执行第一次的时候执行此列，之后执行别的列
                    data += api.get_security_bars(freq_dict[freq], market, symbol, k + 1 + 800 * (n-2), 800)  # 待调整顺序
                    n -= 1
                    m += 1

                elif m == 0 :
                    data += api.get_security_bars(freq_dict[freq], market, symbol, 0, k + 1)
                    m += 1
                print('*'*20)
                # print(k + 1 + index_of_index_end)
            else:
                data += api.get_security_bars(freq_dict[freq], market, symbol, k + 1 + 800 * n , 800)

                # print(index_of_index_start - 800 * n)
                # print('data = ', i - 800 * n)
                n += 1
                print('n的值为：', n)
                print('*' * 20)
            # if i == 0:
            #     x = 1
            # else:
            #     x = 0
            # print(k * x)
            # data = api.get_security_bars(freq_dict[freq], market, symbol, (m - 1 - i) * 800 + k * x, 1)

            # print(data)
            # df = api.to_df(data)
            # print(data)
            # exit()
            # 判断end_date在哪个位置

      # print(api.to_df(data))

        # for start in range(m, 0, -1):
        #     li = api.get_security_bars(freq_dict[freq], market, symbol, start * 800, 800)
        #     li = api.get_security_bars(freq_dict[freq], market, symbol, k, 800)
        df = api.to_df(data)
        df.to_csv('111.csv')
        '''

        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        index_length = (end_date - start_date).days + 1


        n = math.ceil(index_length / 800) * min_dict[freq]
        if n >=19:
            n = 19
            print('超出数据获取范围，仅展示部分数据')


        for i in range(n):
          data+=api.get_security_bars(freq_dict[freq], market, symbol,(n - 1 - i)*800,800)

        df = api.to_df(data)
        df['datetime'] = pd.to_datetime(df['datetime'])

        df = df[(df['datetime'] >= start_date ) & (df['datetime'] <= (end_date + datetime.timedelta(days=1)))]
        df = df[['datetime','open','close','high','low','vol','amount']]

        bars_list = df.values.tolist()
        bars = []
        i = -1

        for row in bars_list:
            dt = pd.to_datetime(row[0])
            if int(row[5]) > 0:
                i += 1
                bars.append(RawBar(symbol=symbol, dt=dt, id=i, freq=freq_map[freq],
                                   open=round(float(row[1]), 2),
                                   close=round(float(row[2]), 2),
                                   high=round(float(row[3]), 2),
                                   low=round(float(row[4]), 2),
                                   vol=int(row[5])))

        if start_date:
            bars = [x for x in bars if x.dt >= pd.to_datetime(start_date)]
        if "min" in freq and bars:
            bars[-1].dt = freq_end_time(bars[-1].dt, freq=freq_map[freq])
        bars = [x for x in bars if x.dt <= pd.to_datetime(end_date + datetime.timedelta(days=1))]

    return bars

