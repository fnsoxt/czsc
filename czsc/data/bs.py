# -*- coding: utf-8 -*-
"""
"""
import time
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta
from typing import List
from tqdm import tqdm

from ..analyze import RawBar
from ..enum import Freq


# 数据频度 ：支持分钟(min)/日(D)/周(W)/月(M)K线，其中1min表示1分钟（类推1/5/15/30/60分钟）。

freq_map = {Freq.F1: "1min", Freq.F5: '5min', Freq.F15: "15min", Freq.F30: '30min',
            Freq.F60: "60min", Freq.D: 'D', Freq.W: "W", Freq.M: "M"}
freq_cn_map = {"1分钟": Freq.F1, "5分钟": Freq.F5, "15分钟": Freq.F15, "30分钟": Freq.F30,
        "60分钟": Freq.F60, "日线": Freq.D, "周线": Freq.W, "月线": Freq.M}

dt_fmt = "%Y%m%d%H%M%S%f"
date_fmt = "%Y%m%d"

try:
    lg = bs.login()
except:
    print("BaoStock 初始化失败" + lg.error_msg)


def format_kline(kline: pd.DataFrame, freq: Freq) -> List[RawBar]:
    """BaoStock K线数据转换

    :param kline: BaoStock 数据接口返回的K线数据
    :param freq: K线周期
    :return: 转换好的K线数据
    """
    bars = []
    dt_key = 'trade_time' if '分钟' in freq.value else 'trade_date'
    kline = kline.sort_values(dt_key, ascending=True, ignore_index=True)
    records = kline.to_dict('records')

    for i, record in enumerate(records):
        if freq == Freq.D:
            vol = int(record['vol']*100)
            amount = int(record.get('amount', 0)*1000)
        else:
            vol = int(record['vol'])
            amount = int(record.get('amount', 0))

        # 将每一根K线转换成 RawBar 对象
        bar = RawBar(symbol=record['bs_code'], dt=pd.to_datetime(record[dt_key]),
                     id=i, freq=freq, open=record['open'], close=record['close'],
                     high=record['high'], low=record['low'],
                     vol=vol,          # 成交量，单位：股
                     amount=amount,    # 成交额，单位：元
                     )
        bars.append(bar)
    return bars
