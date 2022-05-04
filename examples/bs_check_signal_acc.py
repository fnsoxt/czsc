# -*- coding: utf-8 -*-
"""
author: zengbin93
email: zeng_bin8888@163.com
create_dt: 2021/12/13 17:48
describe: 验证信号计算的准确性
"""
import sys
sys.path.insert(0, '.')
sys.path.insert(0, '..')

import os
from collections import OrderedDict
from czsc.data.bs_cache import BsDataCache
from czsc import signals
from czsc import CzscAdvancedTrader
from czsc.objects import Signal, Freq
from czsc.sensors.utils import check_signals_acc

os.environ['czsc_verbose'] = '1'
os.environ['czsc_min_bi_len'] = "6"     # 通过环境变量设定最小笔长度，6 对应新笔定义，7 对应老笔定义
dc = BsDataCache('.', sdt='2010-01-01', edt='2023-12-30')

def get_signals(cat: CzscAdvancedTrader) -> OrderedDict:
    s = OrderedDict({"symbol": cat.symbol, "dt": cat.end_dt, "close": cat.latest_price})
    for _, c in cat.kas.items():
        if c.freq == Freq.D:
            s.update(signals.bxt.get_s_like_bs(c, di=1))
    return s

def get_f5_signals(cat: CzscAdvancedTrader) -> OrderedDict:
    s = OrderedDict({"symbol": cat.symbol, "dt": cat.end_dt, "close": cat.latest_price})
    for _, c in cat.kas.items():
        if c.freq == Freq.F5:
            s.update(signals.bxt.get_s_like_bs(c, di=1))
    return s

if __name__ == '__main__':
    # 直接查看全部信号的隔日快照
    # 五梁液
    '''
    symbol = 'sz.000858'
    bars = dc.query_minutes(bs_code=symbol, freq="5", sdt='2018-11-01', edt='2022-05-02')
    # print(bars)
    # exit()
    check_signals_acc(bars, get_signals=get_signals)
    '''
    # 国芳集团
    symbol = 'sh.601086'
    bars = dc.query_minutes(bs_code=symbol, freq="5", sdt='2022-04-01', edt='2022-05-02')
    # print(bars)
    # exit()
    check_signals_acc(bars, get_signals=get_f5_signals)

    # 查看指定信号的隔日快照
    # signals = [
    #     Signal("5分钟_倒9笔_类买卖点_类一买_任意_任意_0"),
    #     Signal("5分钟_倒9笔_类买卖点_类一卖_任意_任意_0"),
    # ]
    # check_signals_acc(bars, signals=signals, get_signals=get_signals)