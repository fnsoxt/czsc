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
from czsc.data.ts_cache import TsDataCache
from czsc import signals
from czsc import CzscAdvancedTrader
from czsc.objects import Signal, Freq
from czsc.sensors.utils import check_signals_acc

os.environ['czsc_verbose'] = '1'
os.environ['czsc_min_bi_len'] = "6"     # 通过环境变量设定最小笔长度，6 对应新笔定义，7 对应老笔定义
dc = TsDataCache('.', sdt='2010-01-01', edt='20231230')
symbol = '000858.SZ'
bars = dc.daily(ts_code=symbol, start_date='20181101', end_date='20220502')

def get_signals(cat: CzscAdvancedTrader) -> OrderedDict:
    s = OrderedDict({"symbol": cat.symbol, "dt": cat.end_dt, "close": cat.latest_price})
    for _, c in cat.kas.items():
        if c.freq == Freq.D:
            s.update(signals.bxt.get_s_like_bs(c, di=1))
            s.update(signals.bxt.get_s_three_bi(c, di=1))
    return s


if __name__ == '__main__':
    # 直接查看全部信号的隔日快照
    # check_signals_acc(bars, get_signals=get_signals)

    # 查看指定信号的隔日快照
    signal_list = [
        Signal("日线_倒1笔_三笔形态_向上盘背_任意_任意_0"),
        Signal("日线_倒1笔_三笔形态_向下盘背_任意_任意_0"),
    ]
    check_signals_acc(bars, signals=signal_list, get_signals=get_signals)
