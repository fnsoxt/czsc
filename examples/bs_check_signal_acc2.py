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

def get_f5_signals(cat: CzscAdvancedTrader) -> OrderedDict:
    s = OrderedDict({"symbol": cat.symbol, "dt": cat.end_dt, "close": cat.latest_price})
    for _, c in cat.kas.items():
        s.update(signals.bxt.get_s_three_bi(c, di=1))
        s.update(signals.bxt.get_s_base_xt(c, di=1))
    return s

if __name__ == '__main__':
    symbol = 'sz.002329'
    bars = dc.query_minutes(bs_code=symbol, freq="5", sdt='2022-04-01', edt='2022-11-25')

    signals_list = [
        Signal("5分钟_倒1笔_三笔形态_向下盘背_任意_任意_0"),
        Signal("15分钟_倒1笔_三笔形态_向下盘背_任意_任意_0"),
        Signal("30分钟_倒1笔_三笔形态_向下盘背_任意_任意_0"),
        Signal("60分钟_倒1笔_三笔形态_向下盘背_任意_任意_0"),
        Signal("日线_倒1笔_三笔形态_向下盘背_任意_任意_0"),

        Signal("5分钟_倒1笔_基础形态_底背驰_五笔aAb式_任意_0"),
        Signal("15分钟_倒1笔_基础形态_底背驰_五笔aAb式_任意_0"),
        Signal("30分钟_倒1笔_基础形态_底背驰_五笔aAb式_任意_0"),
        Signal("60分钟_倒1笔_基础形态_底背驰_五笔aAb式_任意_0"),
        Signal("日线_倒1笔_基础形态_底背驰_五笔aAb式_任意_0"),

        Signal("5分钟_倒1笔_基础形态_底背驰_五笔类趋势_任意_0"),
        Signal("15分钟_倒1笔_基础形态_底背驰_五笔类趋势_任意_0"),
        Signal("30分钟_倒1笔_基础形态_底背驰_五笔类趋势_任意_0"),
        Signal("60分钟_倒1笔_基础形态_底背驰_五笔类趋势_任意_0"),
        Signal("日线_倒1笔_基础形态_底背驰_五笔类趋势_任意_0"),

        Signal("5分钟_倒1笔_基础形态_底背驰_七笔aAbcd式_任意_0"),
        Signal("15分钟_倒1笔_基础形态_底背驰_七笔aAbcd式_任意_0"),
        Signal("30分钟_倒1笔_基础形态_底背驰_七笔aAbcd式_任意_0"),
        Signal("60分钟_倒1笔_基础形态_底背驰_七笔aAbcd式_任意_0"),
        Signal("日线_倒1笔_基础形态_底背驰_七笔aAbcd式_任意_0"),

        Signal("5分钟_倒1笔_基础形态_底背驰_七笔abcAd式_任意_0"),
        Signal("15分钟_倒1笔_基础形态_底背驰_七笔abcAd式_任意_0"),
        Signal("30分钟_倒1笔_基础形态_底背驰_七笔abcAd式_任意_0"),
        Signal("60分钟_倒1笔_基础形态_底背驰_七笔abcAd式_任意_0"),
        Signal("日线_倒1笔_基础形态_底背驰_七笔abcAd式_任意_0"),

        Signal("5分钟_倒1笔_基础形态_底背驰_七笔aAb式_任意_0"),
        Signal("15分钟_倒1笔_基础形态_底背驰_七笔aAb式_任意_0"),
        Signal("30分钟_倒1笔_基础形态_底背驰_七笔aAb式_任意_0"),
        Signal("60分钟_倒1笔_基础形态_底背驰_七笔aAb式_任意_0"),
        Signal("日线_倒1笔_基础形态_底背驰_七笔aAb式_任意_0"),

        Signal("5分钟_倒1笔_基础形态_底背驰_笔类趋势_任意_0"),
        Signal("15分钟_倒1笔_基础形态_底背驰_笔类趋势_任意_0"),
        Signal("30分钟_倒1笔_基础形态_底背驰_笔类趋势_任意_0"),
        Signal("60分钟_倒1笔_基础形态_底背驰_笔类趋势_任意_0"),
        Signal("日线_倒1笔_基础形态_底背驰_笔类趋势_任意_0"),
    ]
    check_signals_acc(bars, signals=signals_list, get_signals=get_f5_signals)
