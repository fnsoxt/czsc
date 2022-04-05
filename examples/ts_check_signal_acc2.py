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
from czsc import CZSC
from czsc.objects import Signal, Freq
from czsc.sensors.utils import check_signals_acc
from czsc.signals.signals import get_s_like_bs

os.environ['czsc_verbose'] = '1'
dc = TsDataCache('.', sdt='2010-01-01', edt='20211209')
symbol = '000858.SZ'
bars = dc.daily(ts_code=symbol, start_date='20181101', end_date='20220402')

def get_signals(c: CZSC) -> OrderedDict:
    s = OrderedDict({"symbol": c.symbol, "dt": c.bars_raw[-1].dt, "close": c.bars_raw[-1].close})
    if c.freq == Freq.D:
        s.update(get_s_like_bs(c, di=9))
    return s


if __name__ == '__main__':
    # 直接查看全部信号的隔日快照
    check_signals_acc(bars, get_signals=get_signals)
