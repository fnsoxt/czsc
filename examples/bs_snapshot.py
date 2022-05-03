# -*- coding: utf-8 -*-
"""
author: zengbin93
email: zeng_bin8888@163.com
create_dt: 2022/4/27 21:51
describe: 以 Tushare 数据为例编写快速入门样例
"""
import sys
sys.path.insert(0, '.')
sys.path.insert(0, '..')
import os
import pandas as pd
from czsc import CZSC
from czsc.data.bs_cache import BsDataCache

os.environ['czsc_verbose'] = "1"        # 是否输出详细执行信息，0 不输出，1 输出
os.environ['czsc_min_bi_len'] = "6"     # 通过环境变量设定最小笔长度，6 对应新笔定义，7 对应老笔定义
pd.set_option('mode.chained_assignment', None)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 20)



symbol = 'sh.601086'
freq="5"
sdt='2021-04-01'
edt='2022-05-02'
dc = BsDataCache('.', sdt='2010-01-01', edt='2023-12-30')
bars = dc.query_minutes(bs_code=symbol, freq=freq, sdt=sdt, edt=edt)

c = CZSC(bars)
home_path = os.path.expanduser("~")
file_html = os.path.join(home_path, "temp_czsc.html")
chart = c.to_echarts("1400px", "580px")
chart.render(file_html)
print("file://" + file_html)
