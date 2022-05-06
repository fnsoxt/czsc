# -*- coding: utf-8 -*-
"""
describe:
Tdx 数据缓存，这是用pickle缓存数据，是临时性的缓存。
单次缓存，多次使用，但是不做增量更新，适用于研究场景。
数据缓存是一种用空间换时间的方法，需要有较大磁盘空间，跑全市场至少需要50GB以上。
"""
import os.path
import shutil
import datetime
import pandas as pd
import math

from .tdx import *
from .. import envs
from ..utils import io


class TdxDataCache:
    """Tdx 数据缓存"""
    def __init__(self, data_path, sdt, edt):
        """
        :param data_path: 数据路径
        :param sdt: 缓存开始时间
        :param edt: 缓存结束时间
        """
        self.date_fmt = "%Y%m%d"
        self.verbose = envs.get_verbose()
        self.sdt = pd.to_datetime(sdt).strftime(self.date_fmt)
        self.edt = pd.to_datetime(edt).strftime(self.date_fmt)
        self.data_path = data_path
        self.prefix = "TDX_CACHE"
        self.name = f"{self.prefix}_{self.sdt}_{self.edt}"
        self.cache_path = os.path.join(self.data_path, self.name)
        os.makedirs(self.cache_path, exist_ok=True)
        self.__prepare_api_path()

        self.freq_map = {
            7: Freq.F1,
            0: Freq.F5,
            1: Freq.F15,
            2: Freq.F30,
            3: Freq.F60,
            9: Freq.D,
            5: Freq.W,
            6: Freq.M,
        }
        self.freq_reverse_map = {
            Freq.F1: 7,
            Freq.F5: 0,
            Freq.F15: 1,
            Freq.F30: 2,
            Freq.F60: 3,
            Freq.D: 9,
            Freq.W: 5,
            Freq.M: 6,
        }
        self.kline_num_map = {
            Freq.F1: 240,
            Freq.F5: 48,
            Freq.F15: 16,
            Freq.F30: 8,
            Freq.F60: 4,
            Freq.D: 1,
        }

    def __prepare_api_path(self):
        """给每个Tdx数据接口创建一个缓存路径"""
        cache_path = self.cache_path
        self.api_names = [
            'query_day', 'query_minutes'
        ]
        self.api_path_map = {k: os.path.join(cache_path, k) for k in self.api_names}

        for k, path in self.api_path_map.items():
            os.makedirs(path, exist_ok=True)

    def clear(self):
        """清空缓存"""
        for path in os.listdir(self.data_path):
            if path.startswith(self.prefix):
                path = os.path.join(self.data_path, path)
                shutil.rmtree(path)
                if self.verbose:
                    print(f"clear: remove {path}")
                if os.path.exists(path):
                    print(f"Tdx 数据缓存清理失败，请手动删除缓存文件夹：{self.cache_path}")

    # ------------------------------------Tdx 原生接口----------------------------------------------

    def query_minutes(self, tdx_code, sdt, edt, freq=Freq.D, adj="3", raw_bar=True):
        """获取分钟线

        https://rainx.gitbooks.io/pytdx/content/pytdx_hq.html

        :param tdx_code: 标的代码
        :param sdt: 开始时间，精确到分钟
        :param edt: 结束时间，精确到分钟
        :param freq: 分钟周期，可选值 1min, 5min, 15min, 30min, 60min
        :param adj: 3不复权，2前复权，1后复权
        :param raw_bar: 是否返回 RawBar 对象列表
        :return:
        """
        cache_path = self.api_path_map['query_minutes']
        file_cache = os.path.join(cache_path, f"query_minutes_{tdx_code}_{freq}_{adj}.pkl")

        if os.path.exists(file_cache):
            kline = io.read_pkl(file_cache)
            if self.verbose:
                print(f"query_minutes: read cache {file_cache}")
        else:
            data_list = []
            market = 1 if tdx_code[0] == '6' else 0
            start_date = datetime.datetime.strptime(sdt, "%Y-%m-%d")
            end_date = datetime.datetime.strptime(edt, "%Y-%m-%d")
            index_length = (end_date - start_date).days + 1
            n = math.ceil(index_length / 800) * self.kline_num_map[freq]
            tdx_freq = self.freq_reverse_map[freq]
            for i in range(n):
              data_list += api.get_security_bars(tdx_freq, market, tdx_code, (n - 1 - i)*800, 800)
            kline = api.to_df(data_list)
            kline['tdx_code'] = tdx_code
            kline['trade_time'] = pd.to_datetime(kline['datetime'], format=dt_fmt)
            kline['dt'] = kline['trade_time']
            float_cols = ['open', 'close', 'high', 'low', 'vol', 'amount']
            kline[float_cols] = kline[float_cols].astype('float32')
            kline['avg_price'] = kline['amount'] / kline['vol']
            # print(data_list)
            # exit()
            # 删除9:30的K线
            kline['keep'] = kline['trade_time'].apply(lambda x: 0 if x.hour == 9 and x.minute == 30 else 1)
            kline = kline[kline['keep'] == 1]
            # 删除没有成交量的K线
            kline = kline[kline['vol'] > 0]
            kline.drop(['keep'], axis=1, inplace=True)

            start_date = pd.to_datetime(self.sdt)
            end_date = pd.to_datetime(self.edt)
            kline = kline[(kline['trade_time'] >= start_date) & (kline['trade_time'] <= end_date)]
            kline = kline.reset_index(drop=True)
            kline['trade_date'] = kline.trade_time.apply(lambda x: x.strftime(date_fmt))

            for bar_number in (1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377):
                # 向后看
                n_col_name = 'n' + str(bar_number) + 'b'
                kline[n_col_name] = (kline['close'].shift(-bar_number) / kline['close'] - 1) * 10000
                kline[n_col_name] = kline[n_col_name].round(4)

                # 向前看
                b_col_name = 'b' + str(bar_number) + 'b'
                kline[b_col_name] = (kline['close'] / kline['close'].shift(bar_number) - 1) * 10000
                kline[b_col_name] = kline[b_col_name].round(4)

            io.save_pkl(kline, file_cache)

        sdt = pd.to_datetime(sdt)
        edt = pd.to_datetime(edt)
        bars = kline[(kline['trade_time'] >= sdt) & (kline['trade_time'] <= edt)]
        bars.reset_index(drop=True, inplace=True)
        if raw_bar:
            bars = format_kline(bars, freq=freq)
        return bars
