# -*- coding: utf-8 -*-
"""
describe:
BaoStock 数据缓存，这是用pickle缓存数据，是临时性的缓存。
单次缓存，多次使用，但是不做增量更新，适用于研究场景。
数据缓存是一种用空间换时间的方法，需要有较大磁盘空间，跑全市场至少需要50GB以上。
"""
import os.path
import shutil

import pandas as pd

from .bs import *
from .. import envs
from ..utils import io


class BsDataCache:
    """BaoStock 数据缓存"""
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
        self.prefix = "BS_CACHE"
        self.name = f"{self.prefix}_{self.sdt}_{self.edt}"
        self.cache_path = os.path.join(self.data_path, self.name)
        os.makedirs(self.cache_path, exist_ok=True)
        self.__prepare_api_path()

        self.freq_map = {
            "1": Freq.F1,
            "5": Freq.F5,
            "15": Freq.F15,
            "30": Freq.F30,
            "60": Freq.F60,
            "D": Freq.D,
            "W": Freq.W,
            "M": Freq.M,
        }

    def __prepare_api_path(self):
        """给每个BaoStock数据接口创建一个缓存路径"""
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
                    print(f"BaoStock 数据缓存清理失败，请手动删除缓存文件夹：{self.cache_path}")

    # ------------------------------------BaoStock 原生接口----------------------------------------------

    def query_day(self, bs_code, start_date, end_date, freq='D', asset="E", adj='qfq', raw_bar=True):
        """获取日线以上数据

        https://BaoStock.pro/document/2?doc_id=109

        :param bs_code:
        :param start_date:
        :param end_date:
        :param freq:
        :param asset: 资产类别：E股票 I沪深指数 C数字货币 FT期货 FD基金 O期权 CB可转债（v1.2.39），默认E
        :param adj: 3不复权，2前复权，1后复权
        :param raw_bar:
        :return:
        """
        cache_path = self.api_path_map['pro_bar']
        file_cache = os.path.join(cache_path, f"pro_bar_{bs_code}_{asset}_{freq}_{adj}.pkl")

        if os.path.exists(file_cache):
            kline = io.read_pkl(file_cache)
            if self.verbose:
                print(f"pro_bar: read cache {file_cache}")
        else:
            start_date_ = (pd.to_datetime(self.sdt) - timedelta(days=1000)).strftime('%Y%m%d')
            kline = bs.pro_bar(bs_code=bs_code, asset=asset, adj=adj, freq=freq,
                               start_date=start_date_, end_date=self.edt)
            kline = kline.sort_values('trade_date', ignore_index=True)
            kline['trade_date'] = pd.to_datetime(kline['trade_date'], format=self.date_fmt)
            kline['dt'] = kline['trade_date']
            kline['avg_price'] = kline['amount'] / kline['vol']

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

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        bars = kline[(kline['trade_date'] >= start_date) & (kline['trade_date'] <= end_date)]
        bars.reset_index(drop=True, inplace=True)
        if raw_bar:
            bars = format_kline(bars, freq=self.freq_map[freq])
        return bars

    def query_minutes(self, bs_code, sdt, edt, freq='60', adj="3", raw_bar=True):
        """获取分钟线

        http://baostock.com/baostock/index.php/Python_API文档#.E8.8E.B7.E5.8F.96.E5.8E.86.E5.8F.B2A.E8.82.A1K.E7.BA.BF.E6.95.B0.E6.8D.AE

        :param bs_code: 标的代码
        :param sdt: 开始时间，精确到分钟
        :param edt: 结束时间，精确到分钟
        :param freq: 分钟周期，可选值 1min, 5min, 15min, 30min, 60min
        :param adj: 3不复权，2前复权，1后复权
        :param raw_bar: 是否返回 RawBar 对象列表
        :return:
        """
        cache_path = self.api_path_map['query_minutes']
        file_cache = os.path.join(cache_path, f"query_minutes_{bs_code}_{freq}_{adj}.pkl")

        if os.path.exists(file_cache):
            kline = io.read_pkl(file_cache)
            if self.verbose:
                print(f"query_minutes: read cache {file_cache}")
        else:
            data_list = []
            file = "time,code,open,high,low,close,volume,amount,adjustflag"
            rs = bs.query_history_k_data_plus(bs_code,
                    file,
                    start_date=sdt, end_date=edt,
                    frequency=freq, adjustflag=adj)
            if rs.error_code == '0':
                print(bs_code, '  处理完成')
            else:
                print(rs.error_code, rs.error_msg)
            while (rs.error_code == '0') & rs.next():
                # 获取一条记录，将记录合并在一起
                data_list.append(rs.get_row_data())
            fields = ['trade_time', 'bs_code', 'open', 'high', 'low', 'close', 'vol', 'amount', 'adj']
            kline = pd.DataFrame(data_list, columns=fields)
            kline['trade_time'] = pd.to_datetime(kline['trade_time'], format=dt_fmt)
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
            bars = format_kline(bars, freq=self.freq_map[freq])
        return bars
