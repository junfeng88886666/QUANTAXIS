# coding:utf-8

import pandas as pd
import numpy as np
import copy
'''
输入对对应的单品种data_struc.groupby('date')
输出为单列dataframe,索引为date,列名为factor_id
'''
class QA_factor_future_day_func_1():
    def __init__(self,
                 factor_cn_name=None,
                 factor_en_name=None,
                 factor_create_time=None,
                 data_engine_name=None,
                 data_engine_backward_length=None):
        '''定义因子基本属性,有必填项'''
        self.factor_cn_name = '多仓基尼系数'
        self.factor_en_name = 'positive_position_gink_coefficient'
        self.comment = None
        self.factor_id = 'factor_future_day_1'
        self.factor_create_time = '2019-06-12 16:50:00'
        self.data_engine_name = 'QA_fetch_future_transaction_adv'
        self.data_engine_backward_day = 1
        self.data_engine_backward_min = 0

    def get(self, data=None, start=None, end=None):
        '''
        data = copy.deepcopy(check)
        '''
        # if start != None: data=data['tradetime']>=start
        # if end != None: data=data['tradetime']<=end

        # if 'minute' not in data.columns: data['minute'] = list(map(lambda x: str(x)[11:], data['datetime']))
        temp = data[data['nature_name'].isin(['多开', '多换'])]
        temp['close'] = temp.groupby('date')['price'].transform('last')
        temp['ret'] = (temp['close'] / temp['price'])  # *temp['volume']

        def get_gink(df):
            df = df.sort_values(by='volume')
            wealths = df['ret'].tolist()
            cum_wealths = np.cumsum(sorted(np.append(wealths, 0)))
            sum_wealths = cum_wealths[-1]
            xarray = np.array(range(0, len(cum_wealths))) / np.float(len(cum_wealths) - 1)
            upper = copy.deepcopy(xarray)
            yarray = cum_wealths / sum_wealths
            B = np.trapz(yarray, x=xarray)
            A = 0.5 - B
            G = A / (A + B)
            df['gnik'] = G
            return df

        temp = temp.groupby('date').apply(get_gink)
        return pd.DataFrame(temp.reset_index(drop=True).groupby('date')['gnik'].last()).rename(
            columns={'gnik': self.factor_id})


class QA_factor_future_day_func_2():
    def __init__(self,
                 factor_cn_name=None,
                 factor_en_name=None,
                 factor_create_time=None,
                 data_engine_name=None,
                 data_engine_backward_length=None):
        '''定义因子基本属性,有必填项'''
        self.factor_cn_name = '空仓基尼系数'
        self.factor_en_name = 'negative position gink coefficient'
        self.comment = None
        self.factor_id = 'factor_future_day_2'
        self.factor_create_time = '2019-06-12 16:50:00'
        self.data_engine_name = 'QA_fetch_future_transaction_adv'
        self.data_engine_backward_day = 1
        self.data_engine_backward_min = 0

    def get(self, data=None, start=None, end=None):
        '''
        data = copy.deepcopy(check)
        '''
        # if start != None: data=data['tradetime']>=start
        # if end != None: data=data['tradetime']<=end

        # if 'minute' not in data.columns: data['minute'] = list(map(lambda x: str(x)[11:], data['datetime']))
        temp = data[data['nature_name'].isin(['空开', '空换'])]
        temp['close'] = temp.groupby('date')['price'].transform('last')
        temp['ret'] = (temp['price'] / temp['close'])  # *temp['volume']

        def get_gink(df):
            df = df.sort_values(by='volume')
            wealths = df['ret'].tolist()
            cum_wealths = np.cumsum(sorted(np.append(wealths, 0)))
            sum_wealths = cum_wealths[-1]
            xarray = np.array(range(0, len(cum_wealths))) / np.float(len(cum_wealths) - 1)
            upper = copy.deepcopy(xarray)
            yarray = cum_wealths / sum_wealths
            B = np.trapz(yarray, x=xarray)
            A = 0.5 - B
            G = A / (A + B)
            df['gnik'] = G
            return df

        temp = temp.groupby('date').apply(get_gink)
        return pd.DataFrame(temp.reset_index(drop=True).groupby('date')['gnik'].last()).rename(
            columns={'gnik': self.factor_id})


class QA_factor_future_day_func_3():
    def __init__(self,
                 factor_cn_name=None,
                 factor_en_name=None,
                 factor_create_time=None,
                 data_engine_name=None,
                 data_engine_backward_length=None):
        '''定义因子基本属性,有必填项'''
        self.factor_cn_name = '全天走势形态分类'
        self.factor_en_name = 'All-day trend form classification'
        self.comment = '1, 全天定义为21点或9点或9点半开盘; 2, 样本内必为<2018-01-01的日期，后面的数据都是根据判断往之前的聚类中插入数据的'
        self.factor_id = 'factor_future_day_3'
        self.factor_create_time = '2019-06-13 11:17:00'
        self.data_engine_name = 'QA_fetch_future_min_adv'
        self.data_engine_backward_day = 1
        self.data_engine_backward_min = 0

    def get(self, data=None, start=None, end=None):
        '''
        data = copy.deepcopy(check)
        '''
        '''先读取最优簇数，没有则计算生成最优簇数'''
        '''样本内生成簇，并存储簇的中心，以便样本外生成因子时判断样本所属的簇'''

        pass
