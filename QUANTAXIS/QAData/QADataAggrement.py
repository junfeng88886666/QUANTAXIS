# coding:utf-8

import datetime
import pandas as pd

from QUANTAXIS.QAData import (QADataAggrement_CoFund)

from QUANTAXIS.QAUtil import DATABASE, QA_util_log_info
# TODO 当前只有COFund期货列表，日线和分钟线的数据协议

def use(package):
    if package in ['cofund', 'cof','CoFund','COFUND']:
        return QADataAggrement_CoFund
    else: raise NotImplementedError

def QA_DataAggrement_Future_day(package,DataFrame):
    Engine = use(package)
    return Engine.QA_DataAggrement_Future_day(DataFrame)

def QA_DataAggrement_Future_min(package,DataFrame):
    '''
    该数据协议为：返回的数据应包含以下列和对应的数据格式，若无该列数据，则填充0
        index:[str] 真实的datetime
        open [float] 开盘价
        high [float] 最高价
        low [float] 最低价
        close [float] 收盘价
        price [float] 结算价
        position [float] 持仓量
        trade [float] 交易量
        amount [float] 成交额
        datetime [str] 真实的datetime
        tradetime [str] 交易的datetime(晚上21点之后进入下一天)
        code [str] 品种代码
        contract [str] 品种合约
        date [str] 交易日期
        date_stamp [float] 日期的时间戳
        time_stamp [float] datetime的时间戳
        type [str] 级别
    :return: 经过数据协议调整格式后的国内期货分钟数据数据集
    '''
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Future_min(DataFrame)
        data = data[['index','open','high','low','close','price','position','trade','amount','datetime','tradetime','code','contract','date','date_stamp','time_stamp','type','source']]
        data['index'] = data['index'].astype(str)
        data['open'] = data['open'].astype('float64')
        data['high'] = data['high'].astype('float64')
        data['low'] = data['low'].astype('float64')
        data['close'] = data['close'].astype('float64')
        data['price'] = data['price'].astype('float64')
        data['position'] = data['position'].astype('float64')
        data['trade'] = data['trade'].astype('float64')
        data['amount'] = data['amount'].astype('float64')
        data['datetime'] = data['datetime'].astype(str)
        data['tradetime'] = data['tradetime'].astype(str)
        data['code'] = data['code'].astype(str)
        data['contract'] = data['contract'].astype(str)
        data['date'] = data['date'].astype(str)
        data['date_stamp'] = data['date_stamp'].astype('float64')
        data['time_stamp'] = data['time_stamp'].astype('float64')
        data['type'] = data['type'].astype(str)
        data['source'] = data['source'].astype(str)
        return data.set_index('index')
    except:
        print('QUANTAXIS DataAggrement Error')
        return None

def QA_DataAggrement_Future_list(package,DataFrame):
    Engine = use(package)
    return Engine.QA_DataAggrement_Future_list(DataFrame)
