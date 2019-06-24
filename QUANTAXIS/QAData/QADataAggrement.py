# coding:utf-8

import datetime
import pandas as pd

from QUANTAXIS.QAData import (QADataAggrement_CoFund)
from QUANTAXIS.QAUtil import DATABASE, QA_util_log_info
from QUANTAXIS.QAUtil.QAParameter import ERRORTYPE

# TODO 当前只有COFund期货列表，日线和分钟线的数据协议

def use(package):
    if package in ['cofund', 'cof','CoFund','COFUND']:
        return QADataAggrement_CoFund
    else: raise NotImplementedError

def QA_DataAggrement_Stock_day(package,DataFrame,ui_log = None):
    '''
    该数据协议为：返回的数据应包含以下列和对应的数据格式，若无该列数据，则填充0
        index:[str] 真实的交易日
        open [float64] 开盘价
        high [float64] 最高价
        low [float64] 最低价
        close [float64] 收盘价
        volume [float64] 成交量
        amount [float64] 成交额
        date [str] 真实的交易日
        date_stamp [float64] 真实的交易日的时间戳
        source [str] 数据来源
    :return: 经过数据协议调整格式后的国内期货分钟数据数据集
    '''
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Future_min(DataFrame)

        data.index = data.index.astype(str)

        data[['code',
              'date',
              'source']]\
        = data[['code',
                'date',
                'source']].astype(str)

        data[['open',
              'high',
              'low',
              'close',
              'volume',
              'amount',
              'date_stamp']]\
        = data[['open',
                'high',
                'low',
                'close',
                'volume',
                'amount',
                'date_stamp']].astype('float64')

        return data[['code','open','high','low','close','volume','amount','date','date_stamp','source']]
    except:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR+', package: '+str(package), ui_log=ui_log)
        return None

def QA_DataAggrement_Future_day(package,DataFrame):
    Engine = use(package)
    return Engine.QA_DataAggrement_Future_day(DataFrame)

def QA_DataAggrement_Future_min(package,DataFrame,ui_log = None):
    '''
    该数据协议为：返回的数据应包含以下列和对应的数据格式，若无该列数据，则填充0
        index:[str] 真实的交易时间
        open [float64] 开盘价
        high [float64] 最高价
        low [float64] 最低价
        close [float64] 收盘价
        price [float64] 结算价
        position [float64] 持仓量
        trade [float64] 交易量
        amount [float64] 成交额
        datetime [str] 真实的交易时间
        tradetime [str] 交易的交易时间(晚上21点之后进入下一天)
        code [str] 品种代码
        contract [str] 品种合约
        date [str] 交易日期
        date_stamp [float64] 真实的交易日期的时间戳
        time_stamp [float64] 真实的交易时间的时间戳
        type [str] 级别
        source [str] 数据来源
    :return: 经过数据协议调整格式后的国内期货分钟数据数据集
    '''
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Future_min(DataFrame)

        data[['index',
              'datetime',
              'tradetime',
              'code',
              'contract',
              'date',
              'type',
              'source']]\
        = data[['index',
                'datetime',
                'tradetime',
                'code',
                'contract',
                'date',
                'type',
                'source']].astype(str)

        data[['open',
              'high',
              'low',
              'close',
              'price',
              'position',
              'trade',
              'amount',
              'date_stamp',
              'time_stamp']] \
        = data[['open',
                'high',
                'low',
                'close',
                'price',
                'position',
                'trade',
                'amount',
                'date_stamp',
                'time_stamp']].astype('float64')

        data = data[['index','open','high','low','close','price','position','trade','amount','datetime','tradetime','code','contract','date','date_stamp','time_stamp','type','source']]

        return data.set_index('index')
    except:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR+', package: '+str(package), ui_log=ui_log)
        return None

def QA_DataAggrement_Future_list(package,DataFrame):
    Engine = use(package)
    return Engine.QA_DataAggrement_Future_list(DataFrame)
