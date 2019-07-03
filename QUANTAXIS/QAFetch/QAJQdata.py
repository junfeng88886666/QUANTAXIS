# coding:utf-8
import json
import pandas as pd
import tushare as ts
from retrying import retry
import jqdatasdk
import pymongo

import time
from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_date_int2str,
    QA_util_date_stamp,
    QASETTING,
    QA_util_log_info,
    QA_util_to_json_from_pandas,
    QA_tuil_dateordatetime_valid
)
from QUANTAXIS.QAUtil.QAParameter import FREQUENCE,MARKET_TYPE,DATASOURCE,DATABASE_NAME

from QUANTAXIS.QAData.QADataAggrement import select_DataAggrement


DEFAULT_ACCOUNT = '13018055851'
DEFAULT_PASSWORD = 'LIWEIQI199'

def get_config(account = None, password = None, remember = False):
    try:
        if account is None: 
            account = QASETTING.get_config('JQDATA', 'account', None)
            if account == None: 
                QASETTING.set_config('JQDATA', 'account', DEFAULT_ACCOUNT)
                account = DEFAULT_ACCOUNT
        else: 
            if remember: QASETTING.set_config('JQDATA', 'account', account)
            
        if password is None: 
            password = QASETTING.get_config('JQDATA', 'password', None)
            if password == None: 
                QASETTING.set_config('JQDATA', 'password', DEFAULT_PASSWORD)
                password = DEFAULT_PASSWORD
        else: 
            if remember: QASETTING.set_config('JQDATA', 'password', password)
            
        jqdatasdk.auth(account,password)
    except:
        print('请升级jqdatasdk 至最新版本 pip install jqdatasdk -U')
        
def reset_config():
    QASETTING.set_config('JQDATA', 'account', DEFAULT_ACCOUNT)
    QASETTING.set_config('JQDATA', 'password', DEFAULT_PASSWORD)
    
    
def JQDATA_login(account = None, password = None, remember = False):
    get_config(account = account, password = password, remember = remember)

def reset_JQDOTA_code_compare(client = DATABASE,reset = False,ui_log = None):
    if reset: client.drop_collection('jqdata_securities_record')
    coll = client.jqdata_securities_record
    coll.create_index(
        [("jqcode",pymongo.ASCENDING),
         ("jqcode_simple",pymongo.ASCENDING),
         ("type",pymongo.ASCENDING)],
          unique = True
    )
    ref_ = coll.find()
    if (ref_.count() <= 0)|(reset == True):        
        security_info = jqdatasdk.get_all_securities(['stock', 'fund', 'index', 'futures', 'etf', 'lof', 'fja', 'fjb'])
        security_info = security_info.reset_index().rename(columns = {'index':'jqcode'})
        security_info['jqcode_simple'] = list(map(lambda x:x.split('.')[0],security_info['jqcode']))
        coll.insert_many(
                   QA_util_to_json_from_pandas(security_info)
                        )
   
        QA_util_log_info(
            '# Saving success: jqdata_securities_record',
            ui_log
        )
    else:
        pass

reset_JQDOTA_code_compare(reset = False)

def _QA_fetch_jqdata_securities_record(jqcode_simple = 'RB9999',type_ = 'futures',client = DATABASE,ui_log = None):
    coll = client.jqdata_securities_record
    ref_ = coll.find({'jqcode_simple':jqcode_simple,'type':type_})
    if (ref_.count() <= 0): 
        QA_util_log_info('# jqcode_simple: {}, type: {}, is not recorded in jqdata_securities_record'
       .format(jqcode_simple,type_),ui_log)
    elif (ref_.count() > 1):
        QA_util_log_info('# jqcode_simple: {}, type: {}, has {} records in jqdata_securities_record'
       .format(jqcode_simple,type_,ref_.count()),ui_log)
    else:
        return list(ref_)[0]

def _QA_code_toJQDATA(code,type_):
    try:
        if type_ == 'futures':
            if code[-2:] == 'L8': jqcode_simple = code[:-2]+str('9999')
            elif code[-2:] == 'L9': jqcode_simple = code[:-2] + str('8888')
        else:
            jqcode_simple = code
        jqdata_securities_record = _QA_fetch_jqdata_securities_record(jqcode_simple,type_)
        return jqdata_securities_record['jqcode']
    except Exception as e:
        QA_util_log_info(e)
        return code
    
def _QA_freq_toJQDATA(frequence):
    if frequence in ['day', 'd', 'D', 'DAY', 'Day']: return '1d'
    elif frequence in ['w', 'W', 'Week', 'week']: return None
    elif frequence in ['month', 'M', 'm', 'Month']: return None
    elif frequence in ['quarter', 'Q', 'Quarter', 'q']: return None
    elif frequence in ['y', 'Y', 'year', 'Year']: return None
    elif str(frequence) in ['1', '1m', '1min', 'one']: return '1m'
    elif str(frequence) in ['5', '5m', '5min', 'five']: return '5m'
    elif str(frequence) in ['15', '15m', '15min', 'fifteen']: return '15m'
    elif str(frequence) in ['30', '30m', '30min', 'half']: return '30m'
    elif str(frequence) in ['60', '60m', '60min', '1h']: return '60m'
        
        
@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_min(code, start, end, frequence='1min', fill_data_with_tick_database = False, fill_data_with_tick_online = False, method = 'api',account=None, password=None, remember = False):
    assert QA_tuil_dateordatetime_valid(start), 'start input format error'
    assert QA_tuil_dateordatetime_valid(end), 'end input format error'

    if method == 'api':
        JQDATA_login(account = account, password = password, remember = remember)
        jqcode = _QA_code_toJQDATA(code,'stock')
        jqfrequence = _QA_freq_toJQDATA(frequence)

        data = jqdatasdk.get_price(security = jqcode,
                                   start_date=start,
                                   end_date=end,
                                   frequency=jqfrequence,
                                   fields=None,
                                   skip_paused=True,
                                   fq=None,
                                   count=None)
        data['code'] = code
        data['type'] = frequence
    elif method == 'http':
        raise NotImplementedError
    return select_DataAggrement(DATABASE_NAME.STOCK_MIN)(DATASOURCE.JQDATA,data)


@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_future_min(code, start, end, frequence='1min', fill_data_with_tick_database = False, fill_data_with_tick_online = False, method = 'api', account=None, password=None, remember = False):
    assert QA_tuil_dateordatetime_valid(start), 'start input format error'
    assert QA_tuil_dateordatetime_valid(end), 'end input format error'

    if method == 'api':
        JQDATA_login(account = account, password = password, remember = remember)
        jqcode = _QA_code_toJQDATA(code,'futures')
        jqfrequence = _QA_freq_toJQDATA(frequence)
        data = jqdatasdk.get_price(security = jqcode,
                                   start_date=start,
                                   end_date=end,
                                   frequency=jqfrequence,
                                   fields=None,
                                   skip_paused=True,
                                   fq=None,
                                   count=None)
        data['code'] = code
        data['type'] = frequence
    elif method == 'http':
        raise NotImplementedError
    return select_DataAggrement(DATABASE_NAME.FUTURE_MIN)(DATASOURCE.JQDATA,data)

#if __name__ =='__main__':
#    start = '2019-06-27'
#    end = '2019-06-28'
#    frequence = '1min'
#
#    DATA=QA_fetch_get_stock_min('000001',start,end,frequence)
#    DATA=QA_fetch_get_future_min('RBL8',start,end,frequence)
    

"""

get_price

可查询股票、基金、指数、期货的历史及当前交易日的行情数据

可指定单位时间长度，如一天、一分钟、五分钟等

可查询开盘价、收盘价、最高价、最低价、成交量、成交额、涨停、跌停、均价、前收价、是否停牌

支持不同的复权方式

​

get_trade_days

查询指定时间范围的交易日

​

get_all_trade_days

查询所有的交易日

​

get_extras

查询股票是否是ST

查询基金的累计净值、单位净值

查询期货的结算价、持仓量

​

get_index_stocks

查询指定指数在指定交易日的成分股

​

get_industry_stocks

查询指定行业在指定交易日的成分股

​

get_industries

查询行业列表

​

get_concept_stocks

查询指定概念在指定交易日的成分股

​

get_concepts

查询概念列表

​

get_all_securities

查询股票、基金、指数、期货列表

​

get_security_info

查询单个标的的信息

​

get_money_flow

查询某只股票的资金流向数据

​

get_fundamentals

查询财务数据，包含估值表、利润表、现金流量表、资产负债表、银行专项指标、证券专项指标、保险专项指标

​

get_fundamentals_continuously

查询多日的财务数据

​

get_mtss

查询股票、基金的融资融券数据

​

get_billbord_list

查询股票龙虎榜数据

​

get_locked_shares

查询股票限售解禁股数据

​

get_margincash_stocks

获取融资标的列表


get_marginsec_stocks

获取融券标的列表

​

get_future_contracts

查询期货可交易合约列表

​

get_dominant_future

查询主力合约对应的标的

​

get_ticks

查询股票、期货的tick数据

​

normalize_code

归一化证券编码

​

macro.run_query

查询宏观经济数据，具体数据见官网API https://www.joinquant.com/data/dict/macroData

​

alpha101

查询WorldQuant 101 Alphas 因子数据，具体因子解释见官网API https://www.joinquant.com/data/dict/alpha101

​

alpha191

查询短周期价量特征 191 Alphas 因子数据，具体因子解释见官网API https://www.joinquant.com/data/dict/alpha191

​

technical_analysis

技术分析指标，具体因子解释见官网API https://www.joinquant.com/data/dict/technicalanalysis

​

​

baidu_factor

查询股票某日百度搜索量数据
"""