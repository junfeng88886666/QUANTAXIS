# coding:utf-8

import datetime

import pandas as pd
from QUANTAXIS.QAUtil import (QA_util_date_stamp,
                              QA_util_date_str2int, QA_util_date_valid,
                              QA_util_get_real_date, QA_util_get_real_datelist,
                              QA_util_future_to_realdatetime, QA_util_future_to_tradedatetime,
                              QA_util_get_trade_gap, QA_util_log_info,
                              QA_util_time_stamp,trade_date_sse,
                              trade_date_sse,QA_util_get_trade_range,QA_util_listdir,QA_util_datetime_fixstr1,QA_util_date_int2str)
from QUANTAXIS.QAUtil.QAParameter import DATASOURCE
from QUANTAXIS.QAUtil import DATABASE, QA_util_log_info
# TODO 当前只有期货列表，日线和分钟线的数据协议

#%% STOCK_CN_PART

def QA_DataAggrement_Stock_day(data):
    data['source'] = DATASOURCE.JQDATA
    return data.rename(columns = {'vol':'volume'},inplace = False)

def QA_DataAggrement_Stock_min(data):
    data = data.reset_index().rename(columns={'index': 'datetime','money':'amount'})

    data = data.assign(date=data['datetime'].apply(lambda x: str(x)[0:10]))
    data = data \
        .assign(date_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(x))) \
        .assign(time_stamp=data['datetime'].apply(lambda x: QA_util_time_stamp(x)))
    data['source'] = DATASOURCE.JQDATA
    return data

def QA_DataAggrement_Stock_transaction(data):
    data['source'] = DATASOURCE.JQDATA
    return data.rename(columns = {'vol':'volume'},inplace = False)

def QA_DataAggrement_Stock_latest(data):
    data['source'] = DATASOURCE.JQDATA
    return data.rename(columns = {'vol':'volume'},inplace = False)

def QA_DataAggrement_Stock_realtime(data):
    data['source'] = DATASOURCE.JQDATA
    return data

def QA_DataAggrement_Stock_depth_market_data(data):
    data['source'] = DATASOURCE.JQDATA
    return data

def QA_DataAggrement_Stock_list(data):
    data['source'] = DATASOURCE.JQDATA
    if 'sec' not in data.columns: data['sec'] = 'uf'
    return data

def QA_DataAggrement_Stock_transaction_realtime(data):
    data['source'] = DATASOURCE.JQDATA
    return data.rename(columns = {'vol':'volume'},inplace = False)

def QA_DataAggrement_Stock_xdxr(data):
    data['source'] = DATASOURCE.JQDATA
    return data

def QA_DataAggrement_Stock_info(data):
    data['updated_date'] = list(map(lambda x: QA_util_date_int2str(x),data['updated_date']))
    data['ipo_date'] = list(map(lambda x: QA_util_date_int2str(x),data['ipo_date']))
    data['source'] = DATASOURCE.JQDATA
    return data

def QA_DataAggrement_Stock_block(data):
    data['enter_date'] = 'uf'
    data['source'] = DATASOURCE.JQDATA
    return data
#####################################################################################
#%% FUTURE_CN_PART

def QA_DataAggrement_Future_list(data):
    data['source'] = DATASOURCE.JQDATA
    return data

def QA_DataAggrement_Future_day(data):
    if 'contract' not in data.columns: data['contract'] = 'uf'
    data['trade']*=100
    data['source'] = DATASOURCE.JQDATA
    return data

def QA_DataAggrement_Future_transaction(data):
    del data['natrue_name']
    if 'contract' not in data.columns: data['contract'] = 'uf'
    data['source'] = DATASOURCE.JQDATAQAJQdata.py
    return data

def QA_DataAggrement_Future_min(data):
    if 'contract' not in data.columns: data['contract'] = 'uf'
    if 'position' not in data.columns: data['position'] = 0
    data = data.reset_index().rename(columns={'index': 'datetime','money':'amount'})

    data = data.assign(date=data['datetime'].apply(lambda x: str(x)[0:10]))
    data = data \
        .assign(tradetime=pd.to_datetime(data['datetime'].apply(QA_util_future_to_tradedatetime))) \
        .assign(date_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(x))) \
        .assign(time_stamp=data['datetime'].apply(lambda x: QA_util_time_stamp(x)))
    data['source'] = DATASOURCE.JQDATA
    return data