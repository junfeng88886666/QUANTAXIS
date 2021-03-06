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
    data['source'] = DATASOURCE.TDX
    return data.rename(columns = {'vol':'volume'},inplace = False)

def QA_DataAggrement_Stock_min(data):
    data['source'] = DATASOURCE.TDX
    return data.rename(columns = {'vol':'volume'},inplace = False)

def QA_DataAggrement_Stock_transaction(data):
    data['source'] = DATASOURCE.TDX
    return data.rename(columns = {'vol':'volume'},inplace = False)

def QA_DataAggrement_Stock_latest(data):
    data['source'] = DATASOURCE.TDX
    return data.rename(columns = {'vol':'volume'},inplace = False)

def QA_DataAggrement_Stock_realtime(data):
    data['source'] = DATASOURCE.TDX
    return data

def QA_DataAggrement_Stock_depth_market_data(data):
    data['source'] = DATASOURCE.TDX
    return data

def QA_DataAggrement_Stock_list(data):
    data['source'] = DATASOURCE.TDX
    if 'sec' not in data.columns: data['sec'] = 'uf'
    return data

def QA_DataAggrement_Stock_transaction_realtime(data):
    data['source'] = DATASOURCE.TDX
    return data.rename(columns = {'vol':'volume'},inplace = False)

def QA_DataAggrement_Stock_xdxr(data):
    data['source'] = DATASOURCE.TDX
    return data

def QA_DataAggrement_Stock_info(data):
    data['updated_date'] = list(map(lambda x: QA_util_date_int2str(x),data['updated_date']))
    data['ipo_date'] = list(map(lambda x: QA_util_date_int2str(x),data['ipo_date']))
    data['source'] = DATASOURCE.TDX
    return data

def QA_DataAggrement_Stock_block(data):
    data['enter_date'] = 'uf'
    data['source'] = DATASOURCE.TDX
    return data
#####################################################################################
#%% FUTURE_CN_PART

def QA_DataAggrement_Future_list(data):
    data['source'] = DATASOURCE.TDX
    return data

def QA_DataAggrement_Future_day(data):
    data = data.rename(columns={'trade':'volume'})
    if 'contract' not in data.columns: data['contract'] = 'uf'
    data['volume']*=100
    data['source'] = DATASOURCE.TDX
    return data

def QA_DataAggrement_Future_transaction(data):
    del data['natrue_name']
    if 'contract' not in data.columns: data['contract'] = 'uf'
    data['source'] = DATASOURCE.TDX
    return data

def QA_DataAggrement_Future_min(data):
    data = data.rename(columns={'trade':'volume'})
    if 'contract' not in data.columns: data['contract'] = 'uf'
    data['amount'] = 0
    data['source'] = DATASOURCE.TDX
    return data