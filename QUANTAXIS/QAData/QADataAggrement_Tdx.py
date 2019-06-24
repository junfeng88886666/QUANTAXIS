# coding:utf-8

import datetime

import pandas as pd
from QUANTAXIS.QAUtil import (QA_util_date_stamp,
                              QA_util_date_str2int, QA_util_date_valid,
                              QA_util_get_real_date, QA_util_get_real_datelist,
                              QA_util_future_to_realdatetime, QA_util_future_to_tradedatetime,
                              QA_util_get_trade_gap, QA_util_log_info,
                              QA_util_time_stamp,trade_date_sse,
                              trade_date_sse,QA_util_get_trade_range,QA_util_listdir,QA_util_datetime_fixstr1)
from QUANTAXIS.QAUtil.QAParameter import DATASOURCE
from QUANTAXIS.QAUtil import DATABASE, QA_util_log_info
# TODO 当前只有期货列表，日线和分钟线的数据协议

def QA_DataAggrement_Stock_day(data):
    return data



def QA_DataAggrement_Future_day(data):
    raise NotImplementedError

def QA_DataAggrement_Future_min(data):
    return data

def QA_DataAggrement_Future_list(data):
    raise NotImplementedError
