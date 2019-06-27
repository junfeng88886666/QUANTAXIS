# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 13:22:14 2019

@author: Administrator
"""

import QUANTAXIS as QA
data = QA.QAFetch.QATdx.QA_fetch_get_stock_transaction('000001','2019-02-01','2019-02-03')
data['2019-02-01 09:25:00':'2019-02-01 15:00:00']
data['2019-02-01':'2019-02-01']
data = QA.QAFetch.QATdx.QA_fetch_get_stock_transaction('000001','2019-06-23 01:01:00','2019-06-27 02:01:00')
QA.QAFetch

import time
def QA_util_date_valid(date):
    """
    判断字符串是否是 1982-05-11 这种格式
    :param date: date 字符串str -- 格式 字符串长度10
    :return: boolean -- 格式是否正确
    """
    try:
        print(time.strptime(date, "%Y-%m-%d %H:%M:%S"))
        return True
    except:
        return False
    
    
QA_util_date_valid('2019-01-01 00:01:00')


def QA_util_date_valid(date):
    """
    判断字符串是否是 1982-05-11 这种格式
    :param date: date 字符串str -- 格式 字符串长度10
    :return: boolean -- 格式是否正确
    """
    try:
        time.strptime(date, "%Y-%m-%d")
        return True
    except:
        return False
def QA_util_datetime_valid(t):
    """
    判断字符串是否是 1982-05-11 01:01:01 这种格式
    :param date: date 字符串str -- 格式 字符串长度10
    :return: boolean -- 格式是否正确
    """
    try:
        time.strptime(t, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False

def QA_tuil_dateordatetime_valid(t):
    if QA_util_date_valid(t)|QA_util_datetime_valid(t): return True
    else: return False
    
assert QA_tuil_dateordatetime_valid('2019-01-01 '),print('日期输入错误')
from QUANTAXIS import QA_util_get_real_datelist
QA_util_get_real_datelist('2019-02-01 01:00:00','2019-04-30 21:00:00')
