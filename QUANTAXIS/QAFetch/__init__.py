# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2018 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
QA fetch module

@yutiansut

QAFetch is Under [QAStandard#0.0.2@10x] Protocol


"""
from QUANTAXIS.QAFetch import QACoFund as QACoFund
from QUANTAXIS.QAFetch import QAWind as QAWind
from QUANTAXIS.QAFetch import QATushare as QATushare
from QUANTAXIS.QAFetch import QATdx as QATdx
from QUANTAXIS.QAFetch import QAJQdata as QAJQdata
from QUANTAXIS.QAFetch import QAThs as QAThs
from QUANTAXIS.QAFetch import QACrawler as QACL
from QUANTAXIS.QAFetch import QAEastMoney as QAEM
from QUANTAXIS.QAFetch import QAHexun as QAHexun
from QUANTAXIS.QAFetch import QAfinancial
from QUANTAXIS.QAFetch.base import get_stock_market
from QUANTAXIS.QAUtil.QAParameter import DATASOURCE_DEFAULT,DATASOURCE

def use(package):

    if package in ['wind',DATASOURCE.WIND]:
        try:
            from WindPy import w
            # w.start()
            return QAWind
        except ModuleNotFoundError:
            print('NO WIND CLIENT FOUND')
    elif package in ['tushare', 'ts',DATASOURCE.TUSHARE]:
        return QATushare
    elif package in ['tdx', 'pytdx',DATASOURCE.TDX]:
        return QATdx
    elif package in ['ths', 'THS',DATASOURCE.THS]:
        return QAThs
    elif package in ['HEXUN', 'Hexun', 'hexun',DATASOURCE.TUSHARE]:
        return QAHexun
    elif package in ['cofund', 'cof','CoFund','COFUND',DATASOURCE.COFUND]:
        return QACoFund
    elif package in ['JQDATA', 'JQdata', 'JQ', 'jq', 'jqdata',DATASOURCE.JQDATA]:
        return QAJQdata


def _check_func_useful(func,package):
    if func not in dir(DATASOURCE_DEFAULT): assert False,'Unsupport function'
    else:
        if package in ['default','None',None]:
            return eval("DATASOURCE_DEFAULT.{}".format(func))
        else:
            return package


def QA_fetch_get_security_bars(package,code, _type, lens):
    '''目前仅仅有通达信数据有'''
    try:
        package = _check_func_useful('QA_fetch_get_security_bars',package)
        Engine = use(package)
        return Engine.QA_fetch_get_security_bars(code, _type, lens)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_trade_date(package, end, exchange):
    try:
        package = _check_func_useful('QA_fetch_get_security_bars',package)
        Engine = use(package)
        return Engine.QA_fetch_get_trade_date(end, exchange)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_stock_indicator(package, code, start, end):
    try:
        package = _check_func_useful('QA_fetch_get_stock_indicator',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_indicator(code, start, end)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

#%% STOCK_CN_PART
def QA_fetch_get_stock_list(package, type_='stock'):
    try:
        package = _check_func_useful('QA_fetch_get_stock_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_list(type_)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_stock_day(package, code, start, end, if_fq='00', level='day', type_='pd'):
    try:
        package = _check_func_useful('QA_fetch_get_stock_day',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_day(code, start, end, if_fq, level)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_stock_min(package, code, start, end,level='1min', fill_data_with_tick_database = False, fill_data_with_tick_online = False):
    try:
        package = _check_func_useful('QA_fetch_get_stock_min',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_min(code, start, end, level, fill_data_with_tick_database, fill_data_with_tick_online)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_stock_realtime(package, code):
    try:
        package = _check_func_useful('QA_fetch_get_stock_realtime',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_realtime(code)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_stock_latest(package,code,frequence = 'day'):
    try:
        package = _check_func_useful('QA_fetch_get_stock_latest',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_latest(code,frequence)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_stock_transaction(package, code, start, end, frequence = None, retry=2):
    try:
        package = _check_func_useful('QA_fetch_get_stock_transaction',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_transaction(code, start, end, frequence, retry)
    except Exception as e:
        print(e)
        return 'Unsupport packages'


def QA_fetch_get_stock_transaction_realtime(package, code):
    try:
        package = _check_func_useful('QA_fetch_get_stock_transaction_realtime',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_transaction_realtime(code)
    except Exception as e:
        print(e)
        return 'Unsupport packages'


def QA_fetch_get_stock_xdxr(package, code):
    try:
        package = _check_func_useful('QA_fetch_get_stock_xdxr',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_xdxr(code)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_stock_block(package):
    try:
        package = _check_func_useful('QA_fetch_get_stock_block',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_block()
    except Exception as e:
        print(e)
        return 'Unsupport packages'


def QA_fetch_get_stock_info(package, code):
    try:
        package = _check_func_useful('QA_fetch_get_stock_info',package)
        Engine = use(package)
        return Engine.QA_fetch_get_stock_info(code)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

#%% FUTURE_CN_PART
def QA_fetch_get_future_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_future_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_future_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_future_day(package, code, start, end, frequence='day'):
    try:
        package = _check_func_useful('QA_fetch_get_future_day',package)
        Engine = use(package)
        return Engine.QA_fetch_get_future_day(code, start, end, frequence = frequence)
    except Exception as e:
        print(e)
        return 'Unsupport packages'


def QA_fetch_get_future_transaction(package, code, start, end, frequence = None):
    try:
        package = _check_func_useful('QA_fetch_get_future_transaction',package)
        Engine = use(package)
        return Engine.QA_fetch_get_future_transaction(code, start, end, frequence)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_future_min(package, code, start, end, frequence='1min', fill_data_with_tick_database = False, fill_data_with_tick_online = False):
    try:
        package = _check_func_useful('QA_fetch_get_future_min',package)
        Engine = use(package)
        return Engine.QA_fetch_get_future_min(code, start, end, frequence, fill_data_with_tick_database, fill_data_with_tick_online)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_future_transaction_realtime(package, code):
    """
    期货实时tick
    """
    try:
        package = _check_func_useful('QA_fetch_get_future_transaction_realtime',package)
        Engine = use(package)
        return Engine.QA_fetch_get_future_transaction_realtime(code)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_future_realtime(package, code):
    try:
        package = _check_func_useful('QA_fetch_get_future_realtime',package)
        Engine = use(package)
        return Engine.QA_fetch_get_future_realtime(code)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

#%% INDEX_CN_PART
def QA_fetch_get_index_list(package):
    try:
        package = _check_func_useful('QA_fetch_get_index_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_index_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_index_day(package, code, start, end, level='day'):
    try:
        package = _check_func_useful('QA_fetch_get_index_day',package)
        Engine = use(package)
        return Engine.QA_fetch_get_index_day(code, start, end, level)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_index_min(package, code, start, end, level='1min'):
    try:
        package = _check_func_useful('QA_fetch_get_index_min',package)
        Engine = use(package)
        return Engine.QA_fetch_get_index_min(code, start, end, level)
    except Exception as e:
        print(e)
        return 'Unsupport packages'


#%% BOND_CN_PART
def QA_fetch_get_bond_list(package):
    try:
        package = _check_func_useful('QA_fetch_get_bond_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_bond_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'

#%% OPTION_CN_PART
def QA_fetch_get_option_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_option_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_option_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'

#%% GLOBAL_PART
def QA_fetch_get_globalfuture_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_globalfuture_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_globalfuture_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_hkstock_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_hkstock_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_hkstock_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_hkfund_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_hkfund_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_hkfund_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'


def QA_fetch_get_hkindex_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_hkindex_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_hkindex_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'


def QA_fetch_get_usstock_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_usstock_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_usstock_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'


def QA_fetch_get_macroindex_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_macroindex_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_macroindex_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_globalindex_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_globalindex_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_globalindex_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'

def QA_fetch_get_exchangerate_list(package,):
    try:
        package = _check_func_useful('QA_fetch_get_exchangerate_list',package)
        Engine = use(package)
        return Engine.QA_fetch_get_exchangerate_list()
    except Exception as e:
        print(e)
        return 'Unsupport packages'

#######################


def QA_fetch_get_chibor(package, frequence):
    try:
        package = _check_func_useful('QA_fetch_get_chibor',package)
        Engine = use(package)
        return Engine.QA_fetch_get_chibor(frequence)
    except Exception as e:
        print(e)
        return 'Unsupport packages'

QA_fetch_get_option_day = QA_fetch_get_future_day
QA_fetch_get_option_min = QA_fetch_get_future_min

QA_fetch_get_hkstock_day = QA_fetch_get_future_day
QA_fetch_get_hkstock_min = QA_fetch_get_future_min

QA_fetch_get_hkfund_day = QA_fetch_get_future_day
QA_fetch_get_hkfund_min = QA_fetch_get_future_min

QA_fetch_get_hkindex_day = QA_fetch_get_future_day
QA_fetch_get_hkindex_min = QA_fetch_get_future_min


QA_fetch_get_usstock_day = QA_fetch_get_future_day
QA_fetch_get_usstock_min = QA_fetch_get_future_min

QA_fetch_get_option_day = QA_fetch_get_future_day
QA_fetch_get_option_min = QA_fetch_get_future_min

QA_fetch_get_globalfuture_day = QA_fetch_get_future_day
QA_fetch_get_globalfuture_min = QA_fetch_get_future_min

QA_fetch_get_exchangerate_day = QA_fetch_get_future_day
QA_fetch_get_exchangerate_min = QA_fetch_get_future_min


QA_fetch_get_macroindex_day = QA_fetch_get_future_day
QA_fetch_get_macroindex_min = QA_fetch_get_future_min


QA_fetch_get_globalindex_day = QA_fetch_get_future_day
QA_fetch_get_globalindex_min = QA_fetch_get_future_min
