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
from QUANTAXIS.QAFetch import QAThs as QAThs
from QUANTAXIS.QAFetch import QACrawler as QACL
from QUANTAXIS.QAFetch import QAEastMoney as QAEM
from QUANTAXIS.QAFetch import QAHexun as QAHexun
from QUANTAXIS.QAFetch import QAfinancial
from QUANTAXIS.QAFetch.base import get_stock_market

def use(package):
    if package in ['wind']:
        try:
            from WindPy import w
            # w.start()
            return QAWind
        except ModuleNotFoundError:
            print('NO WIND CLIENT FOUND')
    elif package in ['tushare', 'ts']:
        return QATushare
    elif package in ['tdx', 'pytdx']:
        return QATdx
    elif package in ['ths', 'THS']:
        return QAThs
    elif package in ['HEXUN', 'Hexun', 'hexun']:
        return QAHexun
    elif package in ['cofund', 'cof','CoFund','COFUND']:
        return QACoFund

def QA_fetch_get_stock_day(package, code, start, end, if_fq='01', level='day', type_='pd'):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_day(code, start, end, if_fq, level)
    except:
        return 'Unsupport packages'

def QA_fetch_get_stock_realtime(package, code):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_realtime(code)
    except:
        return 'Unsupport packages'

def QA_fetch_get_stock_latest(package,code,frequence = 'day'):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_latest(code,frequence)
    except:
        return 'Unsupport packages'

def QA_fetch_get_stock_indicator(package, code, start, end):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_indicator(code, start, end)
    except:
        return 'Unsupport packages'

def QA_fetch_get_trade_date(package, end, exchange):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_trade_date(end, exchange)
    except:
        return 'Unsupport packages'

def QA_fetch_get_stock_min(package, code, start, end, level='1min'):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_min(code, start, end, level)
    except:
        return 'Unsupport packages'


def QA_fetch_get_stock_transaction(package, code, start, end, retry=2):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_transaction(code, start, end, retry)
    except:
        return 'Unsupport packages'


def QA_fetch_get_stock_transaction_realtime(package, code):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_transaction_realtime(code)
    except:
        return 'Unsupport packages'


def QA_fetch_get_stock_xdxr(package, code):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_xdxr(code)
    except:
        return 'Unsupport packages'


def QA_fetch_get_index_day(package, code, start, end, level='day'):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_index_day(code, start, end, level)
    except:
        return 'Unsupport packages'


def QA_fetch_get_index_min(package, code, start, end, level='1min'):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_index_min(code, start, end, level)
    except:
        return 'Unsupport packages'

def QA_fetch_get_stock_block(package):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_block()
    except:
        return 'Unsupport packages'


def QA_fetch_get_stock_info(package, code):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_info(code)
    except:
        return 'Unsupport packages'

# LIST


def QA_fetch_get_stock_list(package, type_='stock'):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_stock_list(type_)
    except:
        return 'Unsupport packages'

def QA_fetch_get_bond_list(package):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_bond_list()
    except:
        return 'Unsupport packages'

def QA_fetch_get_index_list(package):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_index_list()
    except:
        return 'Unsupport packages'

def QA_fetch_get_future_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_future_list()
    except:
        return 'Unsupport packages'


def QA_fetch_get_option_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_option_list()
    except:
        return 'Unsupport packages'

def QA_fetch_get_globalfuture_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_globalfuture_list()
    except:
        return 'Unsupport packages'


def QA_fetch_get_hkstock_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_hkstock_list()
    except:
        return 'Unsupport packages'

def QA_fetch_get_hkfund_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_hkfund_list()
    except:
        return 'Unsupport packages'


def QA_fetch_get_hkindex_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_hkindex_list()
    except:
        return 'Unsupport packages'


def QA_fetch_get_usstock_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_usstock_list()
    except:
        return 'Unsupport packages'


def QA_fetch_get_macroindex_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_macroindex_list()
    except:
        return 'Unsupport packages'

def QA_fetch_get_globalindex_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_globalindex_list()
    except:
        return 'Unsupport packages'

def QA_fetch_get_exchangerate_list(package,):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_exchangerate_list()
    except:
        return 'Unsupport packages'


#######################

def QA_fetch_get_security_bars(package,code, _type, lens):
    return QATdx.QA_fetch_get_security_bars(code, _type, lens)


def QA_fetch_get_future_transaction(package, code, start, end):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_future_transaction(code, start, end)
    except:
        return 'Unsupport packages'

def QA_fetch_get_future_transaction_realtime(package, code):
    """
    期货实时tick
    """
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_future_transaction_realtime(code)
    except:
        return 'Unsupport packages'

def QA_fetch_get_future_realtime(package, code):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_future_realtime(code)
    except:
        return 'Unsupport packages'

def QA_fetch_get_future_day(package, code, start, end, frequence='day'):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_future_day(code, start, end, frequence=frequence)
    except:
        return 'Unsupport packages'


def QA_fetch_get_future_min(package, code, start, end, frequence='1min'):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_future_min(code, start, end, frequence=frequence)
    except:
        return 'Unsupport packages'

def QA_fetch_get_chibor(package, frequence):
    try:
        Engine = use(package)
        return Engine.QA_fetch_get_chibor(frequence)
    except:
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
