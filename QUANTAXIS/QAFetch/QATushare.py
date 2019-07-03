# coding: utf-8
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

import json
import pandas as pd
import tushare as ts
from retrying import retry

import time
from QUANTAXIS.QAUtil import (
    QA_util_date_int2str,
    QA_util_date_stamp,
    QASETTING,
    QA_util_log_info,
    QA_util_to_json_from_pandas
)
DEFAULT_TUSHARE_TOKEN = '32b744d16a99d60a9eb04079ed703ccbe1f8b516f9daae54d2a52c7c'

'''
只需要完成以下数据获取：
股票tick数据
股票分钟数据
期货tick数据
期货分钟数据
'''
pro = ts.pro_api(DEFAULT_TUSHARE_TOKEN)

df = pro.fut_basic(exchange='CFFEX', fut_type='2', fields='ts_code,symbol,name,list_date,delist_date')

def set_token(token=DEFAULT_TUSHARE_TOKEN):
    try:
        if token is None:
            # 从~/.quantaxis/setting/config.ini中读取配置
            token = QASETTING.get_config('TSPRO', 'token', None)
            if token == 'null': 
                token = DEFAULT_TUSHARE_TOKEN
                QASETTING.set_config('TSPRO', 'token', DEFAULT_TUSHARE_TOKEN)
        else:
            QASETTING.set_config('TSPRO', 'token', token)
        ts.set_token(token)
    except:
        print('请升级tushare 至最新版本 pip install tushare -U')


def get_pro(token):
    try:
        set_token(token)
        pro = ts.pro_api()
    except Exception as e:
        if isinstance(e, NameError):
            print('请设置正确的tushare pro的token凭证码')
        else:
            print('请升级tushare 至最新版本 pip install tushare -U')
            print(e)
        pro = None
    return pro
def QA_fetch_get_stock_info():
    raise NotImplementedError
def QA_fetch_get_stock_tsbasics(code = None,token = None):
    pro = get_pro(token)
    data = ts.get_stock_basics().reset_index()
    data = data.sort_values(by='code')
    try:
        if code != None: return data.loc[code]
        else: return data
    except:
        return None
    
def _QA_code_toTushare(code,type_,token = None):
    pro = get_pro(token)
    exchange_list = ['CFFEX','DCE','CZCE','SHFE','INE']
    code = str(code)
    if type_ == 'E':
        code = code.zfill(6)
        if code[0] in ['0','3']: return code+'.SZ'
        elif code[0] in ['6']: return code+'.SH'
    elif type_ == 'FT':
        if code[-2:] in ['L8']: 
            code = code[:-2]
            for ex in exchange_list:
                fut_list = pro.fut_basic(exchange=ex, fut_type='2', fields='symbol')['symbol'].tolist()
                if code in fut_list:
                    return code+'.'+ex
        elif code[-2:] in ['L9']: 
            code = code[:-2]+'L'
            for ex in exchange_list:
                fut_list = pro.fut_basic(exchange=ex, fut_type='2', fields='symbol')['symbol'].tolist()
                if code in fut_list:
                    return code+'.'+ex    
        else:
            for ex in exchange_list:
                fut_list = pro.fut_basic(exchange=ex, fut_type='1', fields='symbol')['symbol'].tolist()
                if code in fut_list:
                    return code+'.'+ex        
        
#def QA_fetch_get_stock_min():
#    pass
#
#def QA_fetch_get_future_min():
#    pass
# =============================================================================
# 
#pro = ts.pro_api()
#df = pro.query('daily', ts_code='CSL.DCE', start_date='20180601', end_date='20190701')
#
#df = pro.fut_basic(exchange='DCE', fut_type='1', fields='symbol')['symbol'].tolist()
#
#from jqdatasdk import *
#auth('13018055851','LIWEIQI199')
#from jqdatasdk import finance
#finance.run_query(query(finance.FUT_MEMBER_POSITION_RANK).filter(finance.FUT_MEMBER_POSITION_RANK.code==code).limit(n))
#a = get_price(security = 'RB9999.XSGE', 
#              start_date='2015-01-01', 
#              end_date='2019-07-01', 
#              frequency='5m', 
#              fields=['open','close','high','low','volume','money'], 
#              skip_paused=False, 
#              fq=None, 
#              count=None)

#A=ts.pro_bar(
#            ts_code=_QA_code_toTushare('M1909','FT'),
#            asset='FT',
#            adj='bfq',
#            start_date='20190101',
#            end_date='20190702',
#            freq='30min'
#            )
#ts.set_token(DEFAULT_TUSHARE_TOKEN)
#ts.get_tick_data(_QA_code_toTushare('000001','E'), '20190701')
# =============================================================================

#############################################################################
#############################################################################
#############################################################################
def QA_fetch_get_stock_adj(code, end='',token = None):
    """获取股票的复权因子
    
    Arguments:
        code {[type]} -- [description]
    
    Keyword Arguments:
        end {str} -- [description] (default: {''})
    
    Returns:
        [type] -- [description]
    """

    pro = get_pro(token)
    adj = pro.adj_factor(ts_code=code, trade_date=end)
    return adj

QA_fetch_get_stock_adj('000001.SZ','')

def QA_fetch_stock_basic(token = None):

    def fetch_stock_basic():
        stock_basic = None
        try:
            pro = get_pro(token)
            stock_basic = pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,'
                'symbol,'
                'name,'
                'area,industry,list_date'
            )
        except Exception as e:
            print(e)
            print('except when fetch stock basic')
            time.sleep(1)
            stock_basic = fetch_stock_basic()
        return stock_basic

    return fetch_stock_basic()

#QA_fetch_stock_basic()

def cover_time(date):
    """
    字符串 '20180101'  转变成 float 类型时间 类似 time.time() 返回的类型
    :param date: 字符串str -- 格式必须是 20180101 ，长度8
    :return: 类型float
    """
    datestr = str(date)[0:8]
    date = time.mktime(time.strptime(datestr, '%Y%m%d'))
    return date


def _get_subscription_type(if_fq):
    if str(if_fq) in ['qfq', '01']:
        if_fq = 'qfq'
    elif str(if_fq) in ['hfq', '02']:
        if_fq = 'hfq'
    elif str(if_fq) in ['bfq', '00']:
        if_fq = 'bfq'
    else:
        QA_util_log_info('wrong with fq_factor! using qfq')
        if_fq = 'qfq'
    return if_fq


def QA_fetch_get_stock_day(name, start='', end='', if_fq='qfq', type_='pd'):
    if_fq = _get_subscription_type(if_fq)

    def fetch_data():
        data = None
        try:
            time.sleep(0.002)
            pro = get_pro()
            data = ts.pro_bar(
                pro_api=pro,
                ts_code=str(name),
                asset='E',
                adj=if_fq,
                start_date=start,
                end_date=end,
                freq='D',
                factors=['tor',
                         'vr']
            ).sort_index()
            print('fetch done: ' + str(name))
        except Exception as e:
            print(e)
            print('except when fetch data of ' + str(name))
            time.sleep(1)
            data = fetch_data()
        return data

    data = fetch_data()

    data['date_stamp'] = data['trade_date'].apply(lambda x: cover_time(x))
    data['code'] = data['ts_code'].apply(lambda x: str(x)[0:6])
    data['fqtype'] = if_fq
    if type_ in ['json']:
        data_json = QA_util_to_json_from_pandas(data)
        return data_json
    elif type_ in ['pd', 'pandas', 'p']:
        data['date'] = pd.to_datetime(data['trade_date'], format='%Y%m%d')
        data = data.set_index('date', drop=False)
        data['date'] = data['date'].apply(lambda x: str(x)[0:10])
        return data


def QA_fetch_get_stock_realtime():
    data = ts.get_today_all()
    data_json = QA_util_to_json_from_pandas(data)
    return data_json




def _QA_fetch_get_stock_tick(name, date):
    if (len(name) != 6):
        name = str(name)[0:6]
    return ts.get_tick_data(name, date)

def QA_fetch_get_stock_list():
    df = QA_fetch_stock_basic()
    return list(df.ts_code)


def QA_fetch_get_stock_time_to_market():
    data = ts.get_stock_basics()
    return data[data['timeToMarket'] != 0]['timeToMarket']\
        .apply(lambda x: QA_util_date_int2str(x))


def QA_fetch_get_trade_date(end, exchange):
    data = ts.trade_cal()
    da = data[data.isOpen > 0]
    data_json = QA_util_to_json_from_pandas(data)
    message = []
    for i in range(0, len(data_json) - 1, 1):
        date = data_json[i]['calendarDate']
        num = i + 1
        exchangeName = 'SSE'
        data_stamp = QA_util_date_stamp(date)
        mes = {
            'date': date,
            'num': num,
            'exchangeName': exchangeName,
            'date_stamp': data_stamp
        }
        message.append(mes)
    return message


def QA_fetch_get_lhb(date):
    return ts.top_list(date)


def QA_fetch_get_stock_money():
    pass


# print(get_stock_day("000001",'2001-01-01','2010-01-01'))
# print(get_stock_tick("000001.SZ","2017-02-21"))
if __name__ == '__main__':
    df = QA_fetch_get_stock_list()
    print(df)
