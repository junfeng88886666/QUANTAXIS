# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 13:22:14 2019

@author: Administrator
"""
import pandas as pd
import numpy as np
from datetime import time
import QUANTAXIS as QA
#%%
data = QA.QAFetch.QATdx.QA_fetch_get_stock_transaction('000001','2019-02-01 10:30:00','2019-02-03')
data = QA.QAFetch.QATdx.QA_fetch_get_stock_min('000002','2018-06-26','2019-06-27','1min',True,True)
data = QA.QAFetch.QATdx.QA_fetch_get_stock_min('000002','2019-06-26','2019-06-27','30min')
data = QA.QAFetch.QATdx.QA_fetch_get_stock_latest('000002')
data = QA.QAFetch.QATdx.QA_fetch_get_stock_realtime('000002')
data = QA.QAFetch.QATdx.QA_fetch_get_depth_market_data('000002')
data = QA.QAFetch.QATdx.QA_fetch_get_stock_list()
data = QA.QAFetch.QATdx.QA_fetch_get_stock_xdxr('000002')
data = QA.QAFetch.QATdx.QA_fetch_get_stock_info('000002')
data = QA.QAFetch.QATdx.QA_fetch_get_stock_block()


future_list = QA.QAFetch.QATdx.QA_fetch_get_future_list()
data = QA.QA_fetch_get_future_day('tdx','ICL8','2011-06-26','2019-06-27')
data = QA.QAFetch.QATdx.QA_fetch_get_future_transaction('ICL8','2019-06-26','2019-06-27')
data = QA.QA_fetch_get_future_min('jq','ICL8','2015-06-26','2019-06-27')
60*24*250

data1 = QA.QA_fetch_get_future_min('jqdata','AGL8','2010-06-26','2019-06-27')
#%%
data = QA.QA_fetch_stock_day('000001','2018-10-16','2018-10-19','pd')
data = QA.QAFetch.QAQuery.QA_fetch_stock_transaction('000001','2019-02-01 10:30:00','2019-02-03')
data = QA.QAFetch.QAQuery.QA_fetch_stock_min('000002','2018-06-26','2019-06-27','1min',True,True)
data = QA.QAFetch.QAQuery.QA_fetch_stock_min('000001','2018-10-16','2018-10-18','1min','pd')
data = QA.QAFetch.QAQuery.QA_fetch_stock_list()
data = QA.QAFetch.QAQuery.QA_fetch_stock_xdxr('000002')
data = QA.QAFetch.QAQuery.QA_fetch_stock_info('000002')
data = QA.QAFetch.QAQuery.QA_fetch_stock_info('000002')

data = QA.QAFetch.QAQuery.QA_fetch_future_list()
data = QA.QAFetch.QAQuery.QA_fetch_future_day('CJL9','2018-06-26','2019-06-27','pd')

#%%
CODE = 'I1909'
data1 = QA.QAFetch.QATdx.QA_fetch_get_future_min(CODE,'2019-06-26','2019-06-27')

[['open','high','low','close','trade','position']]

tick = QA.QAFetch.QATdx.QA_fetch_get_future_transaction(CODE,'2019-06-26','2019-06-27')
tick_min = QA.QAFetch.QATdx.QA_fetch_get_future_transaction(CODE,'2019-06-26','2019-06-27','1min')


i = data1[['close']]
i['b'] = tick_min[['close']]
i.columns = ['real_close','tick_resample_close']
i.plot()




tick['time'] = tick.datetime.apply(lambda x:str(x)[10:])
min(list(set(tick['time'])))
tick['2000-01-01':'2019-01-01']

type_ = '1min'

resx = pd.DataFrame()
__type_index = type(tick.index[0])
tick.index = pd.to_datetime(tick.index)
_temp = set(tick.index.date)

mindata = tick.resample(
                             type_,
                             closed='left',
                             base=30,
                             loffset=type_
                         ).apply(
                             {
                                 'price': 'ohlc',
                                 'volume': 'sum',
                                 'code': 'last'
                             }
                         )
mindata.columns = ['open','high','low','close','volume','code']
for i in ['open','high','low','close']: mindata[i]/=1000



#%%
from datetime import time
resample_edit_periods = {
    1:[[time(9,0),time(9,1)],time(9,1)]
}


resample_edit_periods[1]
#%%













 for item in _temp:
    _data = tick.loc[str(item)]
    _data1 = _data[time(9,
                        31):time(11,
                                 30)].resample(
                                     type_,
                                     closed='right',
                                     base=30,
                                     loffset=type_
                                 ).apply(
                                     {
                                         'price': 'ohlc',
                                         'volume': 'sum',
                                         'code': 'last',
                                         'amount': 'sum'
                                     }
                                 )














































#%%
data = QA.QAFetch.QATdx.QA_fetch_get_stock_min('000007','2019-06-01 10:30:00','2019-06-10 10:30:00','1min')
data = pd.read_csv('D:\\Quant\\programe\\strategy_pool_adv\\strategy07\\backtest\\backtest03\\check_result\\min_data.csv')
#%%
QA.QA_fetch_stock_day('000001','2018-01-01','2019-01-01')
data  =QA.QA_fetch_stock_transaction('000048','2019-01-02 09:40:00','2019-05-15 09:55:00','pd')
data = QA.QA_fetch_stock_min('000001','2018-01-01','2019-01-01','pd')

data['2019-06-26 09:25:00':'2019-06-26 09:30:00']['volume'].sum()
#%%
start = '2017-06-15'
end = '2017-06-27'
freq = '30min'
from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_stock_min,QA_fetch_get_stock_transaction
data =  QA_fetch_get_stock_min('000002',start,end,freq,True,True)
data1 =  QA_fetch_get_stock_min('000002',start,end,freq)

#data2 = QA_fetch_get_stock_transaction('000002','2018-12-26','2019-02-15 13:02:00','1min')
from QUANTAXIS.QAData.data_resample import QA_data_min_resample_stock,QA_data_stocktick_resample_1min,QA_data_min_resample


data1 = QA_data_stocktick_resample_1min(QA_fetch_get_stock_transaction('000002',start,end))
data3 = QA_data_min_resample_stock(data1,freq)
data3 = QA_data_min_resample(data1,freq)


data1[['datetime','code']]
#%%
data = QA.QAFetch.QATdx.QA_fetch_get_stock_transaction('000001','2015-06-05','2015-06-06')
data.index = pd.to_datetime(data.index)
type(data.index[0])('2019-01-01')

#%%
data = QA.QAFetch.QATdx.QA_fetch_get_future_list()
data['signal'] = data['code'].apply(lambda x:x[-2:])
data[data['signal'].isin(['L8','L9'])]
1300/8200
assert False
STOCK_DAY = (['code', 'open', 'high', 'low','close', 'volume', 'amount', 'date'],'date')

['code','open'] in


from QUANTAXIS.QAUtil import (DATABASE, QA_Setting, QA_util_date_stamp,
                              QA_util_date_valid, QA_util_dict_remove_key,
                              QA_util_log_info, QA_util_code_tostr, QA_util_date_str2int, QA_util_date_int2str,
                              QA_util_sql_mongo_sort_DESCENDING,
                              QA_util_time_stamp, QA_util_to_json_from_pandas,
                              trade_date_sse,QA_util_dateordatetime_valid,QA_util_to_anyformat_from_pandas,
                              QA_util_get_last_day,QA_util_get_next_day,QA_util_get_real_date)


QA_util_time_stamp('2016-01-01 09:00:00')
QA_util_time_stamp('2016-01-01')
QA_util_time_stamp('2016-01-01 00:00:00')
QA_util_get_real_date(QA_util_get_last_day('2019-07-01'), towards = -1)
QA_util_get_real_date(QA_util_get_next_day('2019-07-01'), towards = 1)
