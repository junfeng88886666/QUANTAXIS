# coding:utf-8

import datetime

import numpy as np
import pandas as pd
from retrying import retry
import os
import scipy.io as sio 

from QUANTAXIS.QAFetch.base import _select_market_code, _select_type
from QUANTAXIS.QAUtil import (QA_Setting, QA_util_date_stamp,
                              QA_util_date_str2int, QA_util_date_valid,
                              QA_util_get_real_date, QA_util_get_real_datelist,
                              QA_util_future_to_realdatetime, QA_util_future_to_tradedatetime,
                              QA_util_get_trade_gap, QA_util_log_info,QA_util_date_int2str,
                              QA_util_time_stamp, QA_util_web_ping,
                              QA_util_get_trade_range,QA_util_listdir,QA_util_listfile,QA_util_datetime_fixstr1)
from QUANTAXIS.QASetting.QALocalize import log_path
from QUANTAXIS.QAUtil.QAParameter import FREQUENCE,MARKET_TYPE
from QUANTAXIS.QAData.QADataAggrement import (QA_DataAggrement_Future_day,
                                              QA_DataAggrement_Future_min,
                                              QA_DataAggrement_Future_list
                                                )
## TODO 当前只有期货日线和分钟数据的获取，没有其他的，待补充
#%%
cofund_data_path = {
(MARKET_TYPE.FUTURE_CN, FREQUENCE.DAY,'商品期货','主连'):'Z:/Interns/hengpan/FutureMainDayData/Mat/',
(MARKET_TYPE.FUTURE_CN, FREQUENCE.ONE_MIN,'商品期货','主连'):'Z:/Interns/hengpan/FutureMainMinData/商品期货分钟数据/Data/InitialMinuteDataBase/1min/',
(MARKET_TYPE.FUTURE_CN, FREQUENCE.DAY,'商品期货','指数'):'Z:/Interns/hengpan/FutureIndexDayData/Mat/',
(MARKET_TYPE.FUTURE_CN, FREQUENCE.ONE_MIN,'商品期货','指数'):'Z:/Interns/hengpan/FutureMainMinData/商品期货分钟数据/Data/ContinuousMinuteDataBase/1min/',
(MARKET_TYPE.FUTURE_CN, FREQUENCE.ONE_MIN,'股指期货','主连'):'Z:/Interns/hengpan/IndexMinData/股指期货分钟数据/Data/InitialMinuteDataBase/1min/',
(MARKET_TYPE.FUTURE_CN, FREQUENCE.ONE_MIN,'股指期货','指数'):'Z:/Interns/hengpan/IndexMinData/股指期货分钟数据/Data/ContinuousMinuteDataBase/1min/'
}

############################################################################################## list
def QA_fetch_get_stock_list(type_='stock', ip=None, port=None):
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_stock_list as func
    return func()

def QA_fetch_get_index_list(ip=None, port=None):
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_index_list as func
    return func()

def QA_fetch_get_bond_list(ip=None, port=None):
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_bond_list as func
    return func()

def QA_fetch_get_future_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_future_list as func
    return func()

def QA_fetch_get_globalindex_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_globalindex_list as func
    return func()
def QA_fetch_get_goods_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_goods_list as func
    return func()

def QA_fetch_get_globalfuture_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_globalfuture_list as func
    return func()

def QA_fetch_get_hkstock_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_hkstock_list as func
    return func()

def QA_fetch_get_hkindex_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_hkindex_list as func
    return func()

def QA_fetch_get_hkfund_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_hkfund_list as func
    return func()

def QA_fetch_get_usstock_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_usstock_list as func
    return func()

def QA_fetch_get_macroindex_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_macroindex_list as func
    return func()

def QA_fetch_get_option_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_option_list as func
    return func()

def QA_fetch_get_exchangerate_list():
    '''
    TODO 暂时使用TDX的list
    '''
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_exchangerate_list as func
    return func()
def QA_fetch_get_wholemarket_list():
    from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_wholemarket_list as func
    return func()
############################################################################################################ Data

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_day(code, start_date, end_date, if_fq='00', frequence='day'):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_min(code, start, end, frequence='1min', ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_latest(code, frequence='day', ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_realtime(code=['000001', '000002'], ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_depth_market_data(code=['000001', '000002'], ip=None, port=None):
    raise NotImplementedError


'''
沪市
010xxx 国债
001×××国债现货；
110×××120×××企业债券；
129×××100×××可转换债券；
201×××国债回购；
310×××国债期货；
500×××550×××基金；
600×××A股；
700×××配股；
710×××转配股；
701×××转配股再配股；
711×××转配股再转配股；
720×××红利；
730×××新股申购；
735×××新基金申购；
737×××新股配售；
900×××B股。
深市
第1位	第二位	第3-6位	含义
0	0	XXXX	A股证券
0	3	XXXX	A股A2权证
0	7	XXXX	A股增发
0	8	XXXX	A股A1权证
0	9	XXXX	A股转配
1	0	XXXX	国债现货
1	1	XXXX	债券
1	2	XXXX	可转换债券
1	3	XXXX	国债回购
1	7	XXXX	原有投资基金
1	8	XXXX	证券投资基金
2	0	XXXX	B股证券
2	7	XXXX	B股增发
2	8	XXXX	B股权证
3	0	XXXX	创业板证券
3	7	XXXX	创业板增发
3	8	XXXX	创业板权证
3	9	XXXX	综合指数/成份指数
深市A股票买卖的代码是以000打头，如：顺鑫农业：股票代码是000860。
B股买卖的代码是以200打头，如：深中冠B股，代码是200018。
中小板股票代码以002打头，如：东华合创股票代码是002065。
创业板股票代码以300打头，如：探路者股票代码是：300005
更多参见 issue https://github.com/QUANTAXIS/QUANTAXIS/issues/158
@yutiansut
'''

def for_sz(code):
    """深市代码分类
    Arguments:
        code {[type]} -- [description]
    Returns:
        [type] -- [description]
    """

    if str(code)[0:2] in ['00', '30', '02']:
        return 'stock_cn'
    elif str(code)[0:2] in ['39']:
        return 'index_cn'
    elif str(code)[0:2] in ['15']:
        return 'etf_cn'
    elif str(code)[0:2] in ['10', '11', '12', '13']:
        # 10xxxx 国债现货
        # 11xxxx 债券
        # 12xxxx 可转换债券
        # 12xxxx 国债回购
        return 'bond_cn'

    elif str(code)[0:2] in ['20']:
        return 'stockB_cn'
    else:
        return 'undefined'


def for_sh(code):
    if str(code)[0] == '6':
        return 'stock_cn'
    elif str(code)[0:3] in ['000', '880']:
        return 'index_cn'
    elif str(code)[0:2] == '51':
        return 'etf_cn'
    # 110×××120×××企业债券；
    # 129×××100×××可转换债券；
    elif str(code)[0:3] in ['129', '100', '110', '120']:
        return 'bond_cn'
    else:
        return 'undefined'



@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_bond_day(code, start_date, end_date, frequence='day', ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_index_day(code, start_date, end_date, frequence='day', ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_index_min(code, start, end, frequence='1min', ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_index_latest(code, frequence='day', ip=None, port=None):
    raise NotImplementedError

def __QA_fetch_get_stock_transaction(code, day, retry, api):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_transaction(code, start, end, retry=2, ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_transaction_realtime(code, ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_xdxr(code, ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_info(code, ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_stock_block(ip=None, port=None):
    raise NotImplementedError




#def transform_mat_csv(file_path):
#    file_path = os.path.abspath(file_path)
#    try:
#        try:
#            file = pd.DataFrame(sio.loadmat(file_path)[file_path.split('\\')[-1].lower()])
#        except:
#            file = pd.DataFrame(sio.loadmat(file_path)[file_path.split('\\')[-1].upper()])
#            
#        if len(file.columns.tolist())==8:
#            file.columns = ['date','minute','open','high','low','close','volume','amount']
#        elif len(file.columns.tolist())==6:
#            file.columns = ['date','minute','open','high','low','close']
#            
#        file['minute'] = list(map(lambda x:(str(int(x))).zfill(4),file['minute']))
#        file['date'] = list(map(lambda x:(str(int(x))),file['date']))
#        file['datetime'] = pd.to_datetime(file['date'] + ' ' +file['minute'])
#        del file['minute']
#        del file['date']
#        file = file.set_index('datetime')
#        
#    except: 
#        raise NotImplementedError

def QA_fetch_get_future_min(code, start, end, frequence=FREQUENCE.ONE_MIN):
    '''
    TODO 当前仅支持L8和L9
    期货数据 分钟线
    code = 'IL8'
    start = '2010-01-10'
    end = '2018-05-10'
    '''    
    market_type = MARKET_TYPE.FUTURE_CN
    try:
        if code[-2:] in ['L8']:
            if frequence==FREQUENCE.ONE_MIN:
                '''找到总路径'''
                product_type = '股指期货' if code[:-2] in ['IF','IC','IH'] else '商品期货'
                data_type = '主连' if code[-2:] == 'L8' else '指数'
                path = cofund_data_path[(market_type,frequence,product_type,data_type)]
                
                main_code = code[:-2]
                '''确定品种路径'''
                if main_code.isupper(): file_code = main_code if main_code in QA_util_listdir(path) else main_code.lower()
                elif main_code.islower(): file_code = main_code if main_code in QA_util_listdir(path) else main_code.upper()
                else: raise NotImplementedError
            
                path = os.path.abspath(path+file_code)
                
                date_calendar = QA_util_get_trade_range(max(start[:10],QA_util_date_int2str(min(QA_util_listfile(path,'csv')))),min((end[:10]),QA_util_date_int2str(max(QA_util_listfile(path,'csv')))))
#                date_calendar = QA_util_get_trade_range(start[:10],end[:10])
                data = pd.DataFrame()
                for date in date_calendar:
                    try:
                        intdate = QA_util_date_str2int(date)
                        file_temp = pd.read_csv(os.path.join(path,str(intdate)+'.csv'))
                        data = data.append(file_temp)
                    except: pass
                data = data[data['Time']>=85900]
                data['type'] = frequence
                data['code'] = code
                
                data = QA_DataAggrement_Future_min('CoFund',data)
                
                return data[start:end]
            else:
                print('当前仅支持1分钟的数据调用')
        else:
            print('当前仅支持L8的数据调用')
            raise NotImplementedError
    except Exception as exp:
        print("code is ", code)
        print(exp.__str__)
        return None       

def QA_fetch_get_future_day(code, start_date, end_date, frequence='1min', ip=None, port=None):
    raise NotImplementedError

def __QA_fetch_get_future_transaction(code, day, retry, code_market, apix):
    raise NotImplementedError

def QA_fetch_get_future_transaction(code, start, end, retry=4, ip=None, port=None):
    raise NotImplementedError

def QA_fetch_get_future_transaction_realtime(code, ip=None, port=None):
    raise NotImplementedError

def QA_fetch_get_future_realtime(code, ip=None, port=None):
    raise NotImplementedError

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

QA_fetch_get_globalfuture_day = QA_fetch_get_future_day
QA_fetch_get_globalfuture_min = QA_fetch_get_future_min

QA_fetch_get_exchangerate_day = QA_fetch_get_future_day
QA_fetch_get_exchangerate_min = QA_fetch_get_future_min

QA_fetch_get_macroindex_day = QA_fetch_get_future_day
QA_fetch_get_macroindex_min = QA_fetch_get_future_min

QA_fetch_get_globalindex_day = QA_fetch_get_future_day
QA_fetch_get_globalindex_min = QA_fetch_get_future_min






if __name__ == '__main__':
    pass


