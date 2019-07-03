# # coding:utf-8
#
# '''
# 用来校验新数据源和当前数据库数据源的一致性
# '''
# from QUANTAXIS.QAData import (QA_DataStruct_Index_day, QA_DataStruct_Index_min,
#                               QA_DataStruct_Future_day, QA_DataStruct_Future_min,
#                               QA_DataStruct_Stock_block, QA_DataStruct_Financial,
#                               QA_DataStruct_Stock_day, QA_DataStruct_Stock_min,
#                               QA_DataStruct_Stock_transaction)
# from QUANTAXIS.QAUtil.QAParameter import DATABASE_TABLE,MARKET_TYPE,FREQUENCE,DATABASE_NAME
# from QUANTAXIS.QAUtil.QADate import QA_util_date_int2str
# from
# import pandas as pd
# import copy
#
#
# def select_DataStruct(database_name):
#     '''
#     market_type = MARKET_TYPE.FUTURE_CN
#     freq = FREQUENCE.ONE_MIN
#     '''
#     if database_name == DATABASE_NAME.STOCK_DAY: return QA_DataStruct_Stock_day
#     elif database_name == DATABASE_NAME.STOCK_MIN: return QA_DataStruct_Stock_min
#     elif database_name == DATABASE_NAME.STOCK_TRANSACTION: return QA_DataStruct_Stock_transaction
#     elif database_name == DATABASE_NAME.STOCK_BLOCK: return QA_DataStruct_Stock_block
#
#     elif database_name == DATABASE_NAME.FUTURE_DAY: return QA_DataStruct_Future_day
#     elif database_name == DATABASE_NAME.FUTURE_MIN: return QA_DataStruct_Future_min
#     elif database_name == DATABASE_NAME.INDEX_DAY: return QA_DataStruct_Index_day
#     elif database_name == DATABASE_NAME.INDEX_MIN: return QA_DataStruct_Index_min
#
#     elif database_name == DATABASE_NAME.FINANCIAL: return QA_DataStruct_Financial
#
#
# def load_csv_cofund_future(file_path = None,database_name = None,code = None):
#     '''
#     file_path = 'Z:\Interns\hengpan\FutureMainDayData\Csv\A.csv'
#     database_name = DATABASE_NAME.FUTURE_DAY
#     '''
#     DataStruct = select_DataStruct(database_name)
#     data = pd.read_csv(file_path,index_col = False).set_index('Date').rename(columns = {'Open':'open',
#                                                                                          'High':'high',
#                                                                                          'Low':'low',
#                                                                                          'Price':'close',
#                                                                                          'Oi':'position',
#                                                                                          'Amount':'amount',
#                                                                                          'Vol':'trade'})[['open','high','low','close','position','trade','amount']]
#     data.index = pd.Series(data.index).apply(QA_util_date_int2str)
#     data.index = pd.to_datetime(data.index)
#     data['date'] = data.index
#     data['code'] = code
#     data['trade'] = (data['trade']/100).astype(int)
#     return DataStruct(data.set_index(['date','code'])).data.fillna(0)
#
# def load_csv_data(file_source = 'cofund',file_path = None,database_name = None,code = None):
#     if file_source == 'cofund': return load_csv_cofund_future(file_path = file_path,database_name = database_name,code = code)
#
#
# def QA_CheckDB(new_DATASOURCE = None,
#                database_name = None):
#     '''
#     TODO: 当前只支持价量数据：日线，分钟线,transaction的合并，不涉及其他的合并，未来可以兼容更多
#     '''
#     database_name = copy.deepcopy(check_table_name)
#     dataadv = QA.QA_fetch_future_day_adv('AL8','2013-01-10','2019-05-20').data
#
#
#
# ## =============================================================================
# ## DEBUG
# #data = load_csv_data('cofund','Z:\Interns\hengpan\FutureMainDayData\Csv\A.csv',DATABASE_NAME.FUTURE_DAY,'AL9')
# #data
# #
# #
# #
# #
# #import QUANTAXIS as QA
# #
# #dataadv = QA.QA_fetch_future_day_adv('AL8','2013-01-10','2019-05-20')
# #dataadv.to_qfq()
# #dataadv = QA.QA_fetch_stock_day_adv('000001','2013-01-10','2019-05-20')
# #dataadv.plot('AL8')
# #dataadv.to_qfq().plot('AL8')
# #
# #
# #.data
# #dataadv = QA.QA_fetch_get_future_day('tdx','AL8','2013-01-10','2019-05-20')
# #QA.QA_fetch_future_list_adv()
# ## =============================================================================
# #aa = QA.QA_fetch_get_future_min('tdx','AL8','2019-01-01','2019-05-23')
# ## =============================================================================
# #aa = QA.QA_fetch_get_future_day('tdx','AL8','2013-01-10','2019-05-20')
# #aa
# #QA.QA_fetch_get_stock_latest('tdx',['000001','000002'])
# #
# #QA.QAFetch.QATdx.QA_fetch_get_stock_latest('000001')
# #
# #QA.QA_fetch_get_future_list('tdx')
# #aa
# #
# #import SuperQuant as SQ
# #import os
# #
# #def listdir(path,types = 'mat',if_ends = False):
# #    dirlist = os.listdir(path)
# #    if if_ends: return [i for i in dirlist if (i.split('.')[-1]==types)&(len(i.split('.'))>1)]
# #    return [i.split('.')[0] for i in dirlist if (i.split('.')[-1]==types)&(len(i.split('.'))>1)]
# #
# #a = 'a.mat'
# #a.split('.')[-1]
# #import datetime
# #'21:50:00'<datetime.time(21,0)
# #listdir(path)
# #
# #
# #    for file in os.listdir(path):
# #        file_path = os.path.join(path, file)
# #        if os.path.isdir(file_path):
# #            listdir(file_path, list_name)
# #        elif os.path.splitext(file_path)[1]==types:
# #            list_name.append(file_path)
# #    return list_name
# #
# #a = os.walk('Z:\Interns\hengpan\FutureMainMinData\商品期货分钟数据\Data\InitialMinuteDataBase\1min', topdown= True, onerror=None, followlinks=False)
# #a[0]
# #file_name(os.path.abspath('Z:\Interns\hengpan\FutureMainMinData\商品期货分钟数据\Data\InitialMinuteDataBase\1min'))
# #os.walk(os.path.abspath('Z:\Interns\hengpan\FutureMainMinData\商品期货分钟数据\Data\InitialMinuteDataBase\1min'))
# #path =os.path.abspath('Z:/Interns/hengpan/FutureMainMinData/商品期货分钟数据/Data/InitialMinuteDataBase/1min')
# #list_name = listdir(path)
# #os.listdir(path)
# #QA.QA_fetch_get_stock_day('ts','000001','2010-01-01','2019-05-01')
# #QA.QA_fetch_get_future_list('cof')
# #QA.QA_fetch_get_stock_list('cof')
# #
# #
# #aa = QA.QA_fetch_get_stock_day('tdx','000001','2019-05-29','2019-05-30')
# #bb = QA.QA_fetch_get_stock_latest('tdx','000001')
# #
# #ti = '2019-01-10 21:58:00'
# #len(ti)
# #QA.QAUtil.QADate_trade.QA_util_future_to_realdatetime(ti)
# #270*152034
# #ti.apply(QA.QAUtil.QADate_trade.QA_util_future_to_realdatetime(),1)
# #
# #aaa = QA.QA_fetch_get_future_min('cof','AL8','2018-01-01','2018-05-20')
# #aaa.datetime.min()
# #
# ##%%
# #import QUANTAXIS as QA
# #
# #from QUANTAXIS.QAUtil import (
# #    DATABASE,
# #    QA_util_get_next_day,
# #    QA_util_get_real_date,
# #    QA_util_log_info,
# #    QA_util_to_json_from_pandas,
# #    trade_date_sse
# #)
# #
# #QA_util_to_json_from_pandas(aaa)
# #aaa['2018-05-10 14:10:00':'2018-05-10 15:00:00']
# #
# #aaa = QA.QA_fetch_get_future_min('cof','IL8','2010-01-01','2018-05-20')
#
#
