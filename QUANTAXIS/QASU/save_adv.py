# coding:utf-8

import concurrent
import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import json
import pandas as pd
import pymongo
import copy
import numpy as np
from QUANTAXIS.QAFetch import (
                                QA_fetch_get_stock_day,
                                QA_fetch_get_stock_info,
                                QA_fetch_get_stock_list,
                                QA_fetch_get_stock_min,
                                QA_fetch_get_stock_transaction,
                                QA_fetch_get_stock_xdxr,
                                QA_fetch_get_stock_block,
                                QA_fetch_get_future_list,
                                QA_fetch_get_index_list,
                                QA_fetch_get_future_day,
                                QA_fetch_get_future_min,
                                QA_fetch_get_option_day,
                                QA_fetch_get_option_min,
                                QA_fetch_get_index_day,
                                QA_fetch_get_index_min
                            )

from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_get_next_day,
    QA_util_get_real_date,
    QA_util_log_info,
    QA_util_to_json_from_pandas,
    trade_date_sse
)
from QUANTAXIS.QAUtil import Parallelism
from QUANTAXIS.QAFetch.QATdx import ping, get_ip_list_by_multi_process_ping, stock_ip_list
from multiprocessing import cpu_count

default_max_workers = 100
# ip=select_best_ip()
err = []

def now_time():
    '''
    若当前时间小于15:00:00 则返回昨天的17:00:00 否则返回今天的 15:00:00
    '''
    return str(QA_util_get_real_date(str(datetime.date.today() - datetime.timedelta(days=1)), trade_date_sse, -1)) + \
           ' 17:00:00' if datetime.datetime.now().hour < 15 else str(QA_util_get_real_date(
        str(datetime.date.today()), trade_date_sse, -1)) + ' 15:00:00'

def _saving_work_For_SpecialCode(func = None,
                                 package = None,
                                 code = None,
                                 coll = None,
                                 ui_log = None):
    QA_util_log_info(
        '## Now Saving XDXR INFO ==== {}'.format(str(code)),
        ui_log=ui_log
    )
    try:
        coll.insert_many(
            QA_util_to_json_from_pandas(func(package, str(code))),
            ordered=False
        )

    except:
        err.append(str(code))

def _saving_work_ForDataWithTime_SpecialCode(func = None,
                                              package = None,
                                              code = None,
                                              initial_start = None,
                                              coll = None,
                                              time_type = None,
                                              data_type = None,
                                              message_type = None,
                                              ui_log = None):
    QA_util_log_info(
        '## Now Saving {} ==== {}'.format(str(message_type),str(code)),
        ui_log = ui_log
    )

    try:
        if data_type == None: ref_ = coll.find({'code': code})
        else: ref_ = coll.find({'code': code, 'type': data_type})

        if time_type == 'datetime': end_time = str(now_time())[0:19]
        elif time_type == 'date': end_time = str(now_time())[0:10]

        if ref_.count() > 0:
            if time_type == 'datetime':
                start_time = str(ref_[ref_.count() - 1]['datetime'])[0:19]
            elif time_type == 'date':
                start_time = str(ref_[ref_.count() - 1]['date'])[0:10]
            if any(name in message_type for name in ['TRANSACTION', 'transaction','Transaction']):
                '''若存储的是tick数据，需要同时对order进行增量，这里获取最后的一个order'''
                last_order = int(ref_[ref_.count() - 1]['order'])

            if data_type == None:
                QA_util_log_info(
                    '# Trying updating {} from {} to {}, package: {}'.format(
                                                                              (code),
                                                                              (start_time),
                                                                              (end_time),
                                                                              (package)
                                                                              ),
                    ui_log = ui_log
                )
            else:
                QA_util_log_info(
                    '# Trying updating {} from {} to {} =={}, package: {}'.format(
                                                                                    (code),
                                                                                    (start_time),
                                                                                    (end_time),
                                                                                    (data_type),
                                                                                    (package)
                                                                                    ),
                    ui_log=ui_log
                )
            if start_time != end_time:
                if data_type == None: predata = func(package,code,start_time,end_time)
                else: predata = func(package,code,start_time,end_time,data_type)

                update_start_time = copy.deepcopy(start_time)
                if time_type == 'datetime': data_getted_start_time = predata.datetime.min()
                elif time_type == 'date': data_getted_start_time = predata.date.min()

                if data_getted_start_time == update_start_time:
                    if time_type == 'datetime':
                        save_data = predata[predata['datetime'] > start_time]
                        if any(name in message_type for name in ['TRANSACTION', 'transaction','Transaction']):
                            save_data['order'] = np.arange(last_order+1,len(save_data)+last_order+1)
                        coll.insert_many(
                            QA_util_to_json_from_pandas(save_data)
                        )
                        QA_util_log_info(
                            '# Saving success: code: {}, start time: {},end time: {}'
                                .format(code, save_data.datetime.min(),save_data.datetime.max()),
                            ui_log
                        )
                    elif time_type == 'date':
                        save_data = predata[predata['date'] > start_time]
                        if any(name in message_type for name in ['TRANSACTION', 'transaction', 'Transaction']):
                            save_data['order'] = np.arange(last_order + 1, len(save_data) + last_order + 1)
                        coll.insert_many(
                            QA_util_to_json_from_pandas(save_data)
                        )
                        QA_util_log_info(
                            '# Saving success: code: {}, start time: {},end time: {}'
                                .format(code, save_data.date.min(),save_data.date.max()),
                            ui_log
                        )
                else:
                    QA_util_log_info(
                        '# Data Error: code: {}, reason: start time does not match, start time: {}, database calculated start time: {}'
                            .format(code,data_getted_start_time,update_start_time),
                        ui_log
                    )
                    err.append(code)
            else:
                QA_util_log_info(
                    '# No Update, reason: the update time = the historical end time',
                    ui_log
                )

        else:
            start_time = initial_start

            if time_type == 'datetime': start_time = str(initial_start)[0:19]
            elif time_type == 'date': start_time = str(initial_start)[0:10]

            if data_type == None:
                QA_util_log_info(
                    '# Trying updating {} from {} to {}, package: {}'.format(
                                                                          (code),
                                                                          (start_time),
                                                                          (end_time),
                                                                          (package)
                                                                          ),
                    ui_log = ui_log
                )
                save_data = func(package, code, start_time, end_time)
            else:
                QA_util_log_info(
                    '# Trying updating {} from {} to {} =={}, package: {}'.format(
                                                                                (code),
                                                                                (start_time),
                                                                                (end_time),
                                                                                (data_type),
                                                                                (package)
                                                                                ),
                    ui_log=ui_log
                )
                save_data = func(package, code, start_time, end_time, data_type)

            if type(save_data) != type(None):
                coll.insert_many(
                    QA_util_to_json_from_pandas(save_data)
                )
                QA_util_log_info(
                    '# Saving success: code: {}, start time: {},end time: {}'
                        .format(code, start_time, end_time),
                    ui_log
                )
            else:
                QA_util_log_info(
                    '# Data Error: code: {}, start time: {},end time: {}, reason: No Data'.format(code, start_time, end_time),
                    ui_log
                )
                err.append(code)

    except Exception as e:
        QA_util_log_info(e, ui_log=ui_log)
        err.append(code)
        QA_util_log_info(err, ui_log=ui_log)

def _saving_work_For_SpecialCode_ThreadPool(func = None,
                                            package = None,
                                            code_list = None,
                                            coll=None,
                                            message_type=None,
                                            ui_log=None,
                                            ui_progress=None,
                                            num_threads = default_max_workers):
    executor = ThreadPoolExecutor(max_workers=num_threads)
    res = {
        executor.submit(_saving_work_For_SpecialCode,
                        func,
                        package,
                        code_list[i_],
                        coll,
                        ui_log)
        for i_ in range(len(code_list))
    }
    count = 0
    for i_ in concurrent.futures.as_completed(res):
        QA_util_log_info(
            'The {} of Total {}'.format(count,
                                        len(code_list)),
            ui_log=ui_log
        )

        strProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(count / len(code_list) * 100))[0:4] + '%'
        )
        intProgress = int(count / len(code_list) * 10000.0)
        QA_util_log_info(
            strProgress,
            ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgress
        )
        count = count + 1

    if len(err) < 1:
        QA_util_log_info('SUCCESS save {} ^_^'.format(message_type), ui_log)
    else:
        QA_util_log_info('ERROR CODE \n ', ui_log)
        QA_util_log_info(err, ui_log)


def _saving_work_ForDataWithTime_SpecialCode_ThreadPool(func = None,
                                                          package = None,
                                                          code_list = None,
                                                          initial_start = None,
                                                          coll = None,
                                                          time_type = None,
                                                          data_type = None,
                                                          message_type = None,
                                                          ui_log = None,
                                                          ui_progress = None,
                                                          num_threads = default_max_workers):
    executor = ThreadPoolExecutor(max_workers=num_threads)
    res = {
        executor.submit(_saving_work_ForDataWithTime_SpecialCode,
                        func,
                        package,
                        code_list[i_],
                        initial_start,
                        coll,
                        time_type,
                        data_type,
                        message_type,
                        ui_log)
        for i_ in range(len(code_list))
    }
    count = 0
    for i_ in concurrent.futures.as_completed(res):
        QA_util_log_info(
            'The {} of Total {}'.format(count,
                                        len(code_list)),
            ui_log=ui_log
        )

        strProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(count / len(code_list) * 100))[0:4] + '%'
        )
        intProgress = int(count / len(code_list) * 10000.0)
        QA_util_log_info(
            strProgress,
            ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=intProgress
        )
        count = count + 1

    if len(err) < 1:
        QA_util_log_info('SUCCESS save {} ^_^'.format(message_type), ui_log)
    else:
        QA_util_log_info('ERROR CODE \n ', ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_list(package = None,client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_list

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    client.drop_collection('stock_list')
    coll = client.stock_list
    coll.create_index('code')

    try:
        QA_util_log_info(
            '## Now Saving STOCK_LIST ====, package: {}'.format(package),
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=5000
        )
        stock_list_from_package = QA_fetch_get_stock_list(package = package)
        pandas_data = QA_util_to_json_from_pandas(stock_list_from_package)
        coll.insert_many(pandas_data)
        QA_util_log_info(
            "完成股票列表获取",
            ui_log=ui_log,
            ui_progress=ui_progress,
            ui_progress_int_value=10000
        )
    except Exception as e:
        QA_util_log_info(e, ui_log=ui_log)
        print(" Error save_tdx.QA_SU_save_stock_list exception!")
        pass

def QA_SU_save_stock_day(package = None,initial_start = '1990-01-01',client=DATABASE, ui_log=None, ui_progress=None):
    '''
     save stock_day
    多进程保存日线数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''
    global err
    err = []

    stock_list = QA_fetch_get_stock_list(package = None).code.unique().tolist()
    coll = client.stock_day
    coll.create_index(
        [("code",pymongo.ASCENDING),
         ("date_stamp",pymongo.ASCENDING)],
          unique = True
    )

    _saving_work_ForDataWithTime_SpecialCode_ThreadPool(func = QA_fetch_get_stock_day,
                                                      package = package,
                                                      code_list = stock_list,
                                                      initial_start = initial_start,
                                                      coll = coll,
                                                      time_type = 'date',
                                                      data_type = None,
                                                      message_type = 'STOCK_DAY',
                                                      ui_log = ui_log,
                                                      ui_progress = ui_progress)

def QA_SU_save_stock_transaction(package = None,initial_start = '2019-01-01', client = DATABASE, ui_log = None, ui_progress = None):
    """save stock_transaction
    注：1, transaction数据库索引均不唯一，因为其对应的时间戳有相同时间，入库的时候还需要对order进行增量。
       2, transaction数据库使用日级别增量更新
    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    global err
    err = []

    stock_list = QA_fetch_get_stock_list(package = package).code.unique().tolist()
    coll = client.stock_transaction
    coll.create_index(
        [("code",pymongo.ASCENDING),
         ("time_stamp",pymongo.ASCENDING),
         ("order",pymongo.ASCENDING)],
        unique=True
    )

    _saving_work_ForDataWithTime_SpecialCode_ThreadPool(func = QA_fetch_get_stock_transaction,
                                                      package = package,
                                                      code_list = stock_list,
                                                      initial_start = initial_start,
                                                      coll = coll,
                                                      time_type = 'date',
                                                      data_type = None,
                                                      message_type = 'STOCK_TRANSACTION',
                                                      ui_log = ui_log,
                                                      ui_progress = ui_progress)

def QA_SU_save_stock_min(package = None,data_type = None,num_threads = default_max_workers,initial_start = '2014-01-01', client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    global err
    err = []
    if data_type == None: data_type = ['1min']
    stock_list = QA_fetch_get_stock_list(package = None).code.unique().tolist()
    coll = client.stock_min
    coll.create_index(
        [
            ('code',
             pymongo.ASCENDING),
            ('time_stamp',
             pymongo.ASCENDING),
            ('date_stamp',
             pymongo.ASCENDING)
        ],
        unique = True
    )
    for item in data_type:
        _saving_work_ForDataWithTime_SpecialCode_ThreadPool(func=QA_fetch_get_stock_min,
                                                            package=package,
                                                            code_list=stock_list,
                                                            initial_start=initial_start,
                                                            coll=coll,
                                                            time_type='datetime',
                                                            data_type=item,
                                                            message_type='STOCK_MIN',
                                                            ui_log=ui_log,
                                                            ui_progress=ui_progress,
                                                            num_threads = num_threads)


def QA_SU_save_stock_xdxr(package = None, client=DATABASE, ui_log=None, ui_progress=None):
    """[summary]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    global err
    err = []
    stock_list = QA_fetch_get_stock_list(package = None).code.unique().tolist()
    try:
        coll = client.stock_xdxr
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )
    except:
        client.drop_collection('stock_xdxr')
        coll = client.stock_xdxr
        coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date',
              pymongo.ASCENDING)],
            unique=True
        )

    _saving_work_For_SpecialCode_ThreadPool(func=QA_fetch_get_stock_xdxr,
                                            package=package,
                                            code_list=stock_list,
                                            coll=coll,
                                            message_type='STOCK_XDXR',
                                            ui_log=ui_log,
                                            ui_progress=ui_progress)


def QA_SU_save_stock_info(package = None,client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_info

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    global err
    err = []

    client.drop_collection('stock_info')
    stock_list = QA_fetch_get_stock_list(package = None).code.unique().tolist()
    coll = client.stock_info
    coll.create_index('code')

    _saving_work_For_SpecialCode_ThreadPool(func=QA_fetch_get_stock_info,
                                            package=package,
                                            code_list=stock_list,
                                            coll=coll,
                                            message_type='STOCK_INFO',
                                            ui_log=ui_log,
                                            ui_progress=ui_progress)

def QA_SU_save_stock_block(package = None,client=DATABASE, ui_log=None, ui_progress=None):
    """save stock_info

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    try:
        client.drop_collection('stock_block')
        stock_list = QA_fetch_get_stock_list(package = None).code.unique().tolist()
        coll = client.stock_block
        coll.create_index('code')

        coll.insert_many(
            QA_util_to_json_from_pandas(QA_fetch_get_stock_block(package)),
            ordered=False)

        QA_util_log_info('SUCCESS save {} ^_^'.format('STOCK_BLOCK'), ui_log)
    except:
        QA_util_log_info('ERROR: SAVE {}'.format('STOCK_BLOCK'), ui_log)

#%% FUTURE_CN_PART
def QA_SU_save_future_list(package = None, client=DATABASE, ui_log=None, ui_progress=None):
    client.drop_collection('future_list')
    future_list = QA_fetch_get_future_list(package = None)
    coll_future_list = client.future_list
    coll_future_list.create_index("code", unique=True)

    try:
        coll_future_list.insert_many(
            QA_util_to_json_from_pandas(future_list),
            ordered=False)

        QA_util_log_info('SUCCESS save {} ^_^'.format('FUTURE_LIST'), ui_log)
    except:
        QA_util_log_info('ERROR: SAVE {}'.format('FUTURE_LIST'), ui_log)

def QA_SU_save_future_day(package = None,client=DATABASE, initial_start = '1990-01-01', ui_log=None, ui_progress=None):
    '''
     save future_day
    保存日线数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    :return:
    '''
    global err
    err = []
    future_list = [
        item for item in QA_fetch_get_future_list(package = None).code.unique().tolist()
        if str(item)[-2:] in ['L8',
                              'L9']
    ]
    coll = client.future_day
    coll.create_index(
        [("code",
          pymongo.ASCENDING),
         ("date_stamp",
          pymongo.ASCENDING)]
    )

    _saving_work_ForDataWithTime_SpecialCode_ThreadPool(func = QA_fetch_get_future_day,
                                                          package = package,
                                                          code_list = future_list,
                                                          initial_start = initial_start,
                                                          coll = coll,
                                                          time_type = 'date',
                                                          data_type = None,
                                                          message_type = 'FUTURE_DAY',
                                                          ui_log = ui_log,
                                                          ui_progress = ui_progress)

def QA_SU_save_future_min(package = None, data_type = None,num_threads = default_max_workers, initial_start = '2010-01-01',client=DATABASE, ui_log=None, ui_progress=None):
    """save future_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """
    global err
    err = []

    if data_type == None: data_type = ['1min']
    future_list = [
        item for item in QA_fetch_get_future_list(package = None).code.unique().tolist()
        if str(item)[-2:] in ['L8',
                              'L9']
    ]
    coll = client.future_min
    coll.create_index(
        [
            ('code',
             pymongo.ASCENDING),
            ('time_stamp',
             pymongo.ASCENDING),
            ('date_stamp',
             pymongo.ASCENDING)
        ],
        unique = True
    )

    for item in data_type:
        _saving_work_ForDataWithTime_SpecialCode_ThreadPool(func=QA_fetch_get_future_min,
                                                            package=package,
                                                            code_list=future_list,
                                                            initial_start=initial_start,
                                                            coll=coll,
                                                            time_type='datetime',
                                                            data_type=item,
                                                            message_type='FUTURE_MIN',
                                                            ui_log=ui_log,
                                                            ui_progress=ui_progress,
                                                            num_threads = num_threads)



#
# def QA_SU_save_index_day(package = None,client=DATABASE, ui_log=None, ui_progress=None):
#     """save index_day
#
#     Keyword Arguments:
#         client {[type]} -- [description] (default: {DATABASE})
#     """
#
#     __index_list = QA_fetch_get_index_list(package = package)
#     coll = client.index_day
#     coll.create_index(
#         [('code',
#           pymongo.ASCENDING),
#          ('date_stamp',
#           pymongo.ASCENDING)],
#           unique = True
#     )
#     err = []
#
#     def __saving_work(code, coll):
#         QA_util_log_info(
#             '##JOB03 Now Saving INDEX_DAY ==== {}'.format(str(code)),
#             ui_log=ui_log
#         )
#         try:
#             ref = coll.find({'code': str(code)[0:6]})
#             end_date = str(now_time())[0:10]
#             if ref.count() > 0:
#                 # 接着上次获取的日期继续更新
#                 start_date = ref[ref.count() - 1]['date']
#
#                 QA_util_log_info(
#                     'UPDATE_INDEX_DAY \n Trying updating {} from {} to {}, package: {}'
#                     .format(code,
#                             start_date,
#                             end_date,
#                             package),
#                     ui_log
#                 )
#                 if start_date != end_date:
#                     update_start_date = QA_util_get_next_day(start_date)
#                     predata = QA_fetch_get_index_day(
#                                                     package,
#                                                     str(code),
#                                                     update_start_date,
#                                                     end_date,
#                                                     'day'
#                                                     )
#                     data_getted_start_date = predata.date.min()
#                     if data_getted_start_date == update_start_date:
#                         coll.insert_many(
#                             QA_util_to_json_from_pandas(predata)
#                         )
#                     else:
#                         QA_util_log_info(
#                             'Trying updating {} from {} to {}, package: {}, Data Error: reason: start date does not match, start date: {}, database calculated start date: {}'
#                             .format(code,
#                                     start_date,
#                                     end_date,
#                                     package,
#                                     data_getted_start_date,
#                                     update_start_date
#                                     ),
#                             ui_log
#                         )
#                         err.append(str(code))
#             # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
#             else:
#                 start_date = '1990-01-01'
#                 QA_util_log_info(
#                     'UPDATE_INDEX_DAY \n Trying updating {} from {} to {}, package: {}'
#                     .format(code,
#                             start_date,
#                             end_date,
#                             package),
#                     ui_log
#                 )
#                 predata = QA_fetch_get_index_day(
#                                                 package,
#                                                 str(code),
#                                                 update_start_date,
#                                                 end_date,
#                                                 'day'
#                                                 )
#
#                 coll.insert_many(
#                     QA_util_to_json_from_pandas(predata)
#                 )
#
#         except Exception as e:
#             QA_util_log_info(e, ui_log=ui_log)
#             err.append(str(code))
#             QA_util_log_info(err, ui_log=ui_log)
#
#     for i_ in range(len(__index_list)):
#         # __saving_work('000001')
#         QA_util_log_info(
#             'The {} of Total {}'.format(i_,
#                                         len(__index_list)),
#             ui_log=ui_log
#         )
#
#         strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
#             str(float(i_ / len(__index_list) * 100))[0:4] + '%'
#         )
#         intLogProgress = int(float(i_ / len(__index_list) * 10000.0))
#         QA_util_log_info(
#             strLogProgress,
#             ui_log=ui_log,
#             ui_progress=ui_progress,
#             ui_progress_int_value=intLogProgress
#         )
#         __saving_work(__index_list.index[i_][0], coll)
#     if len(err) < 1:
#         QA_util_log_info('SUCCESS', ui_log=ui_log)
#     else:
#         QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
#         QA_util_log_info(err, ui_log=ui_log)
#
#
# def QA_SU_save_index_min(package = None, client=DATABASE, ui_log=None, ui_progress=None):
#     """save index_min
#
#     Keyword Arguments:
#         client {[type]} -- [description] (default: {DATABASE})
#     """
#
#     __index_list = QA_fetch_get_index_list(package = package)
#     coll = client.index_min
#     coll.create_index(
#         [
#             ('code',
#              pymongo.ASCENDING),
#             ('time_stamp',
#              pymongo.ASCENDING),
#             ('date_stamp',
#              pymongo.ASCENDING)
#         ]
#     )
#     err = []
#
#     def __saving_work(code, coll):
#         QA_util_log_info(
#             '##JOB05 Now Saving Index_MIN ==== {}'.format(str(code)),
#             ui_log=ui_log
#         )
#         try:
#             for type in ['1min', '5min', '15min', '30min', '60min']:
#                 ref_ = coll.find({'code': str(code)[0:6], 'type': type})
#                 end_time = str(now_time())[0:19]
#                 if ref_.count() > 0:
#                     start_time = ref_[ref_.count() - 1]['datetime']
#
#                     QA_util_log_info(
#                         '##JOB03.{} Trying updating {} from {} to {} =={}, package: {}'.format(
#                             ['1min',
#                              '5min',
#                              '15min',
#                              '30min',
#                              '60min'].index(type),
#                             str(code),
#                             start_time,
#                             end_time,
#                             type,
#                             package
#                         ),
#                         ui_log=ui_log
#                     )
#                     if start_time != end_time:
#                         predata = QA_fetch_get_index_min(
#                                                         package,
#                                                         str(code),
#                                                         start_time,
#                                                         end_time,
#                                                         type
#                                                         )
#
#                         update_start_time = copy.deepcopy(start_time)
#                         data_getted_start_time = predata.datetime.min()
#                         if data_getted_start_time == update_start_time:
#                             coll.insert_many(
#                                 QA_util_to_json_from_pandas(predata[predata['datetime']>start_time])
#                             )
#                         else:
#                             QA_util_log_info(
#                                 'Trying updating {} from {} to {}, package: {}, Data Error: reason: start time does not match, start time: {}, database calculated start time: {}'
#                                 .format(code,
#                                         start_time,
#                                         end_time,
#                                         package,
#                                         data_getted_start_time,
#                                         update_start_time
#                                         ),
#                                 ui_log
#                             )
#                             err.append(str(code))
#                 else:
#                     start_time = '2010-01-01'
#                     QA_util_log_info(
#                         '##JOB03.{} Trying updating {} from {} to {} =={}, package: {}'.format(
#                             ['1min',
#                              '5min',
#                              '15min',
#                              '30min',
#                              '60min'].index(type),
#                             str(code),
#                             start_time,
#                             end_time,
#                             type,
#                             package
#                         ),
#                         ui_log=ui_log
#                     )
#                     predata = QA_fetch_get_index_min(
#                                                     package,
#                                                     str(code),
#                                                     start_time,
#                                                     end_time,
#                                                     type
#                                                     )
#
#                     coll.insert_many(
#                         QA_util_to_json_from_pandas(predata)
#                     )
#
#         except:
#             err.append(code)
#
#     executor = ThreadPoolExecutor(max_workers=4)
#
#     res = {
#         executor.submit(__saving_work,
#                         __index_list.index[i_][0],
#                         coll)
#         for i_ in range(len(__index_list))
#     }  # multi index ./.
#     count = 0
#     for i_ in concurrent.futures.as_completed(res):
#         strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
#             str(float(count / len(__index_list) * 100))[0:4] + '%'
#         )
#         intLogProgress = int(float(count / len(__index_list) * 10000.0))
#         QA_util_log_info(
#             'The {} of Total {}'.format(count,
#                                         len(__index_list)),
#             ui_log=ui_log
#         )
#         QA_util_log_info(
#             strLogProgress,
#             ui_log=ui_log,
#             ui_progress=ui_progress,
#             ui_progress_int_value=intLogProgress
#         )
#         count = count + 1
#     if len(err) < 1:
#         QA_util_log_info('SUCCESS', ui_log=ui_log)
#     else:
#         QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
#         QA_util_log_info(err, ui_log=ui_log)
#
#
#
# ########################################################################################################
#

#
#
# def QA_SU_save_index_list(package = None, client=DATABASE, ui_log=None, ui_progress=None):
#     index_list = QA_fetch_get_index_list(package = package)
#     coll_index_list = client.index_list
#     coll_index_list.create_index("code", unique=True)
#
#     try:
#         coll_index_list.insert_many(
#             QA_util_to_json_from_pandas(index_list),
#             ordered=False
#         )
#     except:
#         pass
#
#

#
#
# def QA_SU_save_future_day_all(package = None,client=DATABASE, ui_log=None, ui_progress=None):
#     '''
#      save future_day_all
#     保存日线数据(全部, 包含单月合约)
#     :param client:
#     :param ui_log:  给GUI qt 界面使用
#     :param ui_progress: 给GUI qt 界面使用
#     :param ui_progress_int_value: 给GUI qt 界面使用
#     :return:
#     '''
#     future_list = QA_fetch_get_future_list(package = package).code.unique().tolist()
#     coll = client.future_day
#     coll.create_index(
#         [("code",
#           pymongo.ASCENDING),
#          ("date_stamp",
#           pymongo.ASCENDING)]
#     )
#     err = []
#
#     def __saving_work(code, coll):
#         try:
#             QA_util_log_info(
#                 '##JOB12 Now Saving Future_DAY==== {}'.format(str(code)),
#                 ui_log
#             )
#
#             # 首选查找数据库 是否 有 这个代码的数据
#             ref = coll.find({'code': str(code)[0:4]})
#             end_date = str(now_time())[0:10]
#
#             # 当前数据库已经包含了这个代码的数据， 继续增量更新
#             # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现
#             if ref.count() > 0:
#                 # 接着上次获取的日期继续更新
#                 start_date = ref[ref.count() - 1]['date']
#
#                 QA_util_log_info(
#                     'UPDATE_Future_DAY \n Trying updating {} from {} to {}, package: {}'
#                     .format(code,
#                             start_date,
#                             end_date,
#                             package),
#                     ui_log
#                 )
#                 if start_date != end_date:
#                     update_start_date = QA_util_get_next_day(start_date)
#                     predata = QA_fetch_get_future_day(
#                                                     package,
#                                                     str(code),
#                                                     update_start_date,
#                                                     end_date,
#                                                     'day'
#                                                     )
#                     data_getted_start_date = predata.date.min()
#                     if data_getted_start_date == update_start_date:
#                         coll.insert_many(
#                             QA_util_to_json_from_pandas(predata)
#                         )
#                     else:
#                         QA_util_log_info(
#                             'Trying updating {} from {} to {}, package: {}, Data Error: reason: start date does not match, start date: {}, database calculated start date: {}'
#                             .format(code,
#                                     start_date,
#                                     end_date,
#                                     package,
#                                     data_getted_start_date,
#                                     update_start_date
#                                     ),
#                             ui_log
#                         )
#                         err.append(str(code))
#                 # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
#                 else:
#                     start_date = '1990-01-01'
#                     QA_util_log_info(
#                         'UPDATE_Future_DAY \n Trying updating {} from {} to {}, package: {}'
#                         .format(code,
#                                 start_date,
#                                 end_date,
#                                 package),
#                         ui_log
#                     )
#                     predata = QA_fetch_get_future_day(
#                                                     package,
#                                                     str(code),
#                                                     update_start_date,
#                                                     end_date,
#                                                     'day'
#                                                     )
#
#                     coll.insert_many(
#                         QA_util_to_json_from_pandas(predata)
#                     )
#
#         except Exception as error0:
#             print(error0)
#             err.append(str(code))
#
#     for item in range(len(future_list)):
#         QA_util_log_info('The {} of Total {}'.format(item, len(future_list)))
#
#         strProgressToLog = 'DOWNLOAD PROGRESS {} {}'.format(
#             str(float(item / len(future_list) * 100))[0:4] + '%',
#             ui_log
#         )
#         intProgressToLog = int(float(item / len(future_list) * 100))
#         QA_util_log_info(
#             strProgressToLog,
#             ui_log=ui_log,
#             ui_progress=ui_progress,
#             ui_progress_int_value=intProgressToLog
#         )
#
#         __saving_work(future_list[item], coll)
#
#     if len(err) < 1:
#         QA_util_log_info('SUCCESS save future day ^_^', ui_log)
#     else:
#         QA_util_log_info(' ERROR CODE \n ', ui_log)
#         QA_util_log_info(err, ui_log)
#
#
# def QA_SU_save_future_min(package = None,client=DATABASE, ui_log=None, ui_progress=None):
#     """save future_min
#
#     Keyword Arguments:
#         client {[type]} -- [description] (default: {DATABASE})
#     """
#
#     future_list = [
#         item for item in QA_fetch_get_future_list(package = package).code.unique().tolist()
#         if str(item)[-2:] in ['L8',
#                               'L9']
#     ]
#     coll = client.future_min
#     coll.create_index(
#         [
#             ('code',
#              pymongo.ASCENDING),
#             ('time_stamp',
#              pymongo.ASCENDING),
#             ('date_stamp',
#              pymongo.ASCENDING)
#         ]
#     )
#     err = []
#
#     def __saving_work(code, coll):
#
#         QA_util_log_info(
#             '##JOB13 Now Saving Future_MIN ==== {}'.format(str(code)),
#             ui_log=ui_log
#         )
#         try:
#
#             for type in ['1min', '5min', '15min', '30min', '60min']:
#                 ref_ = coll.find({'code': str(code)[0:6], 'type': type})
#                 end_time = str(now_time())[0:19]
#                 if ref_.count() > 0:
#                     start_time = ref_[ref_.count() - 1]['datetime']
#
#                     QA_util_log_info(
#                         '##JOB03.{} Trying updating {} from {} to {} =={}, package: {}'.format(
#                             ['1min',
#                              '5min',
#                              '15min',
#                              '30min',
#                              '60min'].index(type),
#                             str(code),
#                             start_time,
#                             end_time,
#                             type,
#                             package
#                         ),
#                         ui_log=ui_log
#                     )
#                     if start_time != end_time:
#                         predata = QA_fetch_get_future_min(
#                                                         package,
#                                                         str(code),
#                                                         start_time,
#                                                         end_time,
#                                                         type
#                                                         )
#
#                         update_start_time = copy.deepcopy(start_time)
#                         data_getted_start_time = predata.datetime.min()
#                         if data_getted_start_time == update_start_time:
#                             coll.insert_many(
#                                 QA_util_to_json_from_pandas(predata[predata['datetime']>start_time])
#                             )
#                         else:
#                             QA_util_log_info(
#                                 'Trying updating {} from {} to {}, package: {}, Data Error: reason: start time does not match, start time: {}, database calculated start time: {}'
#                                 .format(code,
#                                         start_time,
#                                         end_time,
#                                         package,
#                                         data_getted_start_time,
#                                         update_start_time
#                                         ),
#                                 ui_log
#                             )
#                             err.append(str(code))
#                 else:
#                     start_time = '2010-01-01'
#                     QA_util_log_info(
#                         '##JOB03.{} Trying updating {} from {} to {} =={}, package: {}'.format(
#                             ['1min',
#                              '5min',
#                              '15min',
#                              '30min',
#                              '60min'].index(type),
#                             str(code),
#                             start_time,
#                             end_time,
#                             type,
#                             package
#                         ),
#                         ui_log=ui_log
#                     )
#                     predata = QA_fetch_get_future_min(
#                                                     package,
#                                                     str(code),
#                                                     start_time,
#                                                     end_time,
#                                                     type
#                                                     )
#
#                     coll.insert_many(
#                         QA_util_to_json_from_pandas(predata)
#                     )
#
#         except:
#             err.append(code)
#
#     executor = ThreadPoolExecutor(max_workers=4)
#
#     res = {
#         executor.submit(__saving_work,
#                         future_list[i_],
#                         coll)
#         for i_ in range(len(future_list))
#     }  # multi index ./.
#     count = 0
#     for i_ in concurrent.futures.as_completed(res):
#         QA_util_log_info(
#             'The {} of Total {}'.format(count,
#                                         len(future_list)),
#             ui_log=ui_log
#         )
#         strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
#             str(float(count / len(future_list) * 100))[0:4] + '%'
#         )
#         intLogProgress = int(float(count / len(future_list) * 10000.0))
#
#         QA_util_log_info(
#             strLogProgress,
#             ui_log=ui_log,
#             ui_progress=ui_progress,
#             ui_progress_int_value=intLogProgress
#         )
#         count = count + 1
#     if len(err) < 1:
#         QA_util_log_info('SUCCESS', ui_log=ui_log)
#     else:
#         QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
#         QA_util_log_info(err, ui_log=ui_log)
#
#
# def QA_SU_save_future_min_all(package = None, client=DATABASE, ui_log=None, ui_progress=None):
#     """save future_min_all  (全部, 包含单月合约)
#
#     Keyword Arguments:
#         client {[type]} -- [description] (default: {DATABASE})
#     """
#
#     future_list = QA_fetch_get_future_list(package = package).code.unique().tolist()
#     coll = client.future_min
#     coll.create_index(
#         [
#             ('code',
#              pymongo.ASCENDING),
#             ('time_stamp',
#              pymongo.ASCENDING),
#             ('date_stamp',
#              pymongo.ASCENDING)
#         ]
#     )
#     err = []
#
#     def __saving_work(code, coll):
#
#         QA_util_log_info(
#             '##JOB13 Now Saving Future_MIN ==== {}'.format(str(code)),
#             ui_log=ui_log
#         )
#         try:
#
#             for type in ['1min', '5min', '15min', '30min', '60min']:
#                 ref_ = coll.find({'code': str(code)[0:6], 'type': type})
#                 end_time = str(now_time())[0:19]
#                 if ref_.count() > 0:
#                     start_time = ref_[ref_.count() - 1]['datetime']
#
#                     QA_util_log_info(
#                         '##JOB03.{} Trying updating {} from {} to {} =={}, package: {}'.format(
#                             ['1min',
#                              '5min',
#                              '15min',
#                              '30min',
#                              '60min'].index(type),
#                             str(code),
#                             start_time,
#                             end_time,
#                             type,
#                             package
#                         ),
#                         ui_log=ui_log
#                     )
#                     if start_time != end_time:
#                         predata = QA_fetch_get_future_min(
#                                                         package,
#                                                         str(code),
#                                                         start_time,
#                                                         end_time,
#                                                         type
#                                                         )
#
#                         update_start_time = copy.deepcopy(start_time)
#                         data_getted_start_time = predata.datetime.min()
#                         if data_getted_start_time == update_start_time:
#                             coll.insert_many(
#                                 QA_util_to_json_from_pandas(predata[predata['datetime']>start_time])
#                             )
#                         else:
#                             QA_util_log_info(
#                                 'Trying updating {} from {} to {}, package: {}, Data Error: reason: start time does not match, start time: {}, database calculated start time: {}'
#                                 .format(code,
#                                         start_time,
#                                         end_time,
#                                         package,
#                                         data_getted_start_time,
#                                         update_start_time
#                                         ),
#                                 ui_log
#                             )
#                             err.append(str(code))
#                 else:
#                     start_time = '2010-01-01'
#                     QA_util_log_info(
#                         '##JOB03.{} Trying updating {} from {} to {} =={}, package: {}'.format(
#                             ['1min',
#                              '5min',
#                              '15min',
#                              '30min',
#                              '60min'].index(type),
#                             str(code),
#                             start_time,
#                             end_time,
#                             type,
#                             package
#                         ),
#                         ui_log=ui_log
#                     )
#                     predata = QA_fetch_get_future_min(
#                                                     package,
#                                                     str(code),
#                                                     start_time,
#                                                     end_time,
#                                                     type
#                                                     )
#
#                     coll.insert_many(
#                         QA_util_to_json_from_pandas(predata)
#                     )
#         except:
#             err.append(code)
#
#     executor = ThreadPoolExecutor(max_workers=4)
#
#     res = {
#         executor.submit(__saving_work,
#                         future_list[i_],
#                         coll)
#         for i_ in range(len(future_list))
#     }  # multi index ./.
#     count = 0
#     for i_ in concurrent.futures.as_completed(res):
#         QA_util_log_info(
#             'The {} of Total {}'.format(count,
#                                         len(future_list)),
#             ui_log=ui_log
#         )
#         strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
#             str(float(count / len(future_list) * 100))[0:4] + '%'
#         )
#         intLogProgress = int(float(count / len(future_list) * 10000.0))
#
#         QA_util_log_info(
#             strLogProgress,
#             ui_log=ui_log,
#             ui_progress=ui_progress,
#             ui_progress_int_value=intLogProgress
#         )
#         count = count + 1
#     if len(err) < 1:
#         QA_util_log_info('SUCCESS', ui_log=ui_log)
#     else:
#         QA_util_log_info(' ERROR CODE \n ', ui_log=ui_log)
#         QA_util_log_info(err, ui_log=ui_log)


# def gen_param(codelist, start_date=None, end_date=None, if_fq='00', frequence='day', IPList=[]):
#     # 生成QA.QAFetch.QATdx.QA_fetch_get_stock_day多进程处理的参数
#     count = len(IPList)
#     my_iterator = iter(range(len(codelist)))
#     start_date = str(start_date)[0:10]
#     end_date = str(end_date)[0:10]
#     return [(code, start_date, end_date, if_fq, frequence, IPList[i % count]['ip'], IPList[i % count]['port'])
#             for code, i in [(code, next(my_iterator) % count) for code in codelist]]

if __name__ == '__main__':
    pass
    # QA_SU_save_stock_day()
    # QA_SU_save_stock_xdxr()
    # QA_SU_save_stock_min()
    # QA_SU_save_stock_transaction()
    # QA_SU_save_index_day()
    # QA_SU_save_stock_list()
    # QA_SU_save_index_min()
    # QA_SU_save_index_list()
    # QA_SU_save_future_list()

    # QA_SU_save_future_day()

#    QA_SU_save_future_min()
