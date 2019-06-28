# coding:utf-8

import datetime
import pandas as pd

from QUANTAXIS.QAData import (QADataAggrement_CoFund,QADataAggrement_Tdx)
from QUANTAXIS.QAUtil import DATABASE, QA_util_log_info
from QUANTAXIS.QAUtil.QAParameter import ERRORTYPE,DATASOURCE,DATA_AGGREMENT_NAME,DATA_QUERY_INDEX_COLUMNS_UNIQUE

# TODO 当前只有COFund期货列表，日线和分钟线的数据协议

def use(package):
    if package in ['cof','CoFund','COFUND',DATASOURCE.COFUND]:
        return QADataAggrement_CoFund
    elif package in ['TDX','Tdx','pytdx',DATASOURCE.TDX]:
        return QADataAggrement_Tdx
    else: raise NotImplementedError

def select_DataAggrement(type):
    '''
    TODO: 增加B股，H股，国外市场的数据协议
    :param type: 输入数据库名称
    :return: 经过数据协议处理过后的DataFrame
    '''
    ############ A股
    ### A股股票日线数据协议
    if type == DATA_AGGREMENT_NAME.STOCK_DAY: return QA_DataAggrement_Stock_day
    ### A股股票分钟线数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_MIN: return QA_DataAggrement_Stock_min
    ### A股股票tick数据数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_TRANSACTION: return QA_DataAggrement_Stock_transaction
    ### A股最近交易周期实时数据数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_LATEST: return QA_DataAggrement_Stock_latest
    ### A股实时盘口数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_REALTIME: return QA_DataAggrement_Stock_realtime
    ### A股实时深度盘口数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_DEPTH_MARKET_DATA: return QA_DataAggrement_Stock_depth_market_data
    ### A股股票列表数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_LIST: return QA_DataAggrement_Stock_list
    ### A股tick实时数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_TRANSACTION_REALTIME: return QA_DataAggrement_Stock_transaction_realtime
    ### A股除权除息数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_XDXR: return QA_DataAggrement_Stock_xdxr
    ### A股股票详情信息数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_INFO: return QA_DataAggrement_Stock_info
    ### A股板块数据协议
    elif type == DATA_AGGREMENT_NAME.STOCK_BLOCK: return QA_DataAggrement_Stock_block



    # '''指数'''
    # elif type == DATA_AGGREMENT_NAME.INDEX_DAY: return QA_DataAggrement_Index_day
    # elif typSe == DATA_AGGREMENT_NAME.INDEX_MIN: return QA_DataAggrement_Index_min
    # elif type == DATA_AGGREMENT_NAME.INDEX_LSIST: return QA_DataAggrement_Index_list
    #
    # '''基金'''
    # elif type == DATA_AGGREMENT_NAME.FUND_DAY: return QA_DataAggrement_Fund_day
    # elif type == DATA_AGGREMENT_NAME.FUND_MIN: return QA_DataAggrement_Fund_min
    # elif type == DATA_AGGREMENT_NAME.FUND_LIST: return QA_DataAggrement_Fund_list
    #
    ############ 期货
    elif type == DATA_AGGREMENT_NAME.FUTURE_DAY: return QA_DataAggrement_Future_day
    elif type == DATA_AGGREMENT_NAME.FUTURE_MIN: return QA_DataAggrement_Future_min
    elif type == DATA_AGGREMENT_NAME.FUTURE_TRANSACTION: return QA_DataAggrement_Future_transaction
    # elif type == DATA_AGGREMENT_NAME.FUTURE_LIST: return QA_DataAggrement_Future_list

def __QA_DataAggrement_check_QAQuery(data,index_columns_unique):
    __check = index_columns_unique
    data = data.set_index(__check[0], drop=False, inplace=False)
    if type(data.index) == pd.core.indexes.multi.MultiIndex: pass
    else: data.index.name = 'index'
    data = data.drop_duplicates((__check[2]))
    data = data.sort_values(by=__check[2])
    assert set(__check[1]).issubset(data.columns.tolist())
    return data

def QA_DataAggrement_Stock_day(package,data):
    '''
    该数据协议为：返回的数据应包含以下列和对应的数据格式，若无该列数据，则填充0
        index:[str] 真实的交易日
        open [float64] 开盘价
        high [float64] 最高价
        low [float64] 最低价
        close [float64] 收盘价
        volume [float64] 成交量
        amount [float64] 成交额
        date [str] 真实的交易日
        date_stamp [float64] 真实的交易日的时间戳
        source [str] 数据来源
    :return: 经过数据协议调整格式后的国内期货分钟数据数据集
    '''
    try:
        if package == None: pass
        else:
            Engine = use(package)
            data = Engine.QA_DataAggrement_Stock_day(data)

        data['volume'][data['volume'] < 1] = 0
        data['amount'][data['amount'] < 1] = 0

        data[['code',
              'date',
              'source']] \
        = data[['code',
                'date',
                'source']].astype(str)

        data[['open',
              'high',
              'low',
              'close',
              'amount']]\
        = data[['open',
                'high',
                'low',
                'close',
                'amount']].astype('float64')

        data[['date_stamp','volume']] \
        = data[['date_stamp','volume']].astype('int64')

        '''为了保证这里的数据和QAQuery结果的一致性'''
        data = __QA_DataAggrement_check_QAQuery(data, DATA_QUERY_INDEX_COLUMNS_UNIQUE.STOCK_DAY)

        return data[['code','open','high','low','close','volume','amount','date','date_stamp','source']]
    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package)+'\n '+'           Error Reason: '+str(e))
        return None

def QA_DataAggrement_Stock_min(package,data):
    '''
    该数据协议为：返回的数据应包含以下列和对应的数据格式，若无该列数据，则填充0
        index:[str] 真实的交易日
        open [float64] 开盘价
        high [float64] 最高价
        low [float64] 最低价
        close [float64] 收盘价
        volume [float64] 成交量
        amount [float64] 成交额
        date [str] 真实的交易日
        date_stamp [float64] 真实的交易日的时间戳
        source [str] 数据来源
    :return: 经过数据协议调整格式后的国内期货分钟数据数据集
    '''
    try:
        if package == None: pass
        else:
            Engine = use(package)
            data = Engine.QA_DataAggrement_Stock_min(data)

        data['volume'][data['volume'] < 1] = 0
        data['amount'][data['amount'] < 1] = 0

        data[['code',
              'datetime',
              'date',
              'type',
              'source']]\
        = data[['code',
                'datetime',
                'date',
                'type',
                'source']].astype(str)

        data[['open',
              'high',
              'low',
              'close',
              'amount']]\
        = data[['open',
                'high',
                'low',
                'close',
                'amount']].astype('float64')

        data[['date_stamp',
              'time_stamp',
              'volume']] \
        = data[['date_stamp',
                'time_stamp',
                'volume']].astype('int64')

        '''为了保证这里的数据和QAQuery结果的一致性'''
        data = __QA_DataAggrement_check_QAQuery(data, DATA_QUERY_INDEX_COLUMNS_UNIQUE.STOCK_MIN)

        return data[['code','open','high','low','close','volume','amount','datetime','date','date_stamp','time_stamp','type','source']]
    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package)+'\n '+'           Error Reason: '+str(e))
        return None

def QA_DataAggrement_Stock_transaction(package,data):
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Stock_transaction(data)
        data[['datetime',
              'code',
              'date',
              'time',
              'source']] \
        = data[['datetime',
              'code',
              'date',
              'time',
              'source']].astype(str)

        data[['price']] \
        = data[['price']].astype('float64')

        data[['buyorsell',
              'volume',
              'order',
              'time_stamp']] \
        = data[['buyorsell',
                'volume',
                'order',
                'time_stamp']].astype('int64')

        '''为了保证这里的数据和QAQuery结果的一致性'''
        data = __QA_DataAggrement_check_QAQuery(data, DATA_QUERY_INDEX_COLUMNS_UNIQUE.STOCK_TRANSACTION)

        return data[['datetime','code','price','volume','buyorsell','date','time','order','time_stamp','source']]
    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package)+'\n '+'           Error Reason: '+str(e))
        return None

def QA_DataAggrement_Stock_latest(package,data):
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Stock_latest(data)
        data[['date',
              'code',
              'source']] \
        = data[['date',
                'code',
                'source']].astype(str)

        data[['open',
              'high',
              'low',
              'close',
              'volume',
              'amount']] \
        = data[['open',
              'high',
              'low',
              'close',
              'volume',
              'amount']].astype('float64')

        data[['date_stamp']] \
        = data[['date_stamp']].astype('int64')

        data = data.set_index('date', drop=False, inplace=False)
        data.index.name = 'index'
        return data[['date','code','open','high','low','close','volume','amount','date_stamp','source']]
    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package)+'\n '+'           Error Reason: '+str(e))
        return None

def QA_DataAggrement_Stock_realtime(package,data):
    '''
    当前此数据协议不对数据的字符格式做定义，追求速度
    '''
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Stock_realtime(data)
        # data[['datetime',
        #       'code',
        #       'source']] \
        # = data[['datetime',
        #         'code',
        #         'source']].astype(str)
        #
        # data[['active1', 'active2', 'last_close', 'open', 'high', 'low', 'price', 'cur_vol',
        #      's_vol', 'b_vol', 'vol', 'ask1', 'ask_vol1', 'bid1', 'bid_vol1', 'ask2', 'ask_vol2',
        #      'bid2', 'bid_vol2', 'ask3', 'ask_vol3', 'bid3', 'bid_vol3', 'ask4',
        #      'ask_vol4', 'bid4', 'bid_vol4', 'ask5', 'ask_vol5', 'bid5', 'bid_vol5']] \
        # = data[['active1', 'active2', 'last_close', 'open', 'high', 'low', 'price', 'cur_vol',
        #      's_vol', 'b_vol', 'vol', 'ask1', 'ask_vol1', 'bid1', 'bid_vol1', 'ask2', 'ask_vol2',
        #      'bid2', 'bid_vol2', 'ask3', 'ask_vol3', 'bid3', 'bid_vol3', 'ask4',
        #      'ask_vol4', 'bid4', 'bid_vol4', 'ask5', 'ask_vol5', 'bid5', 'bid_vol5']].astype('float64')

        data = data.set_index(['datetime','code'], drop=True, inplace=False)
        return data[['active1', 'active2', 'last_close', 'open', 'high', 'low', 'price', 'cur_vol',
             's_vol', 'b_vol', 'vol', 'ask1', 'ask_vol1', 'bid1', 'bid_vol1', 'ask2', 'ask_vol2',
             'bid2', 'bid_vol2', 'ask3', 'ask_vol3', 'bid3', 'bid_vol3', 'ask4',
             'ask_vol4', 'bid4', 'bid_vol4', 'ask5', 'ask_vol5', 'bid5', 'bid_vol5','source']]
    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package)+'\n '+'           Error Reason: '+str(e))
        return None

def QA_DataAggrement_Stock_depth_market_data(package,data):
    '''
    当前此数据协议不对数据的字符格式做定义，追求速度
    '''
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Stock_depth_market_data(data)
        return data.set_index(['datetime','code'], drop=True, inplace=False)
    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package)+'\n '+'           Error Reason: '+str(e))
        return None

def QA_DataAggrement_Stock_transaction_realtime(package,data):
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Stock_transaction_realtime(data)

        data[['datetime',
              'code',
              'date',
              'time',
              'source']] \
        = data[['datetime',
              'code',
              'date',
              'time',
              'source']].astype(str)

        data[['price']] \
        = data[['price']].astype('float64')

        data[['num',
              'buyorsell',
              'order']] \
        = data[['num',
                'buyorsell',
                'order']].astype('int64')

        data = data.set_index('datetime', drop=False, inplace=False)
        return data[['datetime','code','price','volume','num','buyorsell','date','time','order','source']]

    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package)+'\n '+'           Error Reason: '+str(e))
        return None

def QA_DataAggrement_Stock_list(package,data):
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Stock_list(data)
        data[['code',
              'name',
              'sse',
              'sec',
              'source']] \
        = data[['code',
                'name',
                'sse',
                'sec',
                'source']].astype(str)

        data[['pre_close']] \
        = data[['pre_close']].astype('float64')

        data[['volunit',
              'decimal_point']] \
        = data[['volunit',
              'decimal_point']].astype('int64')
        data = __QA_DataAggrement_check_QAQuery(data, DATA_QUERY_INDEX_COLUMNS_UNIQUE.STOCK_LIST)
        return data[['code','name','sse','sec','volunit','decimal_point','pre_close','source']]

    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package)+'\n '+'           Error Reason: '+str(e))
        return None

def QA_DataAggrement_Stock_xdxr(package,data):
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Stock_xdxr(data)

        data[['name',
              'date',
              'category_meaning',
              'code',
              'source']] \
        = data[['name',
              'date',
              'category_meaning',
              'code',
                'source']].astype(str)

        data[['fenhong',
              'peigujia',
              'songzhuangu',
              'peigu',
              'suogu',
              'liquidity_before',
              'liquidity_after',
              'shares_before',
              'shares_after',
              'fenshu',
              'xingquanjia']] \
        = data[['fenhong',
              'peigujia',
              'songzhuangu',
              'peigu',
              'suogu',
              'liquidity_before',
              'liquidity_after',
              'shares_before',
              'shares_after',
              'fenshu',
              'xingquanjia']].astype('float64')

        data[['category']] \
        = data[['category']].astype('int64')

        data = __QA_DataAggrement_check_QAQuery(data, DATA_QUERY_INDEX_COLUMNS_UNIQUE.STOCK_XDXR)
        return data[['name',
                     'category',
                      'date',
                      'category_meaning',
                      'code',
                      'fenhong',
                      'peigujia',
                      'songzhuangu',
                      'peigu',
                      'suogu',
                      'liquidity_before',
                      'liquidity_after',
                      'shares_before',
                      'shares_after',
                      'fenshu',
                      'xingquanjia',
                     'source'
                      ]]

    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package)+'\n '+'           Error Reason: '+str(e))
        return None

def QA_DataAggrement_Stock_info(package,data):
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Stock_info(data)
        data[['code',
              'updated_date',
              'ipo_date',
              'source']]\
        = data[['code',
                'updated_date',
                'ipo_date',
                'source']].astype(str)

        data[['liutongguben',
              'zongguben',
              'guojiagu',
              'faqirenfarengu',
              'farengu',
              'bgu',
              'hgu',
              'zhigonggu',
              'zongzichan',
              'liudongzichan',
              'gudingzichan',
              'wuxingzichan',
              'liudongfuzhai',
              'changqifuzhai',
              'zibengongjijin',
              'jingzichan',
              'zhuyingshouru',
              'zhuyinglirun',
              'yingshouzhangkuan',
              'yingyelirun',
              'touzishouyu',
              'jingyingxianjinliu',
              'zongxianjinliu',
              'cunhuo',
              'lirunzonghe',
              'shuihoulirun',
              'jinglirun',
              'weifenpeilirun',
              'meigujingzichan',
              'baoliu2']]\
        = data[['liutongguben',
              'zongguben',
              'guojiagu',
              'faqirenfarengu',
              'farengu',
              'bgu',
              'hgu',
              'zhigonggu',
              'zongzichan',
              'liudongzichan',
              'gudingzichan',
              'wuxingzichan',
              'liudongfuzhai',
              'changqifuzhai',
              'zibengongjijin',
              'jingzichan',
              'zhuyingshouru',
              'zhuyinglirun',
              'yingshouzhangkuan',
              'yingyelirun',
              'touzishouyu',
              'jingyingxianjinliu',
              'zongxianjinliu',
              'cunhuo',
              'lirunzonghe',
              'shuihoulirun',
              'jinglirun',
              'weifenpeilirun',
              'meigujingzichan',
              'baoliu2']].astype('float64')

        data[['market',
              'province',
              'industry',
              'gudongrenshu']]\
        = data[['market',
              'province',
              'industry',
              'gudongrenshu']].astype('int64')

        data = __QA_DataAggrement_check_QAQuery(data, DATA_QUERY_INDEX_COLUMNS_UNIQUE.STOCK_INFO)
        return data[['code',
                      'updated_date',
                      'ipo_date',
                     'liutongguben',
                     'zongguben',
                     'guojiagu',
                     'faqirenfarengu',
                     'farengu',
                     'bgu',
                     'hgu',
                     'zhigonggu',
                     'zongzichan',
                     'liudongzichan',
                     'gudingzichan',
                     'wuxingzichan',
                     'liudongfuzhai',
                     'changqifuzhai',
                     'zibengongjijin',
                     'jingzichan',
                     'zhuyingshouru',
                     'zhuyinglirun',
                     'yingshouzhangkuan',
                     'yingyelirun',
                     'touzishouyu',
                     'jingyingxianjinliu',
                     'zongxianjinliu',
                     'cunhuo',
                     'lirunzonghe',
                     'shuihoulirun',
                     'jinglirun',
                     'weifenpeilirun',
                     'meigujingzichan',
                     'baoliu2',
                     'market',
                     'province',
                     'industry',
                     'gudongrenshu',
                     'source']]

    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR+', package: '+str(package))
        QA_util_log_info(e)
        return None

def QA_DataAggrement_Stock_block(package,data):
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Stock_block(data)
        data[['blockname', 'code', 'type','enter_date', 'source']]\
        = data[['blockname','code','type','enter_date','source']].astype(str)

        data = __QA_DataAggrement_check_QAQuery(data, DATA_QUERY_INDEX_COLUMNS_UNIQUE.STOCK_BLOCK)
        return data[['blockname','code','type','enter_date','source']]
    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package))
        QA_util_log_info(e)
        return None

def QA_DataAggrement_Future_day(package,data):
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Future_day(data)

        data[['date',
              'code',
              'contract',
              'source']]\
        = data[['date',
                'code',
                'contract',
                'source']].astype(str)

        data[['open',
              'high',
              'low',
              'close',
              'price']]\
        = data[['open',
              'high',
              'low',
              'close',
              'price']].astype('float64')

        data[['position',
              'trade',
              'date_stamp']]\
        = data[['position',
              'trade',
              'date_stamp']].astype('int64')
        data = data.set_index('date',drop = False,inplace = False)
        return data[['date',
                      'code',
                      'open',
                      'high',
                      'low',
                      'close',
                      'price',
                      'position',
                      'trade',
                      'date_stamp',
                      'contract',
                      'source']]
    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package))
        QA_util_log_info(e)
        return None

def QA_DataAggrement_Future_min(package,data):
    '''
    该数据协议为：返回的数据应包含以下列和对应的数据格式，若无该列数据，则填充0
        index:[str] 真实的交易时间
        open [float64] 开盘价
        high [float64] 最高价
        low [float64] 最低价
        close [float64] 收盘价
        price [float64] 结算价s
        position [float64] 持仓量
        trade [float64] 交易量
        amount [float64] 成交额
        datetime [str] 真实的交易时间
        tradetime [str] 交易的交易时间(晚上21点之后进入下一天)
        code [str] 品种代码
        contract [str] 品种合约
        date [str] 交易日期
        date_stamp [float64] 真实的交易日期的时间戳
        time_stamp [float64] 真实的交易时间的时间戳
        type [str] 级别
        source [str] 数据来源
    :return: 经过数据协议调整格式后的国内期货分钟数据数据集
    '''
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Future_min(data)

        data[['datetime',
              'tradetime',
              'code',
              'contract',
              'date',
              'type',
              'source']]\
        = data[['datetime',
                'tradetime',
                'code',
                'contract',
                'date',
                'type',
                'source']].astype(str)

        data[['open',
              'high',
              'low',
              'close',
              'price']] \
        = data[['open',
                'high',
                'low',
                'close',
                'price']].astype('float64')

        data[['position',
              'trade',
              'date_stamp',
              'time_stamp']]\
        = data[['position',
                'trade',
                'date_stamp',
                'time_stamp']].astype('int64')

        data = data.set_index('datetime',drop = False,inplace = False)
        return data[['open','high','low','close','price','position','trade','datetime','tradetime','code','date','date_stamp','time_stamp','type','contract','source']]
    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package))
        QA_util_log_info(e)
        return None

def QA_DataAggrement_Future_transaction(package,data):
    try:
        Engine = use(package)
        data = Engine.QA_DataAggrement_Future_min(data)

        data[['datetime',
              'date',
              'nature_name',
              'code',
              'contract',
              'source']]\
        = data[['datetime',
              'date',
              'nature_name',
              'code',
              'contract',
              'source']].astype(str)

        data[['price']] \
        = data[['price']].astype('float64')

        data[['hour',
              'minute',
              'volume',
              'zengcang',
              'direction',
              'nature',
              'order']]\
        = data[['hour',
              'minute',
              'volume',
              'zengcang',
              'direction',
              'nature',
              'order']].astype('int64')

        data = data.set_index('datetime',drop = False,inplace = False)
        return data[['datetime',
                      'date',
                      'nature_name',
                      'code',
                      'price',
                      'hour',
                      'minute',
                      'volume',
                      'zengcang',
                      'direction',
                      'nature',
                      'order',
                      'contract',
                      'source']]

    except Exception as e:
        QA_util_log_info(ERRORTYPE.DATAAGGREMENT_ERROR + ', package: ' + str(package))
        QA_util_log_info(e)
        return None


def QA_DataAggrement_Future_list(package,data):
    Engine = use(package)
    return Engine.QA_DataAggrement_Future_list(data)
