# coding:utf-8

import datetime

import numpy as np
import pandas as pd
from retrying import retry

from QUANTAXIS.QAFetch.base import _select_market_code, _select_type
from QUANTAXIS.QAUtil import (QA_Setting, QA_util_date_stamp,
                              QA_util_date_str2int, QA_util_date_valid,
                              QA_util_get_real_date, QA_util_get_real_datelist,
                              QA_util_future_to_realdatetime, QA_util_future_to_tradedatetime,
                              QA_util_get_trade_gap, QA_util_log_info,
                              QA_util_time_stamp, QA_util_web_ping,
                              exclude_from_stock_ip_list, future_ip_list,
                              stock_ip_list, trade_date_sse)
from QUANTAXIS.QAUtil.QASetting import QASETTING
from QUANTAXIS.QASetting.QALocalize import log_path
from QUANTAXIS.QAUtil import Parallelism
from QUANTAXIS.QAUtil.QACache import QA_util_cache
## TODO 当前只有期货日线和分钟数据的获取，没有其他的，待补充

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
def QA_fetch_get_stock_list(type_='stock', ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_index_list(ip=None, port=None):
    raise NotImplementedError

@retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
def QA_fetch_get_bond_list(ip=None, port=None):
    raise NotImplementedError

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

def QA_fetch_get_future_list():
    pass



def QA_fetch_get_globalindex_list(ip=None, port=None):
    """全球指数列表
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
       37        11  全球指数(静态)         FW
       12         5      国际指数         WI
    """
    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query('market==12 or market==37')


def QA_fetch_get_goods_list(ip=None, port=None):
    """[summary]
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
    42         3      商品指数         TI
    60         3    主力期货合约         MA
    28         3      郑州商品         QZ
    29         3      大连商品         QD
    30         3      上海期货(原油+贵金属)  QS
    47         3     中金所期货         CZ
    50         3      渤海商品         BH
    76         3      齐鲁商品         QL
    46        11      上海黄金(伦敦金T+D)         SG
    """

    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query('market==50 or market==76 or market==46')


def QA_fetch_get_globalfuture_list(ip=None, port=None):
    """[summary]
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
       14         3      伦敦金属         LM
       15         3      伦敦石油         IP
       16         3      纽约商品         CO
       17         3      纽约石油         NY
       18         3      芝加哥谷         CB
       19         3     东京工业品         TO
       20         3      纽约期货         NB
       77         3     新加坡期货         SX
       39         3      马来期货         ML
    """

    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query(
        'market==14 or market==15 or market==16 or market==17 or market==18 or market==19 or market==20 or market==77 or market==39')


def QA_fetch_get_hkstock_list(ip=None, port=None):
    """[summary]
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
# 港股 HKMARKET
       27         5      香港指数         FH
       31         2      香港主板         KH
       48         2     香港创业板         KG
       49         2      香港基金         KT
       43         1     B股转H股         HB
    """

    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query('market==31 or market==48')


def QA_fetch_get_hkindex_list(ip=None, port=None):
    """[summary]
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
# 港股 HKMARKET
       27         5      香港指数         FH
       31         2      香港主板         KH
       48         2     香港创业板         KG
       49         2      香港基金         KT
       43         1     B股转H股         HB
    """

    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query('market==27')


def QA_fetch_get_hkfund_list(ip=None, port=None):
    """[summary]
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
    # 港股 HKMARKET
        27         5      香港指数         FH
        31         2      香港主板         KH
        48         2     香港创业板         KG
        49         2      香港基金         KT
        43         1     B股转H股         HB
    """

    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query('market==49')


def QA_fetch_get_usstock_list(ip=None, port=None):
    """[summary]
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
    ## 美股 USA STOCK
        74        13      美国股票         US
        40        11     中国概念股         CH
        41        11    美股知名公司         MG
    """

    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query('market==74 or market==40 or market==41')


def QA_fetch_get_macroindex_list(ip=None, port=None):
    """宏观指标列表
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
        38        10      宏观指标         HG
    """
    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query('market==38')


def QA_fetch_get_option_list(ip=None, port=None):
    """期权列表
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
    ## 期权 OPTION
            1        12    临时期权(主要是50ETF)
            4        12    郑州商品期权         OZ
            5        12    大连商品期权         OD
            6        12    上海商品期权         OS
            7        12     中金所期权         OJ
            8        12    上海股票期权         QQ
            9        12    深圳股票期权      (推测)
    """
    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query('category==12 and market!=1')


def QA_fetch_get_option_contract_time_to_market():
    '''
    #🛠todo 获取期权合约的上市日期 ？ 暂时没有。
    :return: list Series
    '''
    result = QA_fetch_get_option_list('tdx')
    # pprint.pprint(result)
    #  category  market code name desc  code
    '''
    fix here : 
    See the caveats in the documentation: http://pandas.pydata.org/pandas-docs/stable/indexing.html#indexing-view-versus-copy
    result['meaningful_name'] = None
    C:\work_new\QUANTAXIS\QUANTAXIS\QAFetch\QATdx.py:1468: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    '''
    # df = pd.DataFrame()
    rows = []

    result['meaningful_name'] = None
    for idx in result.index:
        # pprint.pprint((idx))
        strCategory = result.loc[idx, "category"]
        strMarket = result.loc[idx, "market"]
        strCode = result.loc[idx, "code"]  # 10001215
        strName = result.loc[idx, 'name']  # 510050C9M03200
        strDesc = result.loc[idx, 'desc']  # 10001215

        if strName.startswith("510050"):
            # print(strCategory,' ', strMarket, ' ', strCode, ' ', strName, ' ', strDesc, )

            if strName.startswith("510050C"):
                putcall = '50ETF,认购期权'
            elif strName.startswith("510050P"):
                putcall = '50ETF,认沽期权'
            else:
                putcall = "Unkown code name ： " + strName

            expireMonth = strName[7:8]
            if expireMonth == 'A':
                expireMonth = "10月"
            elif expireMonth == 'B':
                expireMonth = "11月"
            elif expireMonth == 'C':
                expireMonth = "12月"
            else:
                expireMonth = expireMonth + '月'

            # 第12位期初设为“M”，并根据合约调整次数按照“A”至“Z”依序变更，如变更为“A”表示期权合约发生首次调整，变更为“B”表示期权合约发生第二次调整，依此类推；
            # fix here : M ??
            if strName[8:9] == "M":
                adjust = "未调整"
            elif strName[8:9] == 'A':
                adjust = " 第1次调整"
            elif strName[8:9] == 'B':
                adjust = " 第2调整"
            elif strName[8:9] == 'C':
                adjust = " 第3次调整"
            elif strName[8:9] == 'D':
                adjust = " 第4次调整"
            elif strName[8:9] == 'E':
                adjust = " 第5次调整"
            elif strName[8:9] == 'F':
                adjust = " 第6次调整"
            elif strName[8:9] == 'G':
                adjust = " 第7次调整"
            elif strName[8:9] == 'H':
                adjust = " 第8次调整"
            elif strName[8:9] == 'I':
                adjust = " 第9次调整"
            elif strName[8:9] == 'J':
                adjust = " 第10次调整"
            else:
                adjust = " 第10次以上的调整，调整代码 %s" + strName[8:9]

            executePrice = strName[9:]
            result.loc[idx, 'meaningful_name'] = '%s,到期月份:%s,%s,行权价:%s' % (
                putcall, expireMonth, adjust, executePrice)

            row = result.loc[idx]
            rows.append(row)

        elif strName.startswith("SR"):
            # print("SR")
            # SR1903-P-6500
            expireYear = strName[2:4]
            expireMonth = strName[4:6]

            put_or_call = strName[7:8]
            if put_or_call == "P":
                putcall = "白糖,认沽期权"
            elif put_or_call == "C":
                putcall = "白糖,认购期权"
            else:
                putcall = "Unkown code name ： " + strName

            executePrice = strName[9:]
            result.loc[idx, 'meaningful_name'] = '%s,到期年月份:%s%s,行权价:%s' % (
                putcall, expireYear, expireMonth, executePrice)

            row = result.loc[idx]
            rows.append(row)

            pass
        elif strName.startswith("CU"):
            # print("CU")

            # print("SR")
            # SR1903-P-6500
            expireYear = strName[2:4]
            expireMonth = strName[4:6]

            put_or_call = strName[7:8]
            if put_or_call == "P":
                putcall = "铜,认沽期权"
            elif put_or_call == "C":
                putcall = "铜,认购期权"
            else:
                putcall = "Unkown code name ： " + strName

            executePrice = strName[9:]
            result.loc[idx, 'meaningful_name'] = '%s,到期年月份:%s%s,行权价:%s' % (
                putcall, expireYear, expireMonth, executePrice)

            row = result.loc[idx]
            rows.append(row)

            pass
        # todo 新增期权品种 棉花，玉米， 天然橡胶
        elif strName.startswith("RU"):
            # print("M")
            # print(strName)
            ##
            expireYear = strName[2:4]
            expireMonth = strName[4:6]

            put_or_call = strName[7:8]
            if put_or_call == "P":
                putcall = "天然橡胶,认沽期权"
            elif put_or_call == "C":
                putcall = "天然橡胶,认购期权"
            else:
                putcall = "Unkown code name ： " + strName

            executePrice = strName[9:]
            result.loc[idx, 'meaningful_name'] = '%s,到期年月份:%s%s,行权价:%s' % (
                putcall, expireYear, expireMonth, executePrice)

            row = result.loc[idx]
            rows.append(row)

            pass

        elif strName.startswith("CF"):
            # print("M")
            # print(strName)
            ##
            expireYear = strName[2:4]
            expireMonth = strName[4:6]

            put_or_call = strName[7:8]
            if put_or_call == "P":
                putcall = "棉花,认沽期权"
            elif put_or_call == "C":
                putcall = "棉花,认购期权"
            else:
                putcall = "Unkown code name ： " + strName

            executePrice = strName[9:]
            result.loc[idx, 'meaningful_name'] = '%s,到期年月份:%s%s,行权价:%s' % (
                putcall, expireYear, expireMonth, executePrice)

            row = result.loc[idx]
            rows.append(row)

            pass

        elif strName.startswith("M"):
            # print("M")
            # print(strName)
            ##
            expireYear = strName[1:3]
            expireMonth = strName[3:5]

            put_or_call = strName[6:7]
            if put_or_call == "P":
                putcall = "豆粕,认沽期权"
            elif put_or_call == "C":
                putcall = "豆粕,认购期权"
            else:
                putcall = "Unkown code name ： " + strName

            executePrice = strName[8:]
            result.loc[idx, 'meaningful_name'] = '%s,到期年月份:%s%s,行权价:%s' % (
                putcall, expireYear, expireMonth, executePrice)

            row = result.loc[idx]
            rows.append(row)

            pass
        elif strName.startswith("C") and strName[1] != 'F' and strName[1] != 'U':
            # print("M")
            # print(strName)
            ##
            expireYear = strName[1:3]
            expireMonth = strName[3:5]

            put_or_call = strName[6:7]
            if put_or_call == "P":
                putcall = "玉米,认沽期权"
            elif put_or_call == "C":
                putcall = "玉米,认购期权"
            else:
                putcall = "Unkown code name ： " + strName

            executePrice = strName[8:]
            result.loc[idx, 'meaningful_name'] = '%s,到期年月份:%s%s,行权价:%s' % (
                putcall, expireYear, expireMonth, executePrice)

            row = result.loc[idx]
            rows.append(row)

            pass
        else:
            print("未知类型合约")
            print(strName)

    return rows


def QA_fetch_get_option_50etf_contract_time_to_market():
    '''
        #🛠todo 获取期权合约的上市日期 ？ 暂时没有。
        :return: list Series
        '''
    result = QA_fetch_get_option_list('tdx')
    # pprint.pprint(result)
    #  category  market code name desc  code
    '''
    fix here : 
    See the caveats in the documentation: http://pandas.pydata.org/pandas-docs/stable/indexing.html#indexing-view-versus-copy
    result['meaningful_name'] = None
    C:\work_new\QUANTAXIS\QUANTAXIS\QAFetch\QATdx.py:1468: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    '''
    # df = pd.DataFrame()
    rows = []

    result['meaningful_name'] = None
    for idx in result.index:
        # pprint.pprint((idx))
        strCategory = result.loc[idx, "category"]
        strMarket = result.loc[idx, "market"]
        strCode = result.loc[idx, "code"]  # 10001215
        strName = result.loc[idx, 'name']  # 510050C9M03200
        strDesc = result.loc[idx, 'desc']  # 10001215

        if strName.startswith("510050"):
            # print(strCategory,' ', strMarket, ' ', strCode, ' ', strName, ' ', strDesc, )

            if strName.startswith("510050C"):
                putcall = '50ETF,认购期权'
            elif strName.startswith("510050P"):
                putcall = '50ETF,认沽期权'
            else:
                putcall = "Unkown code name ： " + strName

            expireMonth = strName[7:8]
            if expireMonth == 'A':
                expireMonth = "10月"
            elif expireMonth == 'B':
                expireMonth = "11月"
            elif expireMonth == 'C':
                expireMonth = "12月"
            else:
                expireMonth = expireMonth + '月'

            # 第12位期初设为“M”，并根据合约调整次数按照“A”至“Z”依序变更，如变更为“A”表示期权合约发生首次调整，变更为“B”表示期权合约发生第二次调整，依此类推；
            # fix here : M ??
            if strName[8:9] == "M":
                adjust = "未调整"
            elif strName[8:9] == 'A':
                adjust = " 第1次调整"
            elif strName[8:9] == 'B':
                adjust = " 第2调整"
            elif strName[8:9] == 'C':
                adjust = " 第3次调整"
            elif strName[8:9] == 'D':
                adjust = " 第4次调整"
            elif strName[8:9] == 'E':
                adjust = " 第5次调整"
            elif strName[8:9] == 'F':
                adjust = " 第6次调整"
            elif strName[8:9] == 'G':
                adjust = " 第7次调整"
            elif strName[8:9] == 'H':
                adjust = " 第8次调整"
            elif strName[8:9] == 'I':
                adjust = " 第9次调整"
            elif strName[8:9] == 'J':
                adjust = " 第10次调整"
            else:
                adjust = " 第10次以上的调整，调整代码 %s" + strName[8:9]

            executePrice = strName[9:]
            result.loc[idx, 'meaningful_name'] = '%s,到期月份:%s,%s,行权价:%s' % (
                putcall, expireMonth, adjust, executePrice)

            row = result.loc[idx]
            rows.append(row)
    return rows


def QA_fetch_get_commodity_option_CF_contract_time_to_market():
    '''
    铜期权  CU 开头   上期证
    豆粕    M开头     大商所
    白糖    SR开头    郑商所
    测试中发现，行情不太稳定 ？ 是 通达信 IP 的问题 ？
    '''

    result = QA_fetch_get_option_list('tdx')
    # pprint.pprint(result)
    #  category  market code name desc  code

    # df = pd.DataFrame()
    rows = []
    result['meaningful_name'] = None
    for idx in result.index:
        # pprint.pprint((idx))
        strCategory = result.loc[idx, "category"]
        strMarket = result.loc[idx, "market"]
        strCode = result.loc[idx, "code"]  #
        strName = result.loc[idx, 'name']  #
        strDesc = result.loc[idx, 'desc']  #

        # 如果同时获取， 不同的 期货交易所数据， pytdx会 connection close 连接中断？
        # if strName.startswith("CU") or strName.startswith("M") or strName.startswith('SR'):
        if strName.startswith("CF"):
            # print(strCategory,' ', strMarket, ' ', strCode, ' ', strName, ' ', strDesc, )
            row = result.loc[idx]
            rows.append(row)

    return rows

    pass


def QA_fetch_get_commodity_option_RU_contract_time_to_market():
    '''
    铜期权  CU 开头   上期证
    豆粕    M开头     大商所
    白糖    SR开头    郑商所
    测试中发现，行情不太稳定 ？ 是 通达信 IP 的问题 ？
    '''

    result = QA_fetch_get_option_list('tdx')
    # pprint.pprint(result)
    #  category  market code name desc  code

    # df = pd.DataFrame()
    rows = []
    result['meaningful_name'] = None
    for idx in result.index:
        # pprint.pprint((idx))
        strCategory = result.loc[idx, "category"]
        strMarket = result.loc[idx, "market"]
        strCode = result.loc[idx, "code"]  #
        strName = result.loc[idx, 'name']  #
        strDesc = result.loc[idx, 'desc']  #

        # 如果同时获取， 不同的 期货交易所数据， pytdx会 connection close 连接中断？
        # if strName.startswith("CU") or strName.startswith("M") or strName.startswith('SR'):
        if strName.startswith("RU"):
            # print(strCategory,' ', strMarket, ' ', strCode, ' ', strName, ' ', strDesc, )
            row = result.loc[idx]
            rows.append(row)

    return rows

    pass


def QA_fetch_get_commodity_option_C_contract_time_to_market():
    '''
    铜期权  CU 开头   上期证
    豆粕    M开头     大商所
    白糖    SR开头    郑商所
    测试中发现，行情不太稳定 ？ 是 通达信 IP 的问题 ？
    '''

    result = QA_fetch_get_option_list('tdx')
    # pprint.pprint(result)
    #  category  market code name desc  code

    # df = pd.DataFrame()
    rows = []
    result['meaningful_name'] = None
    for idx in result.index:
        # pprint.pprint((idx))
        strCategory = result.loc[idx, "category"]
        strMarket = result.loc[idx, "market"]
        strCode = result.loc[idx, "code"]  #
        strName = result.loc[idx, 'name']  #
        strDesc = result.loc[idx, 'desc']  #

        # 如果同时获取， 不同的 期货交易所数据， pytdx会 connection close 连接中断？
        # if strName.startswith("CU") or strName.startswith("M") or strName.startswith('SR'):
        if strName.startswith("C") and strName[1] != 'F' and strName[1] != 'U':
            # print(strCategory,' ', strMarket, ' ', strCode, ' ', strName, ' ', strDesc, )
            row = result.loc[idx]
            rows.append(row)

    return rows

    pass


def QA_fetch_get_commodity_option_CU_contract_time_to_market():
    '''
    #🛠todo 获取期权合约的上市日期 ？ 暂时没有。
    :return: list Series
    '''
    result = QA_fetch_get_option_list('tdx')
    # pprint.pprint(result)
    #  category  market code name desc  code

    # df = pd.DataFrame()
    rows = []
    result['meaningful_name'] = None
    for idx in result.index:
        # pprint.pprint((idx))
        strCategory = result.loc[idx, "category"]
        strMarket = result.loc[idx, "market"]
        strCode = result.loc[idx, "code"]  #
        strName = result.loc[idx, 'name']  #
        strDesc = result.loc[idx, 'desc']  #

        # 如果同时获取， 不同的 期货交易所数据， pytdx会 connection close 连接中断？
        # if strName.startswith("CU") or strName.startswith("M") or strName.startswith('SR'):
        if strName.startswith("CU"):
            # print(strCategory,' ', strMarket, ' ', strCode, ' ', strName, ' ', strDesc, )
            row = result.loc[idx]
            rows.append(row)

    return rows


def QA_fetch_get_commodity_option_M_contract_time_to_market():
    '''
    #🛠todo 获取期权合约的上市日期 ？ 暂时没有。
    :return: list Series
    '''
    result = QA_fetch_get_option_list('tdx')
    # pprint.pprint(result)
    #  category  market code name desc  code
    '''
    铜期权  CU 开头   上期证
    豆粕    M开头     大商所
    白糖    SR开头    郑商所
    '''
    # df = pd.DataFrame()
    rows = []
    result['meaningful_name'] = None
    for idx in result.index:
        # pprint.pprint((idx))
        strCategory = result.loc[idx, "category"]
        strMarket = result.loc[idx, "market"]
        strCode = result.loc[idx, "code"]  #
        strName = result.loc[idx, 'name']  #
        strDesc = result.loc[idx, 'desc']  #

        # 如果同时获取， 不同的 期货交易所数据， pytdx connection close 连接中断？
        # if strName.startswith("CU") or strName.startswith("M") or strName.startswith('SR'):
        if strName.startswith("M"):
            # print(strCategory,' ', strMarket, ' ', strCode, ' ', strName, ' ', strDesc, )
            row = result.loc[idx]
            rows.append(row)

    return rows


def QA_fetch_get_commodity_option_SR_contract_time_to_market():
    '''
    #🛠todo 获取期权合约的上市日期 ？ 暂时没有。
    :return: list Series
    '''
    result = QA_fetch_get_option_list('tdx')
    # pprint.pprint(result)
    #  category  market code name desc  code
    '''
    铜期权  CU 开头   上期证
    豆粕    M开头     大商所
    白糖    SR开头    郑商所
    '''
    # df = pd.DataFrame()
    rows = []
    result['meaningful_name'] = None
    for idx in result.index:
        # pprint.pprint((idx))
        strCategory = result.loc[idx, "category"]
        strMarket = result.loc[idx, "market"]
        strCode = result.loc[idx, "code"]  #
        strName = result.loc[idx, 'name']  #
        strDesc = result.loc[idx, 'desc']  #

        # 如果同时获取， 不同的 期货交易所数据， pytdx connection close 连接中断？
        # if strName.startswith("CU") or strName.startswith("M") or strName.startswith('SR'):
        if strName.startswith("SR"):
            # print(strCategory,' ', strMarket, ' ', strCode, ' ', strName, ' ', strDesc, )
            row = result.loc[idx]
            rows.append(row)

    return rows


def QA_fetch_get_exchangerate_list(ip=None, port=None):
    """汇率列表
    Keyword Arguments:
        ip {[type]} -- [description] (default: {None})
        port {[type]} -- [description] (default: {None})
    ## 汇率 EXCHANGERATE
        10         4      基本汇率         FE
        11         4      交叉汇率         FX
    """
    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    return extension_market_list.query('market==10 or market==11').query('category==4')


def QA_fetch_get_future_day(code, start_date, end_date, frequence='day', ip=None, port=None):
    '期货数据 日线'
    ip, port = get_extensionmarket_ip(ip, port)
    apix = TdxExHq_API()
    start_date = str(start_date)[0:10]
    today_ = datetime.date.today()
    lens = QA_util_get_trade_gap(start_date, today_)
    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    with apix.connect(ip, port):
        code_market = extension_market_list.query(
            'code=="{}"'.format(code)).iloc[0]

        data = pd.concat(
            [apix.to_df(apix.get_instrument_bars(
                _select_type(frequence),
                int(code_market.market),
                str(code),
                (int(lens / 700) - i) * 700, 700)) for i in range(int(lens / 700) + 1)],
            axis=0)

        try:

            # 获取商品期货会报None
            data = data.assign(date=data['datetime'].apply(lambda x: str(x[0:10]))).assign(code=str(code)) \
                .assign(date_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(str(x)[0:10]))).set_index('date',
                                                                                                                 drop=False,
                                                                                                                 inplace=False)

        except Exception as exp:
            print("code is ", code)
            print(exp.__str__)
            return None

        return data.drop(['year', 'month', 'day', 'hour', 'minute', 'datetime'], axis=1)[start_date:end_date].assign(
            date=data['date'].apply(lambda x: str(x)[0:10]))


def QA_fetch_get_future_min(code, start, end, frequence='1min', ip=None, port=None):
    '期货数据 分钟线'
    ip, port = get_extensionmarket_ip(ip, port)
    apix = TdxExHq_API()
    type_ = ''
    start_date = str(start)[0:10]
    today_ = datetime.date.today()
    lens = QA_util_get_trade_gap(start_date, today_)
    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    if str(frequence) in ['5', '5m', '5min', 'five']:
        frequence, type_ = 0, '5min'
        lens = 48 * lens * 2.5
    elif str(frequence) in ['1', '1m', '1min', 'one']:
        frequence, type_ = 8, '1min'
        lens = 240 * lens * 2.5
    elif str(frequence) in ['15', '15m', '15min', 'fifteen']:
        frequence, type_ = 1, '15min'
        lens = 16 * lens * 2.5
    elif str(frequence) in ['30', '30m', '30min', 'half']:
        frequence, type_ = 2, '30min'
        lens = 8 * lens * 2.5
    elif str(frequence) in ['60', '60m', '60min', '1h']:
        frequence, type_ = 3, '60min'
        lens = 4 * lens * 2.5
    if lens > 20800:
        lens = 20800

    # print(lens)
    with apix.connect(ip, port):

        code_market = extension_market_list.query(
            'code=="{}"'.format(code)).iloc[0]
        data = pd.concat([apix.to_df(apix.get_instrument_bars(frequence, int(code_market.market), str(
            code), (int(lens / 700) - i) * 700, 700)) for i in range(int(lens / 700) + 1)], axis=0)
        # print(data)
        # print(data.datetime)
        data = data \
            .assign(tradetime=data['datetime'].apply(str), code=str(code)) \
            .assign(datetime=pd.to_datetime(data['datetime'].apply(QA_util_future_to_realdatetime, 1))) \
            .drop(['year', 'month', 'day', 'hour', 'minute'], axis=1, inplace=False) \
            .assign(date=data['datetime'].apply(lambda x: str(x)[0:10])) \
            .assign(date_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(x))) \
            .assign(time_stamp=data['datetime'].apply(lambda x: QA_util_time_stamp(x))) \
            .assign(type=type_).set_index('datetime', drop=False, inplace=False)
        return data.assign(datetime=data['datetime'].apply(lambda x: str(x)))[start:end].sort_index()


def __QA_fetch_get_future_transaction(code, day, retry, code_market, apix):
    batch_size = 1800  # 800 or 2000 ? 2000 maybe also works
    data_arr = []
    max_offset = 40
    cur_offset = 0

    while cur_offset <= max_offset:
        one_chunk = apix.get_history_transaction_data(
            code_market, str(code), QA_util_date_str2int(day), cur_offset * batch_size)

        if one_chunk is None or one_chunk == []:
            break
        data_arr = one_chunk + data_arr
        cur_offset += 1
    data_ = apix.to_df(data_arr)

    for _ in range(retry):
        if len(data_) < 2:
            import time
            time.sleep(1)
            return __QA_fetch_get_stock_transaction(code, day, 0, apix)
        else:
            return data_.assign(datetime=pd.to_datetime(data_['date'])).assign(date=str(day)) \
                .assign(code=str(code)).assign(order=range(len(data_.index))).set_index('datetime', drop=False,
                                                                                        inplace=False)


def QA_fetch_get_future_transaction(code, start, end, retry=4, ip=None, port=None):
    '期货历史成交分笔'
    ip, port = get_extensionmarket_ip(ip, port)
    apix = TdxExHq_API()
    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list
    real_start, real_end = QA_util_get_real_datelist(start, end)
    if real_start is None:
        return None
    real_id_range = []
    with apix.connect(ip, port):
        code_market = extension_market_list.query(
            'code=="{}"'.format(code)).iloc[0]
        data = pd.DataFrame()
        for index_ in range(trade_date_sse.index(real_start), trade_date_sse.index(real_end) + 1):

            try:
                data_ = __QA_fetch_get_future_transaction(
                    code, trade_date_sse[index_], retry, int(code_market.market), apix)
                if len(data_) < 1:
                    return None
            except Exception as e:
                print(e)
                QA_util_log_info('Wrong in Getting {} history transaction data in day {}'.format(
                    code, trade_date_sse[index_]))
            else:
                QA_util_log_info('Successfully Getting {} history transaction data in day {}'.format(
                    code, trade_date_sse[index_]))
                data = data.append(data_)
        if len(data) > 0:

            return data.assign(datetime=data['datetime'].apply(lambda x: str(x)[0:19]))
        else:
            return None


def QA_fetch_get_future_transaction_realtime(code, ip=None, port=None):
    '期货历史成交分笔'
    ip, port = get_extensionmarket_ip(ip, port)
    apix = TdxExHq_API()
    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list

    code_market = extension_market_list.query(
        'code=="{}"'.format(code)).iloc[0]
    with apix.connect(ip, port):
        data = pd.DataFrame()
        data = pd.concat([apix.to_df(apix.get_transaction_data(
            int(code_market.market), code, (30 - i) * 1800)) for i in range(31)], axis=0)
        return data.assign(datetime=pd.to_datetime(data['date'])).assign(date=lambda x: str(x)[0:10]) \
            .assign(code=str(code)).assign(order=range(len(data.index))).set_index('datetime', drop=False,
                                                                                   inplace=False)


def QA_fetch_get_future_realtime(code, ip=None, port=None):
    '期货实时价格'
    ip, port = get_extensionmarket_ip(ip, port)
    apix = TdxExHq_API()
    global extension_market_list
    extension_market_list = QA_fetch_get_extensionmarket_list(
    ) if extension_market_list is None else extension_market_list
    __data = pd.DataFrame()
    code_market = extension_market_list.query(
        'code=="{}"'.format(code)).iloc[0]
    with apix.connect(ip, port):
        __data = apix.to_df(apix.get_instrument_quote(
            int(code_market.market), code))
        __data['datetime'] = datetime.datetime.now()

        # data = __data[['datetime', 'active1', 'active2', 'last_close', 'code', 'open', 'high', 'low', 'price', 'cur_vol',
        #                's_vol', 'b_vol', 'vol', 'ask1', 'ask_vol1', 'bid1', 'bid_vol1', 'ask2', 'ask_vol2',
        #                'bid2', 'bid_vol2', 'ask3', 'ask_vol3', 'bid3', 'bid_vol3', 'ask4',
        #                'ask_vol4', 'bid4', 'bid_vol4', 'ask5', 'ask_vol5', 'bid5', 'bid_vol5']]
        return __data.set_index(['datetime', 'code'])


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


def QA_fetch_get_wholemarket_list():
    hq_codelist = QA_fetch_get_stock_list(
        type_='all').loc[:, ['code', 'name']].set_index(['code', 'name'], drop=False)
    kz_codelist = QA_fetch_get_extensionmarket_list().loc[:, ['code', 'name']].set_index([
        'code', 'name'], drop=False)

    return pd.concat([hq_codelist, kz_codelist]).sort_index()


if __name__ == '__main__':
    rows = QA_fetch_get_commodity_option_CU_contract_time_to_market()
    print(rows)

    print(QA_fetch_get_stock_day('000001', '2017-07-03', '2017-07-10'))
    print(QA_fetch_get_stock_day('000001', '2013-07-01', '2013-07-09'))
    # print(QA_fetch_get_stock_realtime('000001'))
    # print(QA_fetch_get_index_day('000001', '2017-01-01', '2017-07-01'))
    # print(QA_fetch_get_stock_transaction('000001', '2017-07-03', '2017-07-10'))

    # print(QA_fetch_get_stock_info('600116'))