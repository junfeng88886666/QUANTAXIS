import pandas as pd
import requests,json
from QUANTAXIS.QAUtil.QASetting import QASETTING
from QUANTAXIS.QAUtil.QATransform import QA_util_to_pandas_from_RequestsResponse
from QUANTAXIS.QAUtil import (QA_util_dateordatetime_valid,QA_util_log_info)
url="https://dataapi.joinquant.com/apis"

DEFAULT_ACCOUNT = '13018055851'
DEFAULT_PASSWORD = 'LIWEIQI199'

def get_config(account=None, password=None, remember=False):
    try:
        if account is None:
            account = QASETTING.get_config('JQDATA', 'account', None)
            if account == None:
                QASETTING.set_config('JQDATA', 'account', DEFAULT_ACCOUNT)
                account = DEFAULT_ACCOUNT
        else:
            if remember: QASETTING.set_config('JQDATA', 'account', account)

        if password is None:
            password = QASETTING.get_config('JQDATA', 'password', None)
            if password == None:
                QASETTING.set_config('JQDATA', 'password', DEFAULT_PASSWORD)
                password = DEFAULT_PASSWORD
        else:
            if remember: QASETTING.set_config('JQDATA', 'password', password)

        return (account, password)
    except:
        print('请升级jqdatasdk 至最新版本 pip install jqdatasdk -U')

def get_token(account = None, password = None):
    account,password = get_config(account=None, password=None, remember=False)
    
    body={
        "method": "get_token",
        "mob": account,  #mob是申请JQData时所填写的手机号
        "pwd": password,  #Password为聚宽官网登录密码，新申请用户默认为手机号后6位
    }
    
    
    response = requests.post(url, data = json.dumps(body))
    return response.text

    
def __get_all_securities(types=None, date=None,account = None, password = None):
    account,password = get_config(account=None, password=None, remember=False)
    token = get_token(account, password)
    
    body = {
        "method": "get_all_securities",
        "token": token,
        "code": types,
        "date":date
    }
    response = requests.post(url, data = json.dumps(body))
    data = QA_util_to_pandas_from_RequestsResponse(response).set_index('code')
    data.index.name = 'index'
    return data

def get_all_securities(types=None, date=None,account = None, password = None):
    if type(types) == str:
        return __get_all_securities(types,date,account,password)
    elif type(types) == list:
        data = pd.DataFrame()
        for i in types:
            temp = __get_all_securities(i,date,account,password)
            data = data.append(temp)
        return data

def get_price(security = None,
               start_date=None,
               end_date=None,
               frequency=None,
               fields=None,
               skip_paused=False,
               fq=None,
               count=None):
    if (QA_util_dateordatetime_valid(start_date)) & (QA_util_dateordatetime_valid(end_date)):
        account,password = get_config(account=None, password=None, remember=False)
        token = get_token(account, password)
        if fq in [None,'none']: fq_ref_date = None
        elif fq == 'pre': fq_ref_date = end_date
        elif fq == 'post': fq_ref_date = start_date
        else:
            assert False,"fq 参数应该是下列选项之一: None, 'pre', 'post', 'none'"
        body={
            "method": "get_price_period",
            "token": token
        }
        if security != None: body['code'] = security
        if start_date != None: body['date'] = start_date
        if end_date != None: body['end_date'] = end_date
        if frequency != None: body['unit'] = frequency
        if fq_ref_date != None: body['fq_ref_date'] = fq_ref_date
        response = requests.post(url, data = json.dumps(body))
        data = QA_util_to_pandas_from_RequestsResponse(response)
        data = data.set_index('date')
        data.index.name = 'index'
        if frequency == '1d':
            if skip_paused: data = data[data['paused'] == '0']
            data['paused'] = data['paused'].astype('int64')
            data['high_limit'] = data['high_limit'].astype('float64')
            data['low_limit'] = data['low_limit'].astype('float64')
        if fields == None: fields = ['open','close','high','low','volume','money']
        data['open'] = data['open'].astype('float64')
        data['close'] = data['close'].astype('float64')
        data['high'] = data['high'].astype('float64')
        data['low'] = data['low'].astype('float64')
        data['volume'] = data['volume'].astype('int64')
        data['money'] = data['money'].astype('float64')
        data.index = pd.to_datetime(data.index)
        return data[fields]
    else:
        QA_util_log_info(
            'QA Error QA_fetch_stock_transaction data parameter start=%s end=%s is not right' % (start, end))
        return None
                   
if __name__ == '__main__':      
    data0 = get_all_securities('stock')
    data1 = get_price(security = 'WR9999.XSGE',
                               start_date='2019-01-01',
                               end_date='2019-07-01',
                               frequency='1d',
                               fields=None,
                               skip_paused=False,
                               fq=None,
                               count=None)

