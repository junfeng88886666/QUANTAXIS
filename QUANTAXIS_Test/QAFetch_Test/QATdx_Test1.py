# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 13:22:14 2019

@author: Administrator
"""
import numpy as np
import QUANTAXIS as QA
data = QA.QAFetch.QATdx.QA_fetch_get_stock_transaction('000001','2019-02-01 10:30:00','2019-02-03')
data = QA.QAFetch.QATdx.QA_fetch_get_stock_transaction('000001','2019-06-23 01:01:00','2019-06-27 02:01:00')


last_order = 500
data['order'] = np.arange(last_order+1,len(data)+last_order+1)



from QUANTAXIS.QAUtil.QAParameter import DATABASE_NAME


current_supported_update = {
    DATABASE_NAME.STOCK_DAY:{'tdx':{'data_type':[None]}},
    DATABASE_NAME.STOCK_MIN:{'tdx':{'data_type':['1min','5min','15min','30min','60min']}},
    DATABASE_NAME.STOCK_XDXR:{'tdx':{'data_type':[None]}},
}

database_name = DATABASE_NAME.STOCK_DAY
package = 'tdx'
data_type = None
current_supported_update[database_name][package]['data_type'] == None


def check(database_name = None, package = None, data_type = None):
    try:
        if data_type in current_supported_update[database_name][package]['data_type']:
            return True
        else:
            return False
    except:
        return False
