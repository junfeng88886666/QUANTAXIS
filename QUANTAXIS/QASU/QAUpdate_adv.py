# coding:utf-8
from QUANTAXIS.QASU import save_adv
from QUANTAXIS.QAUtil.QAParameter import DATABASE_NAME,DATA_SOURCE
from QUANTAXIS.QAUtil.QALogs import QA_util_log_info
from QUANTAXIS import __version__ as QAVERSION

DATABASE_NAME_ALL = [i for i in vars(DATABASE_NAME).values() if type(i)==str][2:]

current_supported_update = {
    DATABASE_NAME.STOCK_LIST: {DATA_SOURCE.TDX:{'data_type':[None]}},
    DATABASE_NAME.STOCK_DAY:{DATA_SOURCE.TDX:{'data_type':[None]}},
    DATABASE_NAME.STOCK_MIN:{DATA_SOURCE.TDX:{'data_type':['1min','5min','15min','30min','60min']}},
    DATABASE_NAME.STOCK_TRANSACTION:{DATA_SOURCE.TDX:{'data_type':[None]}},
    DATABASE_NAME.STOCK_XDXR:{DATA_SOURCE.TDX:{'data_type':[None]}},
}


def check_update_permission(database_name=None, package=None, data_type=None):
    try:
        if data_type in current_supported_update[database_name][package]['data_type']:
            return True
        else:
            return False
    except:
        return False


def QA_Update(update_dict = {
                            # DATABASE_NAME.FUTURE_LIST: {'tdx':None},
                            # DATABASE_NAME.FUTURE_DAY: {'tdx':None},
                            # DATABASE_NAME.FUTURE_TRANSACTION: {'tdx':None},
                            # DATABASE_NAME.FUTURE_MIN: {'tdx':{'data_type':['1min','5min','15min','30min','60min']}},

                            DATABASE_NAME.STOCK_LIST:{DATA_SOURCE.TDX:None},
                            # DATABASE_NAME.STOCK_DAY:{'tdx':None},
                            DATABASE_NAME.STOCK_TRANSACTION:{DATA_SOURCE.TDX:None},
                            # DATABASE_NAME.STOCK_MIN:'tdx',
                            # DATABASE_NAME.STOCK_XDXR:'tdx',
                            # DATABASE_NAME.STOCK_BLOCK: 'tdx',
                            #
                            # DATABASE_NAME.INDEX_LIST:'tdx',
                            # DATABASE_NAME.INDEX_DAY:'tdx',
                            # DATABASE_NAME.INDEX_MIN:'tdx',

                            # DATABASE_NAME.FINANCIAL:None
                            },
                            ui_log = None,
                            ui_progress = None):

    '''
    2019/04/28
    TODO: 对不同的financial包增加裁定，当前只使用通达信financial包
    注：股票的分钟数据从其他数据源对接
       期货的分钟数据由tick合成
    :param update_dict:
    :return:
    '''
    QA_util_log_info('QUANTAXIS {} Include these DataBases: {}'.format(QAVERSION,DATABASE_NAME_ALL),ui_log)
    QA_util_log_info('DataBases update this time: {}'.format(update_dict),ui_log)

    for update_database in update_dict.keys():
        update_package = list(update_dict[update_database].keys())[0]

        if update_dict[update_database][update_package] == None:
            QA_SU_update_single(database_name = update_database,package = update_package, ui_log = ui_log)
        else:
            for update_data_type in update_dict[update_database][update_package]['data_type']:
                QA_SU_update_single(database_name = update_database, package = update_package, data_type = update_data_type, ui_log = ui_log)

def QA_SU_update_single(database_name = None,package = None,data_type = None, ui_log = None):
    if check_update_permission(database_name=database_name, package=package, data_type=data_type):
        if database_name == DATABASE_NAME.STOCK_LIST: save_adv.QA_SU_save_stock_list(package = package)
        elif database_name == DATABASE_NAME.STOCK_DAY: save_adv.QA_SU_save_stock_day(package = package)
        elif database_name == DATABASE_NAME.STOCK_TRANSACTION: save_adv.QA_SU_save_stock_transaction(package = package)
        # elif database_name == DATABASE_NAME.STOCK_MIN: save_adv.QA_SU_save_stock_min(package = package,data_type = data_type)

    else:
        QA_util_log_info('Error: DataBase: {}, package: {}, data type: {}; is not supported currently'.format(database_name,package,str(data_type)), ui_log)











    #
    #
    # if DATABASE_NAME.STOCK_LIST in update_dict.keys():
    #     save_adv.QA_SU_save_stock_list(package = update_dict[DATABASE_NAME.STOCK_LIST])
    #
    # if DATABASE_NAME.STOCK_DAY in update_dict.keys():
    #     save_adv.QA_SU_save_stock_day(package = update_dict[DATABASE_NAME.STOCK_DAY])
    #
    # if DATABASE_NAME.STOCK_MIN in update_dict.keys():
    #     save_adv.QA_SU_save_stock_min(package = update_dict[DATABASE_NAME.STOCK_MIN])
    #
    # if DATABASE_NAME.STOCK_TRANSACTION in update_dict.keys():
    #     save_adv.QA_SU_save_stock_transaction(package = update_dict[DATABASE_NAME.STOCK_TRANSACTION])
    #
    # if DATABASE_NAME.STOCK_XDXR in update_dict.keys():
    #     save_adv.QA_SU_save_stock_xdxr(package = update_dict[DATABASE_NAME.STOCK_XDXR])
    #
    # if DATABASE_NAME.STOCK_BLOCK in update_dict.keys():
    #     save_adv.QA_SU_save_stock_block(package = update_dict[DATABASE_NAME.STOCK_BLOCK])
    #
    # if DATABASE_NAME.INDEX_LIST in update_dict.keys():
    #     save_adv.QA_SU_save_index_list(package = update_dict[DATABASE_NAME.INDEX_LIST])
    #
    # if DATABASE_NAME.INDEX_DAY in update_dict.keys():
    #     save_adv.QA_SU_save_index_day(package = update_dict[DATABASE_NAME.INDEX_DAY])
    #
    # if DATABASE_NAME.INDEX_MIN in update_dict.keys():
    #     save_adv.QA_SU_save_index_min(package = update_dict[DATABASE_NAME.INDEX_MIN])
    #
    # if DATABASE_NAME.FUTURE_LIST in update_dict.keys():
    #     save_adv.QA_SU_save_future_list(package = update_dict[DATABASE_NAME.FUTURE_LIST])
    #
    # if DATABASE_NAME.FUTURE_DAY in update_dict.keys():
    #     save_adv.QA_SU_save_future_day(package = update_dict[DATABASE_NAME.FUTURE_DAY])
    #
    # if DATABASE_NAME.FUTURE_MIN in update_dict.keys():
    #     save_adv.QA_SU_save_future_min(package = update_dict[DATABASE_NAME.FUTURE_MIN])
    #
    # if DATABASE_NAME.FINANCIAL in update_dict.keys():
    #     save_adv.QA_SU_save_financial_files(package = update_dict[DATABASE_NAME.FINANCIAL])

if __name__ == '__main__':
    QA_Update()

