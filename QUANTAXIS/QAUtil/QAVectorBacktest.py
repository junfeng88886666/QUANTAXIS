# coding:utf-8

# =============================================================================
# DEBUG
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.offline as pltoff
import os
import pandas as pd
import numpy as np
import pyhht
import math
import copy
import warnings
warnings.simplefilter("ignore")
import matplotlib as mpl
#mpl.use('Agg')
#mpl.rcParams['font.sans-serif'] = ['SimHei']
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pylab import *


import seaborn
import datetime
import prettytable as pt
try:
    import reduce
except:
    from functools import reduce
    
# =============================================================================

def QA_VectorBacktest_InterDayOnceTrading_single_fixed_stop(settle_time = '14:57:00',
                                                         fixed_stop_profit_ret = 0.015,
                                                         fixed_stop_loss_ret = -0.005,
                                                         trading_shift = -1,
                                                         comission = 0.00025,
                                                         data = None
                                                        ):
    '''
    data的index为'datetime'[str],含signal列
    '''
    seaborn.set(palette='deep', style='darkgrid')
    def maximum_down(dataframe):
        data = list(dataframe)
        index_j = np.argmax(np.maximum.accumulate(data) - data)  # 结束位置
        index_i = np.argmax(data[:index_j])  # 开始位置
        d = ((data[index_j]) - (data[index_i]))/(data[index_i])  # 最大回撤
        return d,(index_j-index_i)

    data['date'] = list(map(lambda x:str(x[:10]),data.index))
    data['minute'] = list(map(lambda x:str(x[11:]),data.index))

    data['signal'] = np.where(data['minute']==settle_time,0,data['signal'])
    data['signal'] = data['signal'].ffill().fillna(0)
    data['real_return'] = data['close'].pct_change().shift(trading_shift)
    
    data['strategy'] = data['signal']*data['real_return']

    return_table = data.pivot(index='minute', columns='date', values='strategy')
    return_table = (return_table.fillna(0)+1).cumprod(axis=0)
    return_table = return_table.T
    return_table['cum_ret_series'] = list(map(lambda x:list(x),return_table.values))
    return_table = return_table[['cum_ret_series']]
    return_table['series_max'] = list(map(lambda x:max(x),return_table['cum_ret_series']))
    return_table['series_min'] = list(map(lambda x:min(x),return_table['cum_ret_series']))
    return_table['strategy_return_daily'] = list(map(lambda x:x[-1],return_table['cum_ret_series']))
# =============================================================================
#   止盈止损模块（TODO:完善有止盈的情形）
    if (fixed_stop_profit_ret == None)&(fixed_stop_loss_ret != None): 
        return_table['strategy_return_daily'] = np.where(return_table['series_min']<=(fixed_stop_loss_ret+1),(fixed_stop_loss_ret+1),return_table['strategy_return_daily'])
    elif (fixed_stop_profit_ret == None)&(fixed_stop_loss_ret == None):
        pass
# =============================================================================

    return_table['strategy_return_daily'] = np.where(return_table['strategy_return_daily']!=1,return_table['strategy_return_daily']-comission,return_table['strategy_return_daily'])
    return_table['cum_strategy'] = return_table['strategy_return_daily'].cumprod()
# =============================================================================
#     绘图，报告回测结果
    fig = plt.figure(figsize=(20,10))
    figp = fig.subplots(1,1)
    figp.xaxis.set_major_locator(ticker.MultipleLocator(50))
    figp.plot(return_table['cum_strategy'])
    
    return_table['strategy'] = return_table['strategy_return_daily']-1
    annual_rtn  = pow(return_table['cum_strategy'].iloc[-1] / return_table['cum_strategy'].iloc[0], 250/len(return_table) ) -1
    return_table['ex_pct_close'] = return_table['strategy'] - 0.02/252
    
    if return_table[(return_table['strategy']>0)|(return_table['strategy']<0)].shape[0] == 0:
        P1,P2,P3,P4,P5,P6,P7 = 0,0,0,0,0,0,0
    else:
        P1 = round(return_table[return_table['strategy']>0].shape[0]/return_table[(return_table['strategy']>0)|(return_table['strategy']<0)].shape[0]*100,2)
        P2 = round(annual_rtn*100,2)
        P3 = round(maximum_down(return_table[['cum_strategy']].values)[0][0]*100,2)
        P4 = round((return_table['ex_pct_close'].mean() * math.sqrt(252))/return_table['ex_pct_close'].std(),2)
        P5 = round(return_table[return_table['strategy']>0]['strategy'].mean() / abs(return_table[return_table['strategy']<0]['strategy'].mean()),2)
        P6 = round(return_table.shape[0]/return_table[return_table['strategy']!=0].shape[0],2)
    
    print('胜率: '+str(P1)+'%')
    print('年化收益率：'+str(P2)+'%')
    print('最大回撤：'+str(P3)+'%')
    print('夏普比率：'+str(P4))
    print('平均盈亏比：'+str(P5))
    print('交易频率(天)：'+str(P6))
    
    del return_table['cum_ret_series']
# =============================================================================
    return return_table,[P1,P2,P3,P4,P5,P6]


def QA_VectorBacktest(data = None,
                      func = None, 
                      comission = 0.00025, 
                      params = None,
                      params_optimize_dict = None,
                      run_year_list = None, 
                      if_optimize_parameters = False,
                      if_reorder_params = True,
                      save_path = None):
    '''
    data： QA.DataStruct.data
    '''
    print("注意：输入的data格式应为dataframe,MultiIndex:['datetime','code'][datetime,str], columns: ['close',......][float]")
    print("func 的输入格式应和data相同，输出格式应为dataframe,reset_index,columns: ['datetime','code','close','signal'][str,str,float,float],signal仅为[1,0,-1]")
    code_list = list(set(data.reset_index()['code']))
    data['year'] = list(map(lambda x:str(x)[:4],data.reset_index()['datetime']))

    data = data[data['year'].isin(run_year_list)]
    res = pd.DataFrame()
    print('回测年份：{}'.format(run_year_list))
    print('回测品种列表：{}'.format(code_list))
    print('####################################################################################')
    if if_optimize_parameters == False:
        if if_reorder_params: params_use = _edit_params(params,code_list)
        else: params_use = copy.deepcopy(params)
        print('依据给定参数回测，参数：{}'.format(params))
        params_id = 'params_1'
        ####
        calculated_data = pd.DataFrame()
        for code in code_list:
            print('计算信号，品种：{}'.format(code))
            temp_data = data.loc[(slice(None), code), :]
            temp_data = func(temp_data,params_use)
            calculated_data = calculated_data.append(temp_data)
        ###
        res_temp = _QA_VectorBacktest(calculated_data,comission,params,params_id)
        res = res.append(res_temp)
    else:
        print('参数优化开启，时间：{}'.format(str(datetime.datetime.now())))
        optimize_dict = _get_all_optimize_info(params_optimize_dict)
        print('优化参数字典：{}'.format(optimize_dict))
        print('####################################################################################')
        for steps,optimize_item in enumerate(optimize_dict.keys()):
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            print('优化参数：{}，值：{}'.format(optimize_item,optimize_dict[optimize_item]))
            params_id = 'params_'+str(optimize_item)
            for item_temp in optimize_dict[optimize_item].keys():
                params[item_temp] = optimize_dict[optimize_item][item_temp]
            params_use = _edit_params(params,code_list)
            ###
            calculated_data = pd.DataFrame()
            for code in code_list:
                print('计算信号，品种：{}'.format(code))
                temp_data = data.loc[(slice(None), code), :]
                temp_data = func(temp_data,params_use)
                calculated_data = calculated_data.append(temp_data)
                
            ###
            res_temp = _QA_VectorBacktest(calculated_data,comission,params,params_id)
            res = res.append(res_temp)
    simple_res = res[['code','params_id','winrate','annual_return','max_drawback','sharpe','yingkuibi','trading_freq']]
    params_res = res[['params_id','params']]
                
    '''
    结果展示区域
    （结果存储到指定文件夹，图像：plotly,表格：excel）
    0，全品种全参数展示结果
    
    1, 多图，各品种的各参数回测结果的：一个品种一个图，简易回测结果表
    2，多图，各参数的各品种回测结果的：一个参数一个图，简易回测结果表
    
    3，一图，各品种最大年化收益率参数的：净值走势图，简易回测结果表
    4，一图，各品种最大夏普参数的：净值走势图，简易回测结果表
    5，一图，各品种最大胜率参数的：净值走势图，简易回测结果表
    
    6，一图，各参数最大年化收益率品种的：净值走势图，简易回测结果表
    7，一图，各参数最大夏普品种的：净值走势图，简易回测结果表
    8，一图，各参数最大胜率品种的：净值走势图，简易回测结果表
    
    9.1~9.3，全品种最优参数平均分配资金的：净值走势图，简易回测结果表
    
    10，全品种每月初组合优化的：净值走势图，简易回测结果表，暂时不做
    11，全品种每周初组合优化的：净值走势图，简易回测结果表，暂时不做
    12，全品种每日组合优化的：净值走势图，简易回测结果表，暂时不做
    
    若为参数优化模式，则显示所有
    若为非参数优化模式，则显示0,9
    '''
    code_list = list(set(res.code.tolist()))
    params_id_list = list(set(res.params_id.tolist()))
    '''展示0'''
    _draw_based_on_result_dataframe(result = res,save_path = save_path,title = '所有单一回测结果')
    if if_optimize_parameters == False:
        '''展示1'''
        show_results_single(result = res,code_list = code_list,params_id_list = params_id_list,by = 'code',save_path = save_path)
        '''展示2'''
        show_results_single(result = res,code_list = code_list,params_id_list = params_id_list,by = 'params_id',save_path = save_path)
        '''展示3'''
        show_results_max(result = res, on = 'code',by = 'annual_return',save_path = save_path)
        '''展示4'''
        show_results_max(result = res, on = 'code',by = 'sharpe',save_path = save_path)
        '''展示5'''
        show_results_max(result = res, on = 'code',by = 'winrate',save_path = save_path)    
        '''展示6'''
        show_results_max(result = res, on = 'params',by = 'annual_return',save_path = save_path)
        '''展示7'''
        show_results_max(result = res, on = 'params',by = 'sharpe',save_path = save_path)
        '''展示8'''
        show_results_max(result = res, on = 'params',by = 'winrate',save_path = save_path)   
    '''展示9.1.2.3'''
    res_group_average = pd.DataFrame()

    res_all_1,simple_res_all_1,params_res_all_1 = show_results_group_average(result = res,by = 'annual_return',save_path = save_path)
    res_group_average = res_group_average.append(res_all_1)

    res_all_1,simple_res_all_1,params_res_all_1 = show_results_group_average(result = res,by = 'sharpe',save_path = save_path)
    res_group_average = res_group_average.append(res_all_1)

    res_all_1,simple_res_all_1,params_res_all_1 = show_results_group_average(result = res,by = 'winrate',save_path = save_path)
    res_group_average = res_group_average.append(res_all_1)
        
    simple_res_group_average = res_group_average[['code','params_id','winrate','annual_return','max_drawback','sharpe','yingkuibi','trading_freq','by']]
    params_res_group_average = res_group_average[['params_id','params','by']]
    
    if save_path == None: print('不存储数据')
    else: 
        print('存储数据,存储文件夹：{}'.format(save_path))
        res.to_csv(os.path.join(save_path,'全部回测结果.csv'))
        simple_res.to_csv(os.path.join(save_path,'重要回测结果参数.csv'))
        params_res.to_csv(os.path.join(save_path,'回测参数表.csv'))
        res_group_average.to_csv(os.path.join(save_path,'全市场最优参数平均资金分配_回测结果.csv'))
        simple_res_group_average.to_csv(os.path.join(save_path,'全市场最优参数平均资金分配_重要回测结果参数.csv'))
        params_res_group_average.to_csv(os.path.join(save_path,'全市场最优参数平均资金分配_回测参数表.csv'))
        
    return res,simple_res,params_res,res_group_average,simple_res_group_average,params_res_group_average

# =============================================================================
# 结果展示函数区域
def show_results_single(result = None,code_list = None,params_id_list = None,by = 'code',save_path = None):
    print('####################################################################################')
    print('各品种各参数结果展示')
    all_code_list = list(set(result.code.tolist()))
    all_params_id_list = list(set(result.params_id.tolist()))
    
    if code_list == None: code_list = copy.deepcopy(all_code_list)
    if params_id_list == None: params_id_list = copy.deepcopy(all_params_id_list)

    if by == 'code': 
        for code in code_list:
            print('####################################################################################')
            if len(params_id_list) == len(all_params_id_list): print('展示：单品种的全参数回测结果，品种：{}'.format(code))
            
            temp_result = result[(result.code==code)&(result.params_id.isin(params_id_list))]
            _draw_based_on_result_dataframe(result = temp_result,save_path = save_path,title = '品种：{}，多参数的矢量回测结果'.format(code))
    elif by == 'params_id':
        for params_id in params_id_list:
            print('####################################################################################')
            if len(code_list) == len(all_code_list): print('展示：单参数的全品种回测结果，参数：{}'.format(params_id))

            temp_result = result[(result.params_id==params_id)&(result.code.isin(code_list))]
            _draw_based_on_result_dataframe(result = temp_result,save_path = save_path,title = '参数：{}，多品种的矢量回测结果'.format(params_id))
            
def show_results_max(result = None, on = 'code',by = 'sharpe',save_path = None):
    print('####################################################################################')
    if on == 'code': 
        temp_title = '各品种最优参数结果展示, 最优衡量标准为：{}最优'.format(by)
        print(temp_title)
        result_max = result.groupby('code',as_index = False).apply(lambda t: t[t[by]==t[by].max()])
        
    elif on == 'params': 
        temp_title = '各参数最优品种结果展示, 最优衡量标准为：{}最优'.format(by)
        print(temp_title)
        result_max = result.groupby('params_id',as_index = False).apply(lambda t: t[t[by]==t[by].max()])

    _draw_based_on_result_dataframe(result = result_max,save_path = save_path,title = temp_title)

            
def show_results_group_average(result = None,by = 'sharpe',save_path = None):
    import copy
    print('####################################################################################')
    temp_title = '全品种最优参数平均分配资金, 最优衡量标准为：{}最优'.format(by)
    print(temp_title)
    
# =============================================================================
#   获取平均分配资金的日收益序列
    result_max = result.groupby('code',as_index = False).apply(lambda t: t[t[by]==t[by].max()])
    
    for item in range(len(result_max)):
        temp_result_max = result_max.iloc[item]
        temp_df = pd.DataFrame(index = temp_result_max['trading_date_series'],data = temp_result_max['ret_series']).rename(columns = {0:'series_'+str(item)})
        if item == 0: temp_df_all = copy.deepcopy(temp_df)
        else:
            temp_df_all = pd.merge(temp_df_all,temp_df,left_index = True,right_index = True,how = 'outer')
    temp_df_all = temp_df_all.fillna(1)
    temp_df_all = temp_df_all.sum(axis=1)/temp_df_all.shape[1]   
    temp_df_all = pd.DataFrame(temp_df_all)
    temp_df_all.columns = ['strategy_return_daily']
    
# =============================================================================

    '''画图画表'''
    fig = plt.figure(figsize=(20,10))
    figp = fig.subplots(1,1)
    '''绘制曲线'''
    figp.plot(temp_df_all.cumprod(),label = str(result_max.code.tolist())+' && '+str(result_max.params_id.tolist()))    
    '''设置显示属性和标题'''
    figp.xaxis.set_major_locator(ticker.MultipleLocator(80))
    figp.legend()
    figp.set_title(temp_title)
    fig.show()
    if save_path == None: pass
    else: fig.savefig(os.path.join(save_path,temp_title+'.jpg'))
    
# =============================================================================
#   生成统计表格
    result_max['params'].tolist()
    group_params = dict(
                            zip(
                                result_max['code'].tolist(),
                                result_max['params'].tolist()
                            )
                        )
                            
    res_all_1 = _get_result(return_table = temp_df_all, code = result_max.code.tolist(), params_id = result_max.params_id.tolist(), params = group_params)
    res_all_1['by'] = by
    simple_res_all_1 = res_all_1[['code','params_id','winrate','annual_return','max_drawback','sharpe','yingkuibi','trading_freq','by']]
    params_res_all_1 = res_all_1[['params_id','params','by']]
    
    temp_simple_result = _edit_result_to_print_format(result = result)
    _print_table(temp_simple_result)
# =============================================================================
    return res_all_1,simple_res_all_1,params_res_all_1

def _draw_based_on_result_dataframe(result = None,save_path = None,title = None):
    mpl.rcParams['font.sans-serif'] = ['SimHei'] #设置matplotlib的中文字体显示
    
# =============================================================================
#   图像显示部分
    '''设置画布'''
    fig = plt.figure(figsize=(20,10))
    figp = fig.subplots(1,1)
    '''绘制曲线'''
    for item in range(len(result)):
        '''分参数绘制曲线'''
        temp_result = result.iloc[item]
        temp_draw_series = pd.Series(index = temp_result['trading_date_series'],data = temp_result['ret_series'])
        temp_draw_series = temp_draw_series.cumprod()
        figp.plot(temp_draw_series,label = temp_result.code+' && '+temp_result.params_id)    
    '''设置显示属性和标题'''
    figp.xaxis.set_major_locator(ticker.MultipleLocator(80))
    figp.legend()
    figp.set_title(title)
    fig.show()
    if save_path == None: pass
    else: fig.savefig(os.path.join(save_path,title+'.jpg'))
# =============================================================================
    
# =============================================================================
#   表格显示部分
    temp_simple_result = _edit_result_to_print_format(result = result)
    _print_table(temp_simple_result)
# =============================================================================   
    
    
def _edit_result_to_print_format(result = None):
    temp_simple_result = result[['code','params_id','winrate','annual_return','max_drawback','sharpe','yingkuibi','trading_freq']]
    '''设置临时表格格式（胜率，年化收益率，最大回撤：调整为百分比格式）'''
    temp_simple_result['winrate'] = list(map(lambda x:str(round(x*100,2))+'%',temp_simple_result['winrate']))
    temp_simple_result['annual_return'] = list(map(lambda x:str(round(x*100,2))+'%',temp_simple_result['annual_return']))
    temp_simple_result['max_drawback'] = list(map(lambda x:str(round(x*100,2))+'%',temp_simple_result['max_drawback']))
    return temp_simple_result
    
    
def _print_table(table = None):
    tb = pt.PrettyTable()
    temp_columns_list = table.columns.tolist()
    for temp_column in temp_columns_list:
        tb.add_column(temp_column,table[temp_column].tolist())
    print(tb)
    
      
def _edit_params(params,code_list):
    import copy
    params_record = copy.deepcopy(params)
    params = {}
    for code in code_list:
        params[code] = params_record
    return params
        
def _QA_VectorBacktest(df = None,comission = None, params = None,params_id = None):
    '''
    将日内收益转化成每日来计量的矢量式回测
    输入：multiindex:['datetime','code']
    return: dataframe for the record of each code, columns = ['code','params_id','params','winrate','annual_return','max_drawback','sharpe','yingkuibi','trading_freq','cum_ret_series']
    '''
    seaborn.set(palette='deep', style='darkgrid')
    code_list = list(set(df.reset_index()['code']))
    df = df.sort_values(by=['datetime','code'])
    if 'date' not in df.columns: df['date'] = list(map(lambda x:str(x)[:10],df['datetime']))
    if 'minute' not in df.columns: df['minute'] = list(map(lambda x:str(x)[11:],df['datetime']))
    
#    def maximum_down(dataframe):
#        data = list(dataframe)
#        index_j = np.argmax(np.maximum.accumulate(data) - data)  # 结束位置
#        index_i = np.argmax(data[:index_j])  # 开始位置
#        d = ((data[index_j]) - (data[index_i]))/(data[index_i])  # 最大回撤
#        return d,(index_j-index_i)
    
    result = pd.DataFrame()
    for code in code_list:
        print('回测参数ID：{}, 回测代码：{}'.format(params_id,code))
        data = df[df['code']==code]

        data['real_return'] = data['close'].pct_change().shift(-1)
    
        data['strategy'] = data['signal']*data['real_return']
    
        return_table = data.pivot(index='minute', columns='date', values='strategy')
        return_table = (return_table.fillna(0)+1).cumprod(axis=0)
        return_table = return_table.T
        return_table['cum_ret_series'] = list(map(lambda x:list(x),return_table.values))
        return_table = return_table[['cum_ret_series']]
        return_table['strategy_return_daily'] = list(map(lambda x:x[-1],return_table['cum_ret_series']))

        return_table['strategy_return_daily'] = np.where(return_table['strategy_return_daily']!=1,return_table['strategy_return_daily']-comission,return_table['strategy_return_daily'])
        del return_table['cum_ret_series']
# =============================================================================
#   存储回测结果
#        return_table['cum_strategy'] = return_table['strategy_return_daily'].cumprod()    
#        return_table['strategy'] = return_table['strategy_return_daily']-1
#        
#        annual_rtn  = pow(return_table['cum_strategy'].iloc[-1] / return_table['cum_strategy'].iloc[0], 250/len(return_table) ) -1
#        return_table['ex_pct_close'] = return_table['strategy'] - 0.02/252
#        P1,P2,P3,P4,P5,P6 = 0,0,0,0,0,0
#        try:
#            P1 = round(return_table[return_table['strategy']>0].shape[0]/return_table[(return_table['strategy']>0)|(return_table['strategy']<0)].shape[0]*100,2)
#            P2 = round(annual_rtn*100,2)
#            P3 = round(maximum_down(return_table[['cum_strategy']].values)[0][0]*100,2)
#            P4 = round((return_table['ex_pct_close'].mean() * math.sqrt(252))/return_table['ex_pct_close'].std(),2)
#            P5 = round(return_table[return_table['strategy']>0]['strategy'].mean() / abs(return_table[return_table['strategy']<0]['strategy'].mean()),2)
#            P6 = round(return_table.shape[0]/return_table[return_table['strategy']!=0].shape[0],2)
#        
##            print('胜率: '+str(P1)+'%')
##            print('年化收益率：'+str(P2)+'%')
##            print('最大回撤：'+str(P3)+'%')
##            print('夏普比率：'+str(P4))
##            print('平均盈亏比：'+str(P5))
##            print('交易频率(天)：'+str(P6))
#        except: pass
#        
#        result_temp = {}
#        result_temp['code'] = code
#        result_temp['params_id'] = params_id
#        result_temp['params'] = params
#        result_temp['winrate'] = P1/100
#        result_temp['annual_return'] = P2/100
#        result_temp['max_drawback'] = P3/100
#        result_temp['sharpe'] = P4
#        result_temp['yingkuibi'] = P5
#        result_temp['trading_freq'] = P6
#        result_temp['trading_date_series'] = return_table.index.tolist()
#        result_temp['ret_series'] = return_table['strategy_return_daily'].tolist()
#        result_temp = pd.DataFrame(pd.Series(result_temp)).T
        result_temp = _get_result(return_table = return_table, code = code, params_id = params_id, params = params)
        result = result.append(result_temp)
    return result

def _get_result(return_table = None, code = None, params_id = None, params = None):
    '''
    输入的return_table要求：dataframe, index:date, 列包含：strategy_return_daily不减1但减去了手续费的日收益
    '''    
    def maximum_down(dataframe):
        data = list(dataframe)
        index_j = np.argmax(np.maximum.accumulate(data) - data)  # 结束位置
        index_i = np.argmax(data[:index_j])  # 开始位置
        d = ((data[index_j]) - (data[index_i]))/(data[index_i])  # 最大回撤
        return d,(index_j-index_i)
    return_table['cum_strategy'] = return_table['strategy_return_daily'].cumprod()    
    return_table['strategy'] = return_table['strategy_return_daily']-1
    annual_rtn  = pow(return_table['cum_strategy'].iloc[-1] / return_table['cum_strategy'].iloc[0], 250/len(return_table) ) -1
    return_table['ex_pct_close'] = return_table['strategy'] - 0.02/252
    P1,P2,P3,P4,P5,P6 = 0,0,0,0,0,0
    try:
        P1 = round(return_table[return_table['strategy']>0].shape[0]/return_table[(return_table['strategy']>0)|(return_table['strategy']<0)].shape[0]*100,2)
        P2 = round(annual_rtn*100,2)
        P3 = round(maximum_down(return_table[['cum_strategy']].values)[0][0]*100,2)
        P4 = round((return_table['ex_pct_close'].mean() * math.sqrt(252))/return_table['ex_pct_close'].std(),2)
        P5 = round(return_table[return_table['strategy']>0]['strategy'].mean() / abs(return_table[return_table['strategy']<0]['strategy'].mean()),2)
        P6 = round(return_table.shape[0]/return_table[return_table['strategy']!=0].shape[0],2)
    
#            print('胜率: '+str(P1)+'%')
#            print('年化收益率：'+str(P2)+'%')
#            print('最大回撤：'+str(P3)+'%')
#            print('夏普比率：'+str(P4))
#            print('平均盈亏比：'+str(P5))
#            print('交易频率(天)：'+str(P6))
    except: pass
    result_temp = {}
    result_temp['code'] = code
    result_temp['params_id'] = params_id
    result_temp['params'] = params
    result_temp['winrate'] = P1/100
    result_temp['annual_return'] = P2/100
    result_temp['max_drawback'] = P3/100
    result_temp['sharpe'] = P4
    result_temp['yingkuibi'] = P5
    result_temp['trading_freq'] = P6
    result_temp['trading_date_series'] = return_table.index.tolist()
    result_temp['ret_series'] = return_table['strategy_return_daily'].tolist()
    result_temp = pd.DataFrame(pd.Series(result_temp)).T
    return result_temp    
    


def _get_all_optimize_info(params_optimize_dict):
    '''
    params_optimize_dict = {'fac1':['1','2','3'],
                            'fac2':['1','2','3'],
                            'fac3':['1','2','3']}
    
    '''
    fn = lambda x, code=',': reduce(lambda x, y: [str(i)+code+str(j) for i in x for j in y], x)
    lists = []
    params_name = []
    for i in params_optimize_dict.keys():
        lists.append(params_optimize_dict[i])
        params_name.append(i)
        
    combination = fn(lists,',')
    count = 0
    optimize_dict = {}
    for item in combination:
        count+=1
        splited_item = item.split(',')
        temp = {}
        for i,sub_item in enumerate(params_name):
            temp[sub_item] = splited_item[i]
        optimize_dict[count] = temp     
    return optimize_dict
    
def QA_VectorBacktest_func_add_fixed_stop(data = None,stop_loss_ret = None,stop_profit_ret = None):
    data['enter_long_price_mark'] = np.where(data['signal']==1,data['close'],np.nan)
    data['enter_short_price_mark'] = np.where(data['signal']==-1,data['close'],np.nan)
    data['enter_long_price_mark'] = data['enter_long_price_mark'].ffill()
    data['enter_short_price_mark'] = data['enter_short_price_mark'].ffill()
    data['current_signal'] = data['signal'].ffill()
    data['current_ret'] = np.where(data['current_signal']==1,(data['close']/data['enter_long_price_mark'])-1,np.nan)
    data['current_ret'] = np.where(data['current_signal']==-1,(data['enter_short_price_mark']/data['close'])-1,data['current_ret'])
    data['signal'] = np.where(data['current_ret']<=stop_loss_ret,0,data['signal'])
    data['signal'] = np.where(data['current_ret']>=stop_profit_ret,0,data['signal'])
    return data

def QA_VectorBacktest_func_fill_signal(data = None):
    data['signal'] = data['signal'].ffill().fillna(0)
    data['signal'] = np.where((data['signal']!=0)&(data['signal'].shift(-1)==0),0,data['signal'])
    return data
    









    
#def _draw_single_code_multi_params(result = None, code = None,params_id_list = None,save_path = None):
#    '''
#    调试参数
#    code = 'IC'
#    params_id_list = ['params_1','params_2','params_3']
#    '''
#    '''获取临时表格'''
#    print('####################################################################################')
#    all_params_id_list = list(set(result.params_id.tolist()))
#    if len(params_id_list) == len(all_params_id_list): print('展示：单品种的全参数回测结果, 品种：{}'.format(code))
#    else: print('展示：单品种的部分参数回测结果, 品种：{}'.format(code))
#    mpl.rcParams['font.sans-serif'] = ['SimHei']
#    temp_result = result[(result.code==code)&(result.params_id.isin(params_id_list))]
#    temp_simple_result = temp_result[['code','params_id','winrate','annual_return','max_drawback','sharpe','yingkuibi','trading_freq']]
#    '''设置临时表格格式（胜率，年化收益率，最大回撤：调整为百分比格式）'''
#    temp_simple_result['winrate'] = list(map(lambda x:str(round(x*100,2))+'%',temp_simple_result['winrate']))
#    temp_simple_result['annual_return'] = list(map(lambda x:str(round(x*100,2))+'%',temp_simple_result['annual_return']))
#    temp_simple_result['max_drawback'] = list(map(lambda x:str(round(x*100,2))+'%',temp_simple_result['max_drawback']))
#
## =============================================================================
##   图像显示部分
#    '''设置画布'''
#    fig = plt.figure(figsize=(20,10))
#    figp = fig.subplots(1,1)
#    '''绘制曲线'''
#    for params_id in params_id_list:
#        '''分参数绘制曲线'''
#        single_result = temp_result[(temp_result.code==code)&(temp_result.params_id == params_id)]
#        temp_draw_series = pd.Series(index = single_result['trading_date_series'].values[0],data = single_result['cum_ret_series'].values[0])
#        figp.plot(temp_draw_series,label = params_id)    
#    '''设置显示属性和标题'''
#    figp.xaxis.set_major_locator(ticker.MultipleLocator(80))
#    figp.legend()
#    figp.set_title('品种：{}，多参数的矢量回测结果'.format(code))
#    fig.show()
#    if save_path == None: pass
#    else: fig.savefig(os.path.join(save_path,'品种：{}，多参数的矢量回测结果.jpg'.format(code)))
## =============================================================================
## =============================================================================
##   表格显示部分
#    tb = pt.PrettyTable()
#    temp_columns_list = temp_simple_result.columns.tolist()
#    for temp_column in temp_columns_list:
#        tb.add_column(temp_column,temp_simple_result[temp_column].tolist())
#    print(tb)
## =============================================================================
#    
#def _draw_single_params_multi_code(result = None, params_id = None,code_list = None,save_path = None):
#    '''获取临时表格'''
#    print('####################################################################################')
#    all_code_list = list(set(result.code.tolist()))
#    if len(code_list) == len(all_code_list): print('展示：单参数的全品种回测结果，参数：{}'.format(params_id))
#    else: print('展示：单参数的部分品种回测结果, 参数：{}'.format(params_id))
#
#    mpl.rcParams['font.sans-serif'] = ['SimHei']
#    temp_result = result[(result.params_id==params_id)&(result.code.isin(code_list))]
#    temp_simple_result = temp_result[['code','params_id','winrate','annual_return','max_drawback','sharpe','yingkuibi','trading_freq']]
#    '''设置临时表格格式（胜率，年化收益率，最大回撤：调整为百分比格式）'''
#    temp_simple_result['winrate'] = list(map(lambda x:str(round(x*100,2))+'%',temp_simple_result['winrate']))
#    temp_simple_result['annual_return'] = list(map(lambda x:str(round(x*100,2))+'%',temp_simple_result['annual_return']))
#    temp_simple_result['max_drawback'] = list(map(lambda x:str(round(x*100,2))+'%',temp_simple_result['max_drawback']))
#
## =============================================================================
##   图像显示部分
#    '''设置画布'''
#    fig = plt.figure(figsize=(20,10))
#    figp = fig.subplots(1,1)
#    '''绘制曲线'''
#    for code in code_list:
#        '''分参数绘制曲线'''
#        single_result = temp_result[(temp_result.code==code)&(temp_result.params_id == params_id)]
#        temp_draw_series = pd.Series(index = single_result['trading_date_series'].values[0],data = single_result['cum_ret_series'].values[0])
#        figp.plot(temp_draw_series,label = code)    
#    '''设置显示属性和标题'''
#    figp.xaxis.set_major_locator(ticker.MultipleLocator(80))
#    figp.legend()
#    figp.set_title('参数：{}，多品种的矢量回测结果'.format(params_id))
#    fig.show()
#    if save_path == None: pass
#    else: fig.savefig(os.path.join(save_path,'参数：{}，多品种的矢量回测结果.jpg'.format(params_id)))
## =============================================================================
## =============================================================================
##   表格显示部分
#    tb = pt.PrettyTable()
#    temp_columns_list = temp_simple_result.columns.tolist()
#    for temp_column in temp_columns_list:
#        tb.add_column(temp_column,temp_simple_result[temp_column].tolist())
#    print(tb)