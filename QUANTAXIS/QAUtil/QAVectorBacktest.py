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

import shutil
import seaborn
import datetime
import prettytable as pt
try:
    import reduce
except:
    from functools import reduce


from QUANTAXIS.QAUtil.QARandom import QA_util_random_with_topic
    
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



def QA_VectorBacktest_adv(backtest_id = None,
                          code_list = None,
                          in_sample_year_list = None,
                          out_sample_year_list = None,
                          in_sample_timeperiod = None,
                          out_sample_timeperiod = None,
                          all_yeal_list = None,
                          all_timeperiod = None,
                          data_engine = None,
                          func = None,
                          comission = 0.00025,
                          params=None,
                          params_optimize_dict=None,
                          if_optimize_parameters=False,
                          root_save_path=None,
                          if_reload_save_files=True,
                          if_legend=True
                          ):
    backtest_id = QA_util_random_with_topic(topic='backtest', lens=8) if backtest_id == None else backtest_id
    print('######################################################################################')
    print('此次回测的回测id:{}'.format(backtest_id))
    print('此次回测的回测根目录:{}'.format(root_save_path))

    if_in_sample = False
    if_out_sample = False
    if_all_ample = False

    backtest_list = []
    if (in_sample_year_list!=None)|(in_sample_timeperiod != None):
        if_in_sample = True
        backtest_list.append('样本内')
    if (out_sample_year_list!=None)|(out_sample_timeperiod != None):
        if_out_sample = True
        backtest_list.append('样本外')
    if (all_yeal_list!=None)|(all_timeperiod != None):
        if_all_sample = True
        backtest_list.append('全样本')
    print('此次回测的回测列表：{}'.format(backtest_list))

    if if_in_sample:
        in_sample_path = os.path.join(root_save_path,backtest_id,'in_sample')
        print('此次回测的样本内回测目录:{}'.format(in_sample_path))
    if if_out_sample:
        out_sample_path = os.path.join(root_save_path, backtest_id, 'out_sample')
        print('此次回测的样本外回测目录:{}'.format(out_sample_path))
    if if_all_sample:
        all_sample_path = os.path.join(root_save_path, backtest_id, 'all_sample')
        print('此次回测的全样本回测目录:{}'.format(all_sample_path))

    '''样本内回测'''
    if if_in_sample:
        if in_sample_timeperiod==None:
            print('开始样本内测试，样本内测试年份：'.format(in_sample_year_list))
            data_start = min(in_sample_year_list)+'-01-01 00:00:00'
            data_end = max(in_sample_year_list)+'-12-31 24:00:00'
        elif  in_sample_year_list==None:
            print('开始样本内测试，样本内测试开始时间：{}，样本内测试结束时间：{}'.format(in_sample_timeperiod[0],in_sample_timeperiod[1]))
            data_start = in_sample_timeperiod[0]
            data_end = in_sample_timeperiod[1]
            run_year_list = None

    result,simple_result,params_res,res_group_average,simple_res_group_average,params_res_group_average = QA_VectorBacktest(data = data_engine(code_list,data_start,data_end),
                                                                                                                          func = func,
                                                                                                                          comission = comission,
                                                                                                                          params = params,
                                                                                                                          params_optimize_dict = params_optimize_dict,
                                                                                                                          run_year_list = run_year_list,
                                                                                                                          if_optimize_parameters = True,
                                                                                                                          if_reorder_params = True,
                                                                                                                          save_path = in_sample_path,
                                                                                                                          if_reload_save_files = if_reload_save_files)










def QA_VectorBacktest(data = None,
                      func = None, 
                      comission = 0.00025, 
                      params = None,
                      params_optimize_dict = None,
                      run_year_list = None, 
                      if_optimize_parameters = False,
                      if_reorder_params = True,
                      save_path = None,
                      if_reload_save_files = True,
                      if_legend = True,
                      code_list = None):
    '''
    data： QA.DataStruct.data
    '''
    import copy
    s = datetime.datetime.now()
    '''重置回测文件夹下的子文件夹'''
    if if_reload_save_files: 
        try: shutil.rmtree(save_path)
        except: pass
    if not os.path.exists(save_path):os.makedirs(save_path)
    ''''''
    print('矢量回测开始，开始时间：{}'.format(str(s)))
    print("注意：输入的data格式应为dataframe,MultiIndex:['datetime','code'][datetime,str], columns: ['open','close',......][float]")
    print("func 的输入格式应和data相同，输出格式应为dataframe(切记是datetime而不是tradetime),reset_index,columns: ['datetime','code','open','close','signal'][str,str,float,float],signal为[float]")
    if code_list!= None: data = data.loc[(slice(None), code_list), :]

    code_list = list(set(data.reset_index()['code']))
    if run_year_list != None:
        if 'year' not in data.columns: data['year'] = list(map(lambda x:str(x)[:4],data.reset_index()['datetime']))
        data = data[data['year'].isin(run_year_list)]
        print('回测年份：{}'.format(run_year_list))
    data = data.sort_index()
    res = pd.DataFrame()

    print('回测品种列表：{}'.format(code_list))
    print('####################################################################################')
    if if_optimize_parameters == False:
        params_temp = copy.deepcopy(params)
        if if_reorder_params: params_use = _edit_params(params_temp,code_list)
        else: params_use = copy.deepcopy(params_temp)
        print('不启用参数优化，依据给定参数回测，参数：{}'.format(params_temp))
        params_id = 'params_default'
        ####
        # calculated_data = pd.DataFrame()
        # for code in code_list:
        #     print('计算信号，品种：{}'.format(code))
        #     temp_data = data.loc[(slice(None), code), :]
        #     temp_data = func(temp_data,params_use)
        #     print(temp_data)
        #     calculated_data = calculated_data.append(temp_data)
        ###
        calculated_data = data.groupby(level=1, sort=False).apply(func,params_use).reset_index(drop = True)
        calculated_data = calculated_data.sort_values(by=['datetime', 'code'])
        res_temp = _QA_VectorBacktest(calculated_data,comission,params_temp,params_id,save_path)
        calculated_data.to_csv(os.path.join(save_path,'calculated_data_'+params_id+'.csv'))
        res = res.append(res_temp)
    else:
        print('参数优化开启，时间：{}'.format(str(datetime.datetime.now())))
        optimize_dict = _get_all_optimize_info(params_optimize_dict)
        print('优化参数字典：{}'.format(optimize_dict))
        print('####################################################################################')
        for steps,optimize_item in enumerate(optimize_dict.keys()):
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            print('优化参数：{}，值：{}'.format(optimize_item,optimize_dict[optimize_item]))
            temp_s = datetime.datetime.now()
            params_id = 'params_'+str(optimize_item)
            params_temp = copy.deepcopy(params)
            for item_temp in optimize_dict[optimize_item].keys():
                params_temp[item_temp] = optimize_dict[optimize_item][item_temp]
            params_use = _edit_params(params_temp,code_list)
            ###
            # calculated_data = pd.DataFrame()
            # for code in code_list:
            #     print('计算信号，品种：{}'.format(code))
            #     temp_data = data.loc[(slice(None), code), :]
            #     temp_data = func(temp_data,params_use)
            #     calculated_data = calculated_data.append(temp_data)
            ###
            calculated_data = data.groupby(level=1, sort=False).apply(func,params_use).reset_index(drop = True)
            calculated_data = calculated_data.sort_values(by=['datetime','code'])
            res_temp = _QA_VectorBacktest(calculated_data,comission,params_temp,params_id,save_path)
            calculated_data.to_csv(os.path.join(save_path,'calculated_data_'+params_id+'.csv'))
            res = res.append(res_temp)
            temp_e = datetime.datetime.now()
            print('此轮耗时：{}'.format(temp_e - temp_s))
            print('剩余时间: {}'.format((len(optimize_dict.keys())-steps) * (temp_e - temp_s)))

    simple_res = res[['code','params_id','winrate','annual_return','max_drawback','sharpe','yingkuibi','trading_freq']]
    params_res = res[['code','params_id','params']].drop_duplicates(subset = ['code','params_id'])
                
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
    _draw_based_on_result_dataframe(result = res,save_path = save_path,titles = '所有单一回测结果',if_legend=if_legend)
    if if_optimize_parameters == True:
        '''展示1'''
        show_results_single(result = res,code_list = code_list,params_id_list = params_id_list,by = 'code',save_path = save_path,if_legend=if_legend)
        '''展示2'''
        show_results_single(result = res,code_list = code_list,params_id_list = params_id_list,by = 'params_id',save_path = save_path,if_legend=if_legend)
        '''展示3'''
        show_results_max(result = res, on = 'code',by = 'annual_return',save_path = save_path,if_legend=if_legend)
        '''展示4'''
        show_results_max(result = res, on = 'code',by = 'sharpe',save_path = save_path,if_legend=if_legend)
        '''展示5'''
        show_results_max(result = res, on = 'code',by = 'winrate',save_path = save_path,if_legend=if_legend)
        '''展示6'''
        show_results_max(result = res, on = 'params',by = 'annual_return',save_path = save_path,if_legend=if_legend)
        '''展示7'''
        show_results_max(result = res, on = 'params',by = 'sharpe',save_path = save_path,if_legend=if_legend)
        '''展示8'''
        show_results_max(result = res, on = 'params',by = 'winrate',save_path = save_path,if_legend=if_legend)
    '''展示9.1.2.3'''
    res_group_average = pd.DataFrame()

    res_all_1,simple_res_all_1,params_res_all_1 = show_results_group_average(result = res,by = 'annual_return',save_path = save_path,if_legend=if_legend)
    res_group_average = res_group_average.append(res_all_1)

    res_all_1,simple_res_all_1,params_res_all_1 = show_results_group_average(result = res,by = 'sharpe',save_path = save_path,if_legend=if_legend)
    res_group_average = res_group_average.append(res_all_1)

    res_all_1,simple_res_all_1,params_res_all_1 = show_results_group_average(result = res,by = 'winrate',save_path = save_path,if_legend=if_legend)
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
        
    e = datetime.datetime.now()
    print('矢量回测结束，结束时间：{}'.format(str(e)))
    try: print('矢量回测共耗时：{}，回测品种数：{}，回测参数数：{}'.format(str(e-s),len(code_list),len(optimize_dict)))
    except: print('矢量回测共耗时：{}，回测品种数：{}，回测参数数：{}'.format(str(e-s),len(code_list),1))
    return res,simple_res,params_res,res_group_average,simple_res_group_average,params_res_group_average

# =============================================================================
# 结果展示函数区域
def show_results_single(result = None,code_list = None,params_id_list = None,by = 'code',save_path = None, if_legend=True):
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
            _draw_based_on_result_dataframe(result = temp_result,save_path = save_path,titles = '品种：{}，多参数的矢量回测结果'.format(code),if_legend = if_legend)
    elif by == 'params_id':
        for params_id in params_id_list:
            print('####################################################################################')
            if len(code_list) == len(all_code_list): print('展示：单参数的全品种回测结果，参数：{}'.format(params_id))

            temp_result = result[(result.params_id==params_id)&(result.code.isin(code_list))]
            _draw_based_on_result_dataframe(result = temp_result,save_path = save_path,titles = '参数：{}，多品种的矢量回测结果'.format(params_id),if_legend = if_legend)
            
def show_results_max(result = None, on = 'code',by = 'sharpe',save_path = None,if_legend=True):
    print('####################################################################################')
    if on == 'code': 
        temp_titles = '各品种最优参数结果展示, 最优衡量标准为：{}最优'.format(by)
        print(temp_titles)
        result_max = result.groupby('code',as_index = False).apply(lambda t: t[t[by]==t[by].max()])

    elif on == 'params': 
        temp_titles = '各参数最优品种结果展示, 最优衡量标准为：{}最优'.format(by)
        print(temp_titles)
        result_max = result.groupby('params_id',as_index = False).apply(lambda t: t[t[by]==t[by].max()])

    _draw_based_on_result_dataframe(result = result_max,save_path = save_path,titles = temp_titles,if_legend = if_legend)

            
def show_results_group_average(result = None,by = 'sharpe',save_path = None,if_legend=True):
    import copy
    print('####################################################################################')
    
# =============================================================================
#   '''设置筛选顺序，按by给定的为最优，若有相同的，则按，sharpe>annual_return>winrate来获取最优'''
    if by == 'sharpe': filter_list = ['sharpe','annual_return','winrate']
    elif by == 'annual_return': filter_list = ['annual_return','sharpe','winrate']
    elif by == 'winrate': filter_list = ['winrate','sharpe','annual_return']

# =============================================================================
    temp_titles = '全品种最优参数平均分配资金, 最优衡量标准为：{}最优'.format(by)
    print(temp_titles)    
# =============================================================================
#   获取平均分配资金的日收益序列
    for j,i in enumerate(filter_list):
        if j ==0: result_max = result.groupby('code',as_index = False).apply(lambda t: t[t[i]==t[i].max()])
        else: result_max = result_max.groupby('code',as_index = False).apply(lambda t: t[t[i]==t[i].max()])
    
    for item in range(len(result_max)):
        temp_result_max = result_max.iloc[item]
        temp_df = pd.DataFrame(index = temp_result_max['trading_date_series'],data = temp_result_max['ret_series']).rename(columns = {0:'series_'+str(item)})
        if item == 0: temp_df_all = copy.deepcopy(temp_df)
        else:
            temp_df_all = pd.merge(temp_df_all,temp_df,left_index = True,right_index = True,how = 'outer')
    temp_df_all = temp_df_all.sort_index()
    temp_df_all = temp_df_all.fillna(1)
    temp_df_all = temp_df_all.sum(axis=1)/temp_df_all.shape[1]
    temp_df_all = pd.DataFrame(temp_df_all)
    temp_df_all.columns = ['strategy_return_daily']
    
# =============================================================================
    '''画图画表'''
# =============================================================================
#   生成统计表格
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
    
    temp_simple_result = _edit_result_to_print_format(result = res_all_1)
    _print_table(temp_simple_result)
    
    plot_series = temp_df_all['strategy_return_daily'].cumprod()
    texts = []
    texts.append(plot_series.index.min())
    texts.append(plot_series.min()+(plot_series.max()-plot_series.min())*0.8)
    texts.append('\n 年化收益率: {}  \n 胜率: {} \n 最大回撤:{} \n 夏普比率: {} \
                 \n 盈亏比: {} \n 交易频率: {}'.format(temp_simple_result['annual_return'].values[0],
                                                     temp_simple_result['winrate'].values[0],
                                                     temp_simple_result['max_drawback'].values[0],
                                                     temp_simple_result['sharpe'].values[0],
                                                     temp_simple_result['yingkuibi'].values[0],
                                                     temp_simple_result['trading_freq'].values[0],
                                                     ))    
    
    __matplotlib_plot(data = plot_series,
                      texts = texts,
                      titles = temp_titles,
                      label = temp_titles,
                      if_legend = False,
                      save_path = save_path)
# =============================================================================
    return res_all_1,simple_res_all_1,params_res_all_1

def _draw_based_on_result_dataframe(result = None,save_path = None,titles = None,if_legend = True):
    '''
    TODO 修复绘图的时候时间错乱的问题
    result = res
    titles = 'ABC'
    '''
    mpl.rcParams['font.sans-serif'] = ['SimHei'] #设置matplotlib的中文字体显示
    
# =============================================================================
#   图像显示部分
    '''设置画布'''

    '''绘制曲线'''
    __save_all = pd.DataFrame()
    for item in range(len(result)):
        '''分参数绘制曲线'''
        temp_result = result.iloc[item]
        temp_draw_series = pd.Series(index = temp_result['trading_date_series'],data = temp_result['ret_series'])
        temp_draw_series = pd.DataFrame(temp_draw_series)
        temp_draw_series.columns=[temp_result.code+' && '+temp_result.params_id]
        __save_all = pd.merge(__save_all,temp_draw_series,left_index=True,right_index=True,how='outer')

    '''设置显示属性和标题'''
    __save_all = __save_all.fillna(1)
    __save_all = __save_all.sort_index()
    __save_all = __save_all.cumprod()
    __matplotlib_plot(data = __save_all,
                      titles = titles,
                      label = __save_all.columns.tolist(),
                      if_legend = if_legend,
                      save_path = save_path)
# =============================================================================
# =============================================================================
#   表格显示部分
    temp_simple_result = _edit_result_to_print_format(result = result)
    _print_table(temp_simple_result)
# =============================================================================   
def __matplotlib_plot(data = None,texts = None, titles = None, label = None,figsize = (20,10), if_reset_xaxis_MultipleLocator = True,if_legend = True,save_path = None):
    fig = plt.figure(figsize=figsize)
    figp = fig.subplots(1,1)
    if if_reset_xaxis_MultipleLocator: figp.xaxis.set_major_locator(ticker.MultipleLocator(int(len(__save_all)/10)))
    figp.plot(data,label = label)
    if texts != None: figp.text(texts[0], texts[1], texts[2])
    if titles != None: figp.set_title(titles)
    if if_legend: figp.legend()
    fig.show()
    if save_path == None: pass
    else: fig.savefig(os.path.join(save_path,titles+'.jpg'))   
    
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
        
def _QA_VectorBacktest(df = None,comission = None, params = None,params_id = None,save_path =None):
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
        # data['real_return'] = data['close'].pct_change().shift(-1)
        data['real_return'] = np.where((data['signal']!=0)&(data['signal'].shift(1)!=data['signal']),data['close'].shift(-1)/data['open'].shift(-1),data['close'].shift(-1)/data['close'])
        data['strategy'] = data['signal']*data['real_return']
        data['strategy'] = np.where((data['signal']!=0)&(data['signal'].shift(1)!=data['signal']),data['strategy']-comission,data['strategy'])

        return_table_temp = data.pivot(index='minute', columns='date', values='strategy')
        return_table_temp = (return_table_temp.fillna(0)+1).cumprod(axis=0)
        return_table_temp = return_table_temp.T
        return_table_temp['cum_ret_series'] = list(map(lambda x:list(x),return_table_temp.values))
        return_table_temp = return_table_temp[['cum_ret_series']]
        return_table_temp['strategy_return_daily'] = list(map(lambda x:x[-1],return_table_temp['cum_ret_series']))

#        return_table['strategy_return_daily'] = np.where(return_table['strategy_return_daily']!=1,return_table['strategy_return_daily']-comission,return_table['strategy_return_daily'])
        del return_table_temp['cum_ret_series']
# =============================================================================
#   存储回测结果
        data.to_csv(os.path.join(save_path,'acheck_data_'+code+'_'+params_id+'.csv'))
        return_table_temp.to_csv(os.path.join(save_path,'acheck_'+code+'_'+params_id+'.csv'))
        result_temp = _get_result(return_table = return_table_temp, code = code, params_id = params_id, params = params)
        result = result.append(result_temp)
    return result

def _get_result(return_table = None, code = None, params_id = None, params = None,if_show_params = True):
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
    if if_show_params:
        if code not in list(params.keys()): result_temp['params'] = params
        else:
            result_temp['params'] = params[code]
    else: pass
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
    types_dict = {}
    for i in params_optimize_dict.keys():
        types_dict[i] = type(params_optimize_dict[i][0])
    
    
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
            temp[sub_item] = types_dict[sub_item](splited_item[i])
        optimize_dict[count] = temp   
    return optimize_dict
    
def QA_VectorBacktest_func_add_fixed_stop(data = None,stop_loss_ret = None,stop_profit_ret = None):
    data['enter_long_price_mark'] = np.where(data['signal']>0,data['close'],np.nan)
    data['enter_short_price_mark'] = np.where(data['signal']<0,data['close'],np.nan)
    data['enter_long_price_mark'] = data['enter_long_price_mark'].ffill()
    data['enter_short_price_mark'] = data['enter_short_price_mark'].ffill()
    data['current_signal'] = data['signal'].ffill()
    data['current_ret'] = np.where(data['current_signal']>0,(data['close']/data['enter_long_price_mark'])-1,np.nan)
    data['current_ret'] = np.where(data['current_signal']<0,(data['enter_short_price_mark']/data['close'])-1,data['current_ret'])
    data['signal'] = np.where(data['current_ret']<=stop_loss_ret,0,data['signal'])
    data['signal'] = np.where(data['current_ret']>=stop_profit_ret,0,data['signal'])
    return data

def QA_VectorBacktest_func_add_limit_order(data = None,limit_point = None,limit_ret = None,interday = False):
    pass


def QA_VectorBacktest_func_fill_signal(data = None):
    data['signal'] = data['signal'].ffill().fillna(0)
#    data['signal'] = np.where((data['signal']!=0)&(data['signal'].shift(-1)==0),0,data['signal'])
    return data

def check_result():
    pass