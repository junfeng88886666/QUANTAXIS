# coding:utf-8

# =============================================================================
# DEBUG
import pandas as pd
import numpy as np
import pyhht
import math
import warnings
warnings.simplefilter("ignore") 
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn


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

    data['signal'] = np.where(data['minute']>settle_time,0,data['signal'])
    data['signal'] = data['signal'].ffill().fillna(0)
    data['real_return'] = data['Close'].pct_change().shift(trading_shift)
    
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
