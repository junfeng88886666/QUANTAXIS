# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2018 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from datetime import time

import pandas as pd
from QUANTAXIS.QAUtil.QADate import QA_util_date_stamp,QA_util_time_stamp
from QUANTAXIS.QAUtil.QADate_trade import QA_util_future_to_realdatetime,QA_util_future_to_tradedatetime
from QUANTAXIS.QAUtil.QAParameter import DATASOURCE,DATA_AGGREMENT_NAME
from QUANTAXIS.QAUtil.QACode import QA_util_futurecode_tosimple
from QUANTAXIS.QAData.QADataAggrement import select_DataAggrement

def QA_data_stocktick_resample_1min(tick, type_='1min', source = 'tick_resample',if_drop=True):
    """
    tick 采样为 分钟数据(标准QA数据协议格式)
    1. 仅使用将 tick 采样为 1 分钟数据
    2. 仅测试过，与通达信 1 分钟数据达成一致
    3. 经测试，可以匹配 QA.QA_fetch_get_stock_transaction 得到的数据，其他类型数据未测试
    demo:
    df = QA.QA_fetch_get_stock_transaction(package='tdx', code='000001',
                                           start='2018-08-01 09:25:00',
                                           end='2018-08-03 15:00:00')
    df_min = QA_data_stocktick_resample_1min(df)
    """
    tick['volume'] = tick['volume'] * 100.0
    tick = tick.assign(amount=tick.price * tick.volume)
    resx = pd.DataFrame()
    tick.index=pd.to_datetime(tick.datetime)
    _dates = set(tick.date)

    for date in sorted(list(_dates)):
        _data = tick.loc[tick.date == date]
        # morning min bar
        _data1 = _data[time(9,
                            25):time(11,
                                     30)].resample(
                                         type_,
                                         closed='left',
                                         base=30,
                                         loffset=type_
                                     ).apply(
                                         {
                                             'price': 'ohlc',
                                             'volume': 'sum',
                                             'code': 'last',
                                             'amount': 'sum'
                                         }
                                     )
        _data1.columns = _data1.columns.droplevel(0)
        # do fix on the first and last bar
        # 某些股票某些日期没有集合竞价信息，譬如 002468 在 2017 年 6 月 5 日的数据
        if len(_data.loc[time(9, 25):time(9, 25)]) > 0:
            _data1.loc[time(9,
                            31):time(9,
                                     31),
                       'open'] = _data1.loc[time(9,
                                                 26):time(9,
                                                          26),
                                            'open'].values
            _data1.loc[time(9,
                            31):time(9,
                                     31),
                       'high'] = _data1.loc[time(9,
                                                 26):time(9,
                                                          31),
                                            'high'].max()
            _data1.loc[time(9,
                            31):time(9,
                                     31),
                       'low'] = _data1.loc[time(9,
                                                26):time(9,
                                                         31),
                                           'low'].min()
            _data1.loc[time(9,
                            31):time(9,
                                     31),
                       'volume'] = _data1.loc[time(9,
                                                26):time(9,
                                                         31),
                                           'volume'].sum()
            _data1.loc[time(9,
                            31):time(9,
                                     31),
                       'amount'] = _data1.loc[time(9,
                                                   26):time(9,
                                                            31),
                                              'amount'].sum()
        # 通达信分笔数据有的有 11:30 数据，有的没有
        if len(_data.loc[time(11, 30):time(11, 30)]) > 0:
            _data1.loc[time(11,
                            30):time(11,
                                     30),
                       'high'] = _data1.loc[time(11,
                                                 30):time(11,
                                                          31),
                                            'high'].max()
            _data1.loc[time(11,
                            30):time(11,
                                     30),
                       'low'] = _data1.loc[time(11,
                                                30):time(11,
                                                         31),
                                           'low'].min()
            _data1.loc[time(11,
                            30):time(11,
                                     30),
                       'close'] = _data1.loc[time(11,
                                                  31):time(11,
                                                           31),
                                             'close'].values
            _data1.loc[time(11,
                            30):time(11,
                                     30),
                       'volume'] = _data1.loc[time(11,
                                                30):time(11,
                                                         31),
                                           'volume'].sum()
            _data1.loc[time(11,
                            30):time(11,
                                     30),
                       'amount'] = _data1.loc[time(11,
                                                   30):time(11,
                                                            31),
                                              'amount'].sum()
        _data1 = _data1.loc[time(9, 31):time(11, 30)]

        # afternoon min bar
        _data2 = _data[time(13,
                            0):time(15,
                                    0)].resample(
                                        type_,
                                        closed='left',
                                        base=30,
                                        loffset=type_
                                    ).apply(
                                        {
                                            'price': 'ohlc',
                                            'volume': 'sum',
                                            'code': 'last',
                                            'amount': 'sum'
                                        }
                                    )

        _data2.columns = _data2.columns.droplevel(0)
        # 沪市股票在 2018-08-20 起，尾盘 3 分钟集合竞价
        if (pd.Timestamp(date) <
                pd.Timestamp('2018-08-20')) and (tick.code.iloc[0][0] == '6'):
            # 避免出现 tick 数据没有 1:00 的值
            if len(_data.loc[time(13, 0):time(13, 0)]) > 0:
                _data2.loc[time(15,
                                0):time(15,
                                        0),
                           'high'] = _data2.loc[time(15,
                                                     0):time(15,
                                                             1),
                                                'high'].max()
                _data2.loc[time(15,
                                0):time(15,
                                        0),
                           'low'] = _data2.loc[time(15,
                                                    0):time(15,
                                                            1),
                                               'low'].min()
                _data2.loc[time(15,
                                0):time(15,
                                        0),
                           'close'] = _data2.loc[time(15,
                                                      1):time(15,
                                                              1),
                                                 'close'].values
        else:
            # 避免出现 tick 数据没有 15:00 的值
            if len(_data.loc[time(13, 0):time(13, 0)]) > 0:
                _data2.loc[time(15,
                                0):time(15,
                                        0)] = _data2.loc[time(15,
                                                              1):time(15,
                                                                      1)].values
        _data2 = _data2.loc[time(13, 1):time(15, 0)]
        resx = resx.append(_data1).append(_data2)

    if if_drop:
        resx = resx.dropna()

    '''整理数据格式到STOCK_MIN格式'''
    resx['datetime'] = resx.index
    resx = resx \
               .assign(date=resx['datetime'].apply(lambda x: str(x)[0:10]),
                       date_stamp=resx['datetime'].apply(
                           lambda x: QA_util_date_stamp(x)),
                       time_stamp=resx['datetime'].apply(
                           lambda x: QA_util_time_stamp(x)),
                       type=type_)
    resx['source'] = source
    return select_DataAggrement(DATA_AGGREMENT_NAME.STOCK_MIN)(None, resx)

def _resample_edit_periods(data,edit_time_period):
    raise NotImplementedError


def QA_data_futuretick_resample_min(tick, type_='1min', source = 'tick_resample',if_drop=True):
    """tick采样成1分钟级别分钟线，支持QA_DataAggrement_future_transaction数据
    Arguments:
        tick {[type]} -- transaction

    Returns:
        [type] -- [description]
    """
    tick = tick.sort_values(by=['datetime','code'])
    tick.index = pd.to_datetime(tick.datetime)
    data = tick.resample(
                             type_,
                             closed='left',
                             base=30,
                             loffset=type_
                         ).apply(
                             {
                                 'price': 'ohlc',
                                 'volume': 'sum',
                                 'zengcang':'sum',
                                 'code': 'last'
                             }
                         )
    data.columns = data.columns.droplevel(0)
        # for edit_time_period in MARKET_PRESET.get_resample_edit_periods(code):
        #     _data_min = _resample_edit_periods(_data_min,edit_time_period)
        # resx = resx.append(_data_min)
    '''整理成符合分钟数据协议的数据格式'''
    data.rename(columns = {'volume':'trade','zengcang':'position'},inplace = True)
    for i in ['open', 'high', 'low', 'close']: data[i] /= 1000
    data['datetime'] = list(map(lambda x:str(x)[:19],data.index))
    data['tradetime']=pd.to_datetime(data['datetime'].apply(QA_util_future_to_tradedatetime))
    data = data \
        .assign(date=data['datetime'].apply(lambda x: str(x)[0:10])) \
        .assign(date_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(x))) \
        .assign(time_stamp=data['datetime'].apply(lambda x: QA_util_time_stamp(x))) \
        .assign(type=type_)
    data['source'] = source
    if if_drop: data = data.dropna()
    return select_DataAggrement(DATA_AGGREMENT_NAME.FUTURE_MIN)(DATASOURCE.TDX,data)



def QA_data_min_resample_stock(min_data, type_='30min', source = '1min_resample'):
    """分钟线采样成大周期


    分钟线采样成子级别的分钟线


    time+ OHLC==> resample
    Arguments:
        min {[type]} -- [description]
        raw_type {[type]} -- [description]
        new_type {[type]} -- [description]
    """
    min_data['datetime'] = pd.to_datetime(min_data['datetime'])
    try:
        min_data = min_data.reset_index().set_index('datetime', drop=False)
    except:
        min_data = min_data.set_index('datetime', drop=False)

    CONVERSION = {'code': 'first',
                  'open': 'first',
                  'high': 'max',
                  'low': 'min',
                  'close': 'last',
                  'vol': 'sum',
                  'amount': 'sum'
    } if 'vol' in min_data.columns else {'code': 'first',
                                         'open': 'first',
                                         'high': 'max',
                                         'low': 'min',
                                         'close': 'last',
                                         'volume': 'sum',
                                         'amount': 'sum'}

    resx = pd.DataFrame()
    
    for item in set(min_data.index.date):
        min_data_p = min_data.loc[str(item)]
        n = min_data_p['{} 21:00:00'.format(item):].resample(
            type_,
            base=30,
            closed='right',
            loffset=type_
        ).apply(CONVERSION)

        d = min_data_p[:'{} 11:30:00'.format(item)].resample(
            type_,
            base=30,
            closed='right',
            loffset=type_
        ).apply(CONVERSION)

        f = min_data_p['{} 13:00:00'.format(item):].resample(
            type_,
            closed='right',
            loffset=type_
        ).apply(CONVERSION)

        resx = resx.append(d).append(f)
    '''整理数据格式到STOCK_MIN格式'''
    resx['datetime'] = resx.index
    resx = resx \
               .assign(date=resx['datetime'].apply(lambda x: str(x)[0:10]),
                       date_stamp=resx['datetime'].apply(
                           lambda x: QA_util_date_stamp(x)),
                       time_stamp=resx['datetime'].apply(
                           lambda x: QA_util_time_stamp(x)),
                       type=type_ if 'min' in str(type_) else str(type_)+'min')
    resx['source'] = source
    return select_DataAggrement(DATA_AGGREMENT_NAME.STOCK_MIN)(None, resx)
    # return resx.dropna().reset_index().set_index(['datetime', 'code'])

# def QA_data_min_resample_future():
#     raise NotImplementedError


##########################################################################################################
##########################################################################################################
##########################################################################################################

def QA_data_tick_resample(tick, type_='1min'):
    """tick采样成任意级别分钟线

    Arguments:
        tick {[type]} -- transaction

    Returns:
        [type] -- [description]
    """
    tick = tick.assign(amount=tick['price'] * tick['volume'])
    resx = pd.DataFrame()
    __type_index = type(tick.index[0])
    tick.index = pd.to_datetime(tick.index)
    _temp = set(tick.index.date)

    for item in _temp:
        _data = tick.loc[str(item)]
        _data1 = _data[time(9,
                            31):time(11,
                                     30)].resample(
                                         type_,
                                         closed='right',
                                         base=30,
                                         loffset=type_
                                     ).apply(
                                         {
                                             'price': 'ohlc',
                                             'volume': 'sum',
                                             'code': 'last',
                                             'amount': 'sum'
                                         }
                                     )

        _data2 = _data[time(13,
                            1):time(15,
                                    0)].resample(
                                        type_,
                                        closed='right',
                                        loffset=type_
                                    ).apply(
                                        {
                                            'price': 'ohlc',
                                            'volume': 'sum',
                                            'code': 'last',
                                            'amount': 'sum'
                                        }
                                    )

        resx = resx.append(_data1).append(_data2)
    resx.columns = resx.columns.droplevel(0)
    return resx
    # return resx.reset_index().drop_duplicates().set_index(['datetime', 'code'])


def QA_data_ctptick_resample(tick, type_='1min'):
    """tick采样成任意级别分钟线

    Arguments:
        tick {[type]} -- transaction

    Returns:
        [type] -- [description]
    """

    resx = pd.DataFrame()
    _temp = set(tick.TradingDay)

    for item in _temp:

        _data = tick.query('TradingDay=="{}"'.format(item))
        try:
            _data.loc[time(20, 0):time(21, 0), 'volume'] = 0
        except:
            pass

        _data.volume = _data.volume.diff()
        _data = _data.assign(amount=_data.LastPrice * _data.volume)
        _data0 = _data[time(0,
                            0):time(2,
                                    30)].resample(
                                        type_,
                                        closed='right',
                                        base=30,
                                        loffset=type_
                                    ).apply(
                                        {
                                            'LastPrice': 'ohlc',
                                            'volume': 'sum',
                                            'code': 'last',
                                            'amount': 'sum'
                                        }
                                    )

        _data1 = _data[time(9,
                            0):time(11,
                                    30)].resample(
                                        type_,
                                        closed='right',
                                        base=30,
                                        loffset=type_
                                    ).apply(
                                        {
                                            'LastPrice': 'ohlc',
                                            'volume': 'sum',
                                            'code': 'last',
                                            'amount': 'sum'
                                        }
                                    )

        _data2 = _data[time(13,
                            1):time(15,
                                    0)].resample(
                                        type_,
                                        closed='right',
                                        base=30,
                                        loffset=type_
                                    ).apply(
                                        {
                                            'LastPrice': 'ohlc',
                                            'volume': 'sum',
                                            'code': 'last',
                                            'amount': 'sum'
                                        }
                                    )

        _data3 = _data[time(21,
                            0):time(23,
                                    59)].resample(
                                        type_,
                                        closed='left',
                                        loffset=type_
                                    ).apply(
                                        {
                                            'LastPrice': 'ohlc',
                                            'volume': 'sum',
                                            'code': 'last',
                                            'amount': 'sum'
                                        }
                                    )

        resx = resx.append(_data0).append(_data1).append(_data2).append(_data3)
    resx.columns = resx.columns.droplevel(0)
    return resx.reset_index().drop_duplicates().set_index(['datetime',
                                                           'code']).sort_index()


def QA_data_min_resample(min_data, type_='5min'):
    """分钟线采样成大周期
    分钟线采样成子级别的分钟线
    time+ OHLC==> resample
    Arguments:
        min {[type]} -- [description]
        raw_type {[type]} -- [description]
        new_type {[type]} -- [description]
    """

    try:
        min_data = min_data.reset_index().set_index('datetime', drop=False)
    except:
        min_data = min_data.set_index('datetime', drop=False)

    CONVERSION = {}
    if 'code' in min_data.columns: CONVERSION['code'] = 'first'
    if 'open' in min_data.columns: CONVERSION['open'] = 'first'
    if 'high' in min_data.columns: CONVERSION['high'] = 'max'
    if 'low' in min_data.columns: CONVERSION['low'] = 'min'
    if 'close' in min_data.columns: CONVERSION['close'] = 'last'
    if 'vol' in min_data.columns: CONVERSION['vol'] = 'sum'
    if 'volume' in min_data.columns: CONVERSION['volume'] = 'sum'
    if 'amount' in min_data.columns: CONVERSION['amount'] = 'sum'
    if 'position' in min_data.columns: CONVERSION['position'] = 'last'
    if 'tradetime' in min_data.columns: CONVERSION['tradetime'] = 'last'

    resx = pd.DataFrame()

    for item in set(min_data.index.date):
        min_data_p = min_data.loc[str(item)]
        n = min_data_p['{} 21:00:00'.format(item):].resample(
            type_,
            base=30,
            closed='right',
            loffset=type_
        ).apply(CONVERSION)

        d = min_data_p[:'{} 11:30:00'.format(item)].resample(
            type_,
            base=30,
            closed='right',
            loffset=type_
        ).apply(CONVERSION)
        f = min_data_p['{} 13:00:00'.format(item):].resample(
            type_,
            closed='right',
            loffset=type_
        ).apply(CONVERSION)

        resx = resx.append(d).append(f)

    return resx.dropna().reset_index().set_index(['datetime', 'code'])

def QA_data_futuremin_resample(min_data, type_='5min'):
    """期货分钟线采样成大周期


    分钟线采样成子级别的分钟线

    future:

    vol ==> trade
    amount X
    """

    min_data.tradeime = pd.to_datetime(min_data.tradetime)

    CONVERSION = {
        'code': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'trade': 'sum',
        'tradetime': 'last',
        'date': 'last'
    }
    resx = min_data.resample(
        type_,
        closed='right',
        loffset=type_
    ).apply(CONVERSION)
    return resx.dropna().reset_index().set_index(['datetime', 'code'])


def QA_data_day_resample(day_data, type_='w'):
    """日线降采样

    Arguments:
        day_data {[type]} -- [description]

    Keyword Arguments:
        type_ {str} -- [description] (default: {'w'})

    Returns:
        [type] -- [description]
    """
    # return day_data_p.assign(open=day_data.open.resample(type_).first(),high=day_data.high.resample(type_).max(),low=day_data.low.resample(type_).min(),\
    #             vol=day_data.vol.resample(type_).sum() if 'vol' in day_data.columns else day_data.volume.resample(type_).sum(),\
    #             amount=day_data.amount.resample(type_).sum()).dropna().set_index('date')
    try:
        day_data = day_data.reset_index().set_index('date', drop=False)
    except:
        day_data = day_data.set_index('date', drop=False)

    CONVERSION = {}
    if 'code' in day_data.columns: CONVERSION['code'] = 'first'
    if 'open' in day_data.columns: CONVERSION['open'] = 'first'
    if 'high' in day_data.columns: CONVERSION['high'] = 'max'
    if 'low' in day_data.columns: CONVERSION['low'] = 'min'
    if 'close' in day_data.columns: CONVERSION['close'] = 'last'
    if 'vol' in day_data.columns: CONVERSION['vol'] = 'sum'
    if 'volume' in day_data.columns: CONVERSION['volume'] = 'sum'
    if 'amount' in day_data.columns: CONVERSION['amount'] = 'sum'
    if 'position' in day_data.columns: CONVERSION['position'] = 'last'


    return day_data.resample(
        type_,
        closed='right'
    ).apply(CONVERSION).dropna().reset_index().set_index(['date',
                                                          'code'])


if __name__ == '__main__':
    import QUANTAXIS as QA
    tick = QA.QA_fetch_get_stock_transaction(
        'tdx',
        '000001',
        '2018-08-01',
        '2018-08-02'
    )
    print(QA_data_tick_resample(tick, '60min'))
    print(QA_data_tick_resample(tick, '15min'))
    print(QA_data_tick_resample(tick, '35min'))

    print("test QA_data_min_resample_stock, level: 120")
    start, end, level = "2019-05-01", "2019-05-08", 120
    data = QA.QA_fetch_stock_min_adv("000001", start, end)
    res = QA_data_min_resample_stock(data.data, level)
    print(res)
    res2 = QA.QA_fetch_stock_min_adv(["000001", '000002'], start, end).add_func(QA_data_min_resample_stock, level)
    print(res2)


# def QA_data_min_resample_stock(min_data, period=5, source = '1min_resample'):
#     """
#     1min 分钟线采样成 period 级别的分钟线（标准QA数据协议格式）
#     :param min_data:
#     :param period:
#     :return:
#     """
#     if isinstance(period, float):
#         period = int(period)
#     elif isinstance(period, str):
#         period = period.replace('min', '')
#     elif isinstance(period, int):
#         pass
#     _period = '%sT' % period
#     min_data = min_data.reset_index()
#     if 'datetime' not in min_data.columns:
#         return None
#     # 9:30 - 11:30
#     min_data['datetime'] = pd.to_datetime(min_data['datetime'])
#     min_data_morning = min_data.set_index(
#         "datetime").loc[time(9, 30):time(11, 30)].reset_index()
#     min_data_morning.index = pd.DatetimeIndex(
#         min_data_morning.datetime).to_period('T')
#     # 13:00 - 15:00
#     min_data_afternoon = min_data.set_index(
#         "datetime").loc[time(13, 00):time(15, 00)].reset_index()
#     min_data_afternoon.index = pd.DatetimeIndex(
#         min_data_afternoon.datetime).to_period('T')
#
#     _conversion = {
#         'code': 'first',
#         'open': 'first',
#         'high': 'max',
#         'low': 'min',
#         'close': 'last',
#     }
#     if 'vol' in min_data.columns:
#         _conversion["vol"] = "sum"
#     elif 'volume' in min_data.columns:
#         _conversion["volume"] = "sum"
#     if 'amount' in min_data.columns:
#         _conversion['amount'] = 'sum'
#
#     res = pd.concat([
#         min_data_morning.resample(
#             _period, closed="right", kind="period").apply(_conversion).dropna(),
#         min_data_afternoon.resample(
#             _period, closed="right", kind="period").apply(_conversion).dropna()
#     ])
#     # 10:31:00 => 10:30:00
#     res.index = (res.index + res.index.freq).to_timestamp() - \
#         pd.Timedelta(minutes=1)
#
#     '''整理数据格式到STOCK_MIN格式'''
#     res['datetime'] = res.index
#     res = res \
#                .assign(date=res['datetime'].apply(lambda x: str(x)[0:10]),
#                        date_stamp=res['datetime'].apply(
#                            lambda x: QA_util_date_stamp(x)),
#                        time_stamp=res['datetime'].apply(
#                            lambda x: QA_util_time_stamp(x)),
#                        type=period if 'min' in str(period) else str(period)+'min')
#     res['source'] = source
#     return select_DataAggrement(DATA_AGGREMENT_NAME.STOCK_MIN)(None, res)
