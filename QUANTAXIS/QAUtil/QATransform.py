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

import csv
import json

import numpy as np
import pandas as pd
from QUANTAXIS.QAUtil.QALogs import QA_util_log_info

def QA_util_to_json_from_pandas(data):
    """需要对于datetime 和date 进行转换, 以免直接被变成了时间戳"""
    if 'datetime' in data.columns:
        data.datetime = data.datetime.apply(str)
    if 'date' in data.columns:
        data.date = data.date.apply(str)
    return json.loads(data.to_json(orient='records'))


def QA_util_to_json_from_numpy(data):
    pass


def QA_util_to_json_from_list(data):
    pass


def QA_util_to_list_from_pandas(data):
    return np.asarray(data).tolist()


def QA_util_to_list_from_numpy(data):
    return data.tolist()


def QA_util_to_pandas_from_json(data):

    if isinstance(data, dict):
        return pd.DataFrame(data=[data, ])
    else:
        return pd.DataFrame(data=[{'value': data}])


def QA_util_to_pandas_from_list(data):
    if isinstance(data, list):
        return pd.DataFrame(data=data)

def QA_util_to_anyformat_from_pandas(data = None,format = None):
    if format in ['P', 'p', 'pandas', 'pd']:
        return data
    elif format in ['json', 'dict']:
        return QA_util_to_json_from_pandas(data)
    # 多种数据格式
    elif format in ['n', 'N', 'numpy','array','ndarray']:
        return np.asarray(data)
    elif format in ['list', 'l', 'L']:
        return np.asarray(data).tolist()
    else:
        QA_util_log_info(
            "QA Error format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
        return None

def QA_util_to_pandas_from_RequestsResponse(data):
    list_all = []
    for line in data.iter_lines():
        decoded_line = line.decode('utf-8')
        temp_list = list(decoded_line.split(','))
        list_all.append(temp_list)

    return pd.DataFrame(data=list_all[1:], columns=list_all[0])