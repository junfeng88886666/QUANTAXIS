# coding:utf-8
import pandas as pd
import numpy as np

def QA_util_math_crossup(df = None,fac = None, threshold = None):
    pass

def QA_util_math_crossdown(df = None,fac = None, threshold = None):
    pass

def QA_util_math_cross(price,last_1_price,var):
    if (price>var)&(last_1_price<=var): return 1
    elif (price<var)&(last_1_price>=var): return -1
    else: return 0