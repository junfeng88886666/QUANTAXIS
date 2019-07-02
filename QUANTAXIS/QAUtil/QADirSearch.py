# coding:utf-8

import csv
import json
import numpy as np
import pandas as pd
import os

def QA_util_listdir(path):
    '''返回目标目录下的所有文件夹'''
    dirlist = os.listdir(os.path.abspath(path))
    return [i for i in dirlist if (len(i.split('.'))==1)]


def QA_util_listfile(path,types = 'mat',if_ends = False):
    '''返回目标目录下的所有文件'''
    dirlist = os.listdir(os.path.abspath(path))
    if if_ends: return [i for i in dirlist if (i.split('.')[-1]==types)&(len(i.split('.'))>1)]
    return [i.split('.')[0] for i in dirlist if (i.split('.')[-1]==types)&(len(i.split('.'))>1)]