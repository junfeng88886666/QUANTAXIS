# coding:utf-8

import csv
import json
import numpy as np
import pandas as pd
import os

def QA_util_listdir(path):
    dirlist = os.listdir(os.path.abspath(path))
    return [i for i in dirlist if (len(i.split('.'))==1)]


def QA_util_listfile(path,types = 'mat',if_ends = False):
    dirlist = os.listdir(os.path.abspath(path))
    if if_ends: return [i for i in dirlist if (i.split('.')[-1]==types)&(len(i.split('.'))>1)]
    return [i.split('.')[0] for i in dirlist if (i.split('.')[-1]==types)&(len(i.split('.'))>1)]