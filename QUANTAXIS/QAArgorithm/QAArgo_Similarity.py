# coding:utf-8
import pandas as pd
import numpy as np
import math

def QA_argo_similarity_DTWDistance(s1 = None, s2 = None, w = None):
    DTW={}

    for i in range(len(s1)):
        DTW[(i, -1)] = float('inf')
    for i in range(len(s2)):
        DTW[(-1, i)] = float('inf')
    DTW[(-1, -1)] = 0

    if w==None:
        for i in range(len(s1)):
            for j in range(len(s2)):
                dist= (s1[i]-s2[j])**2
                DTW[(i, j)] = dist + min(DTW[(i-1, j)],DTW[(i, j-1)], DTW[(i-1, j-1)])
    else:
        w = max(w, abs(len(s1)-len(s2)))
        for i in range(len(s1)):
                for j in range(max(0, i-w), min(len(s2), i+w)):
                    dist= (s1[i]-s2[j])**2
                    DTW[(i, j)] = dist + min(DTW[(i-1, j)],DTW[(i, j-1)], DTW[(i-1, j-1)])
    return math.sqrt(DTW[len(s1)-1, len(s2)-1])

def QA_argo_similarity_EuclidDistance(s1 = None, s2 = None):
    return math.sqrt(sum((s1-s2)**2))

