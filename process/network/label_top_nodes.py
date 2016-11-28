# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
为一个社区中出入度比较高的用户查找姓名，以便在可视化的时候显示其标签。
"""

import pandas as pd
import numpy as np
import csv
import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


def calculate_degree(path, edge_file, node_file, column_name, sep = ',',header = None):
    # edge_dataFrame = pd.read_csv(path + edge_file, sep=sep, names = ['source','target', 'weight'], dtype={'source':np.str, 'target':np.str},header=header)
    edge_dataFrame = pd.read_csv(path + edge_file, sep=sep, dtype={'source':np.str, 'target':np.str})
    # node_dataFrame = pd.read_csv(path + node_file, sep = sep, names = ['user_id', 'community_id'], dtype={'user_id':np.str}, header = header)
    node_dataFrame = pd.read_csv(path + node_file, sep = sep,  dtype={'id':np.str})
    node_dataFrame.index = node_dataFrame.id
    node_dataFrame['out_degree'] = 0
    node_dataFrame['in_degree'] = 0
    node_dataFrame['degree'] = 0

    community_id_list = set(list(node_dataFrame.community_id))
    user_id_list = set(list(node_dataFrame['user_id']))
    for user_id in user_id_list:
        out_degree = len(edge_dataFrame[edge_dataFrame.source == user_id])
        in_degree = len(edge_dataFrame[edge_dataFrame.target == user_id])
        node_dataFrame.loc[[user_id],['out_degree']] = out_degree
        node_dataFrame.loc[[user_id],['in_degree']] = in_degree
        node_dataFrame.loc[[user_id],['degree']] = in_degree + out_degree

def label_nodes(node_dataFrame, top_node_size, path, label_file):



def build_id_label(path, id_label_name_file, save_file):
    id_label_dataFrame = pd.read_csv(path + id_label_name_file, names=['id','label','none'], dtype={'id':np.str}, header=None)
    del id_label_dataFrame['none']
    id_list = set(list(id_label_dataFrame.id))
    label_file = open(path + save_file, 'wb')
    writer = csv.writer(label_file)
    for id in id_list:
        label = list(id_label_dataFrame[id_label_dataFrame.id == id]['label'])[-1]
        writer.writerow([id, label])
        print id, label
    label_file.close()


build_id_label(path='D:/LiuQL/eHealth/twitter/visualization/', id_label_name_file= 'idname.txt', save_file='id_label.csv')
