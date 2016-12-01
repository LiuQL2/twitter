# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
为一个社区中出入度比较高的用户查找姓名，以便在可视化的时候显示其标签。
需要的文件：
社区节点文件：(user_id,community_id),csv，逗号隔开，无header。
社区边文件：(source, target, number_of_interaction, weight),csv，逗号隔开，无header。
所有用户id，name的文件(user_id, is_verified, name),csv，逗号隔开，无header。
"""

import pandas as pd
import numpy as np
import csv
import os
import sys
import time

reload(sys)
sys.setdefaultencoding('utf-8')


def calculate_degree(path, edge_file, node_file, sep = ',',header = None):
    """
    计算网络节点的出度、入度、度，根据与该节点文件对应的边文件。
    :param path:节点、边文件保存的位置。
    :param edge_file:边文件。(source, target, number_of_interaction, weight),csv文件逗号分隔。
    :param node_file:节点文件。(user_id, community_id)
    :param sep:文件分隔的符号
    :param header:第一行是否是列名
    :return:返回一个有出入度的节点DataFrame。
    """
    print path + edge_file
    edge_dataFrame = pd.read_csv(path + edge_file, sep=sep, names = ['source','target', 'number_of_interaction','weight'], dtype={'source':np.str, 'target':np.str})
    # edge_dataFrame = pd.read_csv(path + edge_file, sep=sep, dtype={'source':np.str, 'target':np.str})
    node_dataFrame = pd.read_csv(path + node_file, sep = sep, names = ['user_id', 'community_id'], dtype={'user_id':np.str}, header = header)
    # node_dataFrame = pd.read_csv(path + node_file, sep = sep,  dtype={'id':np.str})
    node_dataFrame.index = node_dataFrame.user_id
    node_dataFrame['out_degree'] = 0
    node_dataFrame['in_degree'] = 0
    node_dataFrame['degree'] = 0
    print node_dataFrame
    user_id_list = set(list(node_dataFrame['user_id']))
    index = 0
    for user_id in user_id_list:
        index = index + 1
        out_degree = len(edge_dataFrame[edge_dataFrame.source == user_id])
        in_degree = len(edge_dataFrame[edge_dataFrame.target == user_id])
        node_dataFrame.loc[[user_id],['out_degree']] = out_degree
        node_dataFrame.loc[[user_id],['in_degree']] = in_degree
        node_dataFrame.loc[[user_id],['degree']] = in_degree + out_degree
        print '***', index, user_id, 'degree:',in_degree + out_degree,'out_degree:',out_degree, 'in_degree:',in_degree
    return node_dataFrame


def label_nodes(node_dataFrame, top_node_size, path, label_file,sep = ','):
    """
    根据节点的出入度，对度为top的节点进行打标签。
    :param node_dataFrame: 计算了出入度、度的节点文件。
    :param top_node_size:每一个社区需要标记的节点个数。
    :param path:标签文件的保存路径。
    :param label_file:节点标签文件。格式为(user_id, verify, name),csv文件，以sep分隔
    :param sep:标签文件中的分隔符。
    :param header:标签文件第一行是否是列名。
    :return:
    """
    node_dataFrame['label'] = None
    id_label_dataFrame = pd.read_csv(path + label_file, names = ['id','is_verified','label'],header = None, dtype = {'id':np.str}, sep = sep)
    id_label_dataFrame.index = id_label_dataFrame.id
    community_id_list = set(list(node_dataFrame.community_id))
    node_dataFrame.index = node_dataFrame.user_id
    for community_id in community_id_list:
        print 'community id:', community_id, 'is being label...'
        community_node_dataFrame = (node_dataFrame[node_dataFrame.community_id == community_id]).sort_values(by = ['degree'], ascending = [0])
        top_id_list = list(community_node_dataFrame.user_id)[0:top_node_size]
        for id in top_id_list:
            node_dataFrame.loc[[id],['label']] = id_label_dataFrame.label[id]
            print id, id_label_dataFrame.label[id]
    return node_dataFrame


def main():
    path = 'D:/LiuQL/eHealth/twitter/visualization/network/'
    edge_file = 'community_edges_2000.csv'
    node_file = 'community_nodes_2000.csv'
    label_file = 'user_all_yang.csv'
    node_dataFrame = calculate_degree(path = path,edge_file=edge_file,node_file=node_file)
    labeled_node_dataFrame = label_nodes(node_dataFrame=node_dataFrame,top_node_size=10,path = path, label_file=label_file)
    labeled_node_dataFrame.to_csv( path + 'labeled_' + node_file,index = False, header = True)


# time.sleep(1800)
main()


