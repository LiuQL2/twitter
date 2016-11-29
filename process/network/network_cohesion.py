# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
根据的文件为：
(userId_community_id),
(source,target,number_of_interaction,weight),

计算社区挖掘的效果好坏。如果上面文件是针对某一个社区的，计算的结果就是这个社区内部的网络聚合度，如果是整个网络，那么计算的就是整个网络的聚合度。
"""

import pandas as pd
import numpy as np
import csv
import os
import sys
import time
import json
from utility.functions import get_dirlist
from utility.functions import write_log

reload(sys)
sys.setdefaultencoding('utf-8')


def calculate_degree(path,path_save_to, edge_file, node_file, sep = ',',header = None, type = 'community'):
    """
    计算网络节点的出度、入度、度，根据与该节点文件对应的边文件。并将计算出来的出度入度结果保存到相应的文件中。
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
    if type == 'community':
        node_dataFrame = pd.read_csv(path + node_file, sep = sep, names = ['user_id', 'community_id'], dtype={'user_id':np.str}, header = header)
    else:
        node_dataFrame = pd.read_csv(path + node_file, sep = ' ', names = ['user_id', 'community_id'], dtype={'user_id':np.str}, header = header)
    # node_dataFrame = pd.read_csv(path + node_file, sep = sep,  dtype={'id':np.str})
    node_dataFrame.index = node_dataFrame.user_id
    node_dataFrame['out_degree'] = 0
    node_dataFrame['in_degree'] = 0
    node_dataFrame['degree'] = 0
    print node_dataFrame
    user_id_list = list(set(list(node_dataFrame['user_id'])))
    index = 0
    for user_id in user_id_list:
        index = index + 1
        out_degree = len(edge_dataFrame[edge_dataFrame.source == user_id])
        in_degree = len(edge_dataFrame[edge_dataFrame.target == user_id])
        node_dataFrame.loc[[user_id],['out_degree']] = out_degree
        node_dataFrame.loc[[user_id],['in_degree']] = in_degree
        node_dataFrame.loc[[user_id],['degree']] = in_degree + out_degree
        print '***', index, user_id, 'degree:',in_degree + out_degree,'out_degree:',out_degree, 'in_degree:',in_degree

    node_dataFrame.to_csv(path_save_to + 'total_node_degree.csv',index = False, header = True)
    return node_dataFrame




def calculate_cohesion_whole_network(file_path,node_file_name,type,edge_file_name = None,edge_file_path = None, edge_file_key_word_list = None):
    """
    计算网络的聚合度，这里讲将边文件进行hash处理，以便快速查询。对于点文件，因为不同社区的聚合度为0，所以这里按照社区进行。一个一个考虑。
    这里分为整个网络和部分社区。整个网络需要将变文件hash存储。部分社区不需要。
    :param file_path:节点文件所在的路径，如果是针对部分社区的，也是边文件所在的路径
    :param node_file_name:节点文件名称，第一行是列名。无论整个网络还是部分社区，都是统一格式(user_id,community_id, out_degree,in_degree,degree)
    :param type:  处理的类型，是针对整个网络（’whole‘），还是针对部分社区（’community‘）。如果是针对的部分社区，需要对应的社区文件。
    :param edge_file_name:如果针对的是部分社区，该文件为边文件。且第一行为列名(source, target, number_of_interaction, weight)
    :param edge_file_path: 如果是针对整个网络，该路径为存储hash边文件的路径。
    :param edge_file_key_word_list: 如果是针对整个网络的话，
    :return:返回计算的结果。
    """

    node_dataFrame = pd.read_csv(file_path + node_file_name, dtype={'user_id':np.str})
    node_dataFrame.index = node_dataFrame.user_id
    if type == 'whole':
        edge_file_list = get_dirlist(path=edge_file_path, key_word_list=edge_file_key_word_list)
        print len(edge_file_list)
        print edge_file_list
        time.sleep(30)
        edge_dataFrame_dict = {}
        number_of_edges = 0
        for edge_file_name in edge_file_list:
            number = int((edge_file_name.split('hash_')[-1]).split('.')[0])
            edge_dataFrame_dict[number] = pd.read_csv(edge_file_path + edge_file_name, dtype={'source':np.str,'target':np.str})
            number_of_edges = number_of_edges + len(edge_dataFrame_dict[number])
    else:
        edge_dataFrame_dict = pd.read_csv(file_path + edge_file_name, header = 0, dtype={'source':np.str,'target':np.str})
        number_of_edges = len(edge_dataFrame_dict)
    number_of_edges = float(number_of_edges)
    community_id_list = list(set(list(node_dataFrame.community_id)))
    cohesion = 0.0
    number_i_j = 0
    temp_number_i_j = 0
    number_i_j_edge = 0
    for community_id in community_id_list:
        community_node_dataFrame = node_dataFrame[node_dataFrame.community_id == community_id]
        user_id_list = list(set(list(community_node_dataFrame.user_id)))
        for index_first in range(0, len(user_id_list), 1):
            for index_second in range(0, len(user_id_list), 1):
                if index_first != index_second:
                    number_i_j = number_i_j + 1
                    node_i = user_id_list[index_first]
                    node_j = user_id_list[index_second]
                    node_i_out_degree = community_node_dataFrame.out_degree[node_i]
                    node_j_in_degree = community_node_dataFrame.in_degree[node_j]
                    wether_edge = wether_interaction_between_nodes(node_i = node_i, node_j=node_j,edge_dataFrame_idct=edge_dataFrame_dict,hash_size=100,type = type)
                    cohesion = cohesion + (wether_edge - float(node_i_out_degree) * node_j_in_degree / number_of_edges) / number_of_edges
                    if wether_edge == 1.0:
                        number_i_j_edge = number_i_j_edge + 1
                        print '*' * 140
                        print 'community id:', community_id,'; node_i:',node_i, '; node_j:', node_j,'; cohesion:',cohesion, '; index first:', index_first, '; index_second:',index_second
                        print 'node i out_degree:',node_i_out_degree, '; node j in_degree:', node_j_in_degree, '; wether exits edge:',wether_edge,': number_i_j_edge:', number_i_j_edge
                    else:
                        print '*' * 140
                        print 'community id:', community_id,'; node_i:',node_i, '; node_j:', node_j,'; cohesion:',cohesion, '; index first:', index_first, '; index_second:',index_second
                        print 'node i out_degree:',node_i_out_degree, '; node j in_degree:', node_j_in_degree, '; wether exits edge:',wether_edge,': number_i_j_edge:', number_i_j_edge
                    if number_i_j - temp_number_i_j >= 100000:
                        temp_number_i_j = number_i_j
                        write_log(log_file_name='network_cohesion.log',log_file_path=os.getcwd(),information='Calculating cohesion. Cohesion:'+ str(cohesion) + '; community_id:' + str(community_id) + ' Number_i_j:' + str(number_i_j) )
                else:
                    pass


    return {'cohesion':cohesion, 'number_of_edges':number_of_edges,'number_i_j':number_i_j,'number_i_j_edge':number_i_j_edge }



def wether_interaction_between_nodes(node_i, node_j, edge_dataFrame_idct,type,hash_size = 100):
    """
    判断从节点 i 到节点 j 是否存在一个有向边。分两种情况，全部网络和部分社区。
    :param node_i:节点 i。也即是用户id。
    :param node_j: 节点 j。即用户id。
    :param edge_dataFrame_idct:当为全部网络时：一个字典，hash之后的边；当为部分社区时：一个dataFrame。
    :param type:类型，全部网络：’whole‘， 部分社区：其他。
    :param hash_size:如果是全部网络，hash的文件个数
    :return:存在一个有向边：1，否者：0.
    """
    if type == 'whole':
        node_i_number = hash(str(node_i)) % hash_size
        temp_edge_dataFrame_i = edge_dataFrame_idct[node_i_number]
        if node_j not in list((temp_edge_dataFrame_i[temp_edge_dataFrame_i.source == node_i]).target):
            return 0.0
        else:
            return 1.0
    else:
        if node_j not in list((edge_dataFrame_idct[edge_dataFrame_idct.source == node_i]).target):
            return 0.0
        else:
            return 1.0

def main():
    file_path = 'D:/LiuQL/eHealth/twitter/visualization/node_edge/'
    node_file_name = 'labeled_community_nodes_2000.csv'
    type = 'whole'
    edge_file_name = 'community_edges_2000.csv'
    edge_file_path = 'D:/test/edge_hash/'
    edge_file_key_word_list = ['edge_hash_','.csv']

    #Dubai server
    file_path ='/pegasus/harir/Qianlong/data/network/'
    node_file_name ='total_node_degree.csv'
    type = 'whole'
    edge_file_name = 'total_edge_weight.csv'
    edge_file_path = '/pegasus/harir/Qianlong/data/network/edge_hash/'
    edge_file_key_word_list = ['edge_hash_','.csv']

    write_log(log_file_name='network_cohesion.log', log_file_path=os.getcwd(),
              information='\n' * 4 + '*' * 50 + 'Starting Calculate cohesion' + '*' * 50 )

    #whole
    info_dict = calculate_cohesion_whole_network(file_path = file_path, node_file_name = node_file_name, type = type, edge_file_name=None, edge_file_path=edge_file_path,edge_file_key_word_list=edge_file_key_word_list)
    #community
    # info_dict = calculate_cohesion_whole_network(file_path = file_path, node_file_name = node_file_name, type = 'community', edge_file_name=edge_file_name, edge_file_path=None,edge_file_key_word_list=None)
    write_log(log_file_name='network_cohesion.log', log_file_path=os.getcwd(),
              information=str(info_dict))
    cohesion_file = open(os.getcwd() + '/cohesion_info.json','wb')
    row = json.dumps(info_dict) + '\n'
    cohesion_file.write(row)
    cohesion_file.close()


main()