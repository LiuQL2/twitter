# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
根据的文件为：
(userId_community_id),
(source,target,number_of_interaction,weight),
计算社区挖掘的效果好坏。如果上面文件是针对某一个社区的，计算的结果就是这个社区内部的网络聚合度，如果是整个网络，那么计算的就是整个网络的聚合度。
按照社区进行多线程处理多线程进行。
"""

import pandas as pd
import numpy as np
import csv
import os
import sys
import time
import json
import threading
import Queue
from utility.functions import get_dirlist
from utility.functions import write_log

reload(sys)
sys.setdefaultencoding('utf-8')


class cohesionThread(threading.Thread):
    def __init__(self, edge_dataFrame, node_dataFrame, community_id, lock,cohesion_file,hash_size = 100,number_of_edges = 0):
        threading.Thread.__init__(self)
        self.edge_dataFrame = edge_dataFrame
        self.node_dataFrame = node_dataFrame
        self.community_id = community_id
        self.number_of_edges = number_of_edges
        self.number_i_j = 0
        self.number_i_j_edge = 0
        self.cohesion = 0.0
        self.hash_size = hash_size
        self.cohesion_file = cohesion_file
        self.lock = lock


    def run(self):
        if self.number_of_edges == 0:
            self.get_community_edges()
            self.calculate_degree()
        else:
            pass
        user_id_list = list(set(list(self.node_dataFrame.user_id)))
        temp_number_i_j = 0
        for index_first in range(0, len(user_id_list), 1):
            for index_second in range(0, len(user_id_list), 1):
                if index_first != index_second:
                    self.number_i_j = self.number_i_j + 1
                    node_i = user_id_list[index_first]
                    node_j = user_id_list[index_second]
                    node_i_out_degree = self.node_dataFrame.out_degree[node_i]
                    node_j_in_degree = self.node_dataFrame.in_degree[node_j]
                    wether_edge = self.wether_interaction_between_nodes(node_i=node_i, node_j=node_j)
                    self.cohesion = self.cohesion + (wether_edge - float(
                        node_i_out_degree) * node_j_in_degree / self.number_of_edges) / self.number_of_edges
                    if wether_edge == 1.0:
                        self.number_i_j_edge = self.number_i_j_edge + 1
                        print '\n' + '*' * 140
                        print 'community id:', self.community_id, '; node_i:', node_i, '; node_j:', node_j, '; cohesion:', self.cohesion, '; index first:', index_first, '; index_second:', index_second
                        print 'node i out_degree:', node_i_out_degree, '; node j in_degree:', node_j_in_degree, '; wether exits edge:', wether_edge, ': number_i_j_edge:', self.number_i_j_edge, '; number_i_j:', self.number_i_j
                    else:
                        print '\n' + '*' * 140
                        print 'community id:', self.community_id, '; node_i:', node_i, '; node_j:', node_j, '; cohesion:', self.cohesion, '; index first:', index_first, '; index_second:', index_second
                        print 'node i out_degree:', node_i_out_degree, '; node j in_degree:', node_j_in_degree, '; wether exits edge:', wether_edge, ': number_i_j_edge:', self.number_i_j_edge, '; number_i_j:', self.number_i_j
                    if self.number_i_j - temp_number_i_j >= 100000:
                        temp_number_i_j = self.number_i_j
                        write_log(log_file_name='network_cohesion_community_thread.log', log_file_path=os.getcwd(),
                                  information='Calculating cohesion. Cohesion:' + str(
                                      self.cohesion) + '; community_id:' + str(
                                      self.community_id) + ' Number_i_j:' + str(self.number_i_j))
                else:
                    pass
        self.lock.acquire()
        # write_log(log_file_name='network_cohesion_community_thread.log', log_file_path=os.getcwd(),information='################ Community_id: ' + str(self.community_id) + ' is over. cohesion is: ' + str(self.cohesion) + '#####################')
        cohesion_dict = {"community_id":self.community_id,"cohesion": self.cohesion, "number_of_edges": self.number_of_edges, "number_i_j": self.number_i_j, "number_i_j_edge": self.number_i_j_edge}
        cohesion_dict = dict(
            community_id = int(self.community_id),
            cohesion = self.cohesion,
            number_of_edges = int(self.number_of_edges),
            number_i_j = int(self.number_i_j),
            number_i_j_edge = int(self.number_i_j_edge)
        )
        write_log(log_file_name='network_cohesion_community_thread.log', log_file_path=os.getcwd(),information='################ Community_id: ' + str(self.community_id) + ' is over.' + str(cohesion_dict))

        print cohesion_dict
        row = json.dumps(cohesion_dict) + '\n'
        file = open(os.getcwd() + '/' + self.cohesion_file, 'a+')
        file.write(row)
        # file.write(str(cohesion_dict) + '\n')
        file.close()
        self.lock.release()

    def get_community_edges(self):
        """
        根据过滤掉的社区用户，对原始的社区边进行过滤，即：如果社区中的一条边的source和target被过滤掉了，那这一条边也就要被过滤掉。
        :return:经过过滤后与社区相对应的网络边的DataFrame，格式为(source, target, weight).
        """
        print 'getting edges of this community. community id:', self.community_id
        # time.sleep(5)
        community_edges_dataFrame = pd.DataFrame()
        user_id_list = list(set(list(self.node_dataFrame.user_id)))
        if type(self.edge_dataFrame) == dict:
            for user in user_id_list:
                user_hash_number = hash(str(user)) % self.hash_size
                edge_dataFrame = self.edge_dataFrame[user_hash_number]
                condidate_edges = edge_dataFrame[edge_dataFrame.source == user]
                for target in condidate_edges.target:
                    if target in user_id_list:
                        community_edges_dataFrame = community_edges_dataFrame.append(condidate_edges[condidate_edges.target == target], ignore_index=False)
                    else:
                        pass
        else:
            for user in user_id_list:
                condidate_edges = self.edge_dataFrame[self.edge_dataFrame.source == user]
                for target in condidate_edges.target:
                    if target in user_id_list:
                        community_edges_dataFrame = community_edges_dataFrame.append(condidate_edges[condidate_edges.target == target], ignore_index=False)
                    else:
                        pass

        self.edge_dataFrame = community_edges_dataFrame
        self.number_of_edges = len(community_edges_dataFrame)

    def calculate_degree(self):
        """
        计算网络节点的出度、入度、度，根据与该节点文件对应的边文件。
        :return:返回一个有出入度的节点DataFrame。
        """
        print 'calculating degree for each node in this community. community id:',self.community_id
        print self.node_dataFrame.head()
        # time.sleep(5)
        print self.edge_dataFrame.head()
        if self.number_of_edges == len(self.edge_dataFrame):
            pass
        else:
            self.get_community_edges()
        degree_list = ['out_degree', 'in_degree','degree']
        for degree in degree_list:
            if degree in self.node_dataFrame.columns:
                del self.node_dataFrame[degree]
            else:
                pass
        self.node_dataFrame['out_degree'] = 0
        self.node_dataFrame['in_degree'] = 0
        self.node_dataFrame['degree'] = 0
        print self.node_dataFrame.head()
        user_id_list = list(set(list(self.node_dataFrame['user_id'])))
        index = 0
        for user_id in user_id_list:
            index = index + 1
            out_degree = len(self.edge_dataFrame[self.edge_dataFrame.source == user_id])
            in_degree = len(self.edge_dataFrame[self.edge_dataFrame.target == user_id])
            self.node_dataFrame.loc[[user_id], ['out_degree']] = out_degree
            self.node_dataFrame.loc[[user_id], ['in_degree']] = in_degree
            self.node_dataFrame.loc[[user_id], ['degree']] = in_degree + out_degree
            print '***', index, user_id, 'degree:', in_degree + out_degree, 'out_degree:', out_degree, 'in_degree:', in_degree


    def wether_interaction_between_nodes(self, node_i, node_j):
        """
        判断从节点 i 到节点 j 是否存在一个有向边。分两种情况，全部网络和部分社区。
        :param node_i:节点 i。也即是用户id。
        :param node_j: 节点 j。即用户id。
        :return:存在一个有向边：1，否者：0.
        """
        if type(self.edge_dataFrame) == dict:
            node_i_number = hash(str(node_i)) % self.hash_size
            temp_edge_dataFrame_i = self.edge_dataFrame[node_i_number]
            if node_j not in list((temp_edge_dataFrame_i[temp_edge_dataFrame_i.source == node_i]).target):
                return 0.0
            else:
                return 1.0
        else:
            if node_j not in list((self.edge_dataFrame[self.edge_dataFrame.source == node_i]).target):
                return 0.0
            else:
                return 1.0


def calculate_cohesion_whole_network(file_path, node_file_name, cohesion_type, cohesion_file, edge_file_name=None, edge_file_path=None,
                                     edge_file_key_word_list=None):
    """
    计算网络的聚合度，这里讲将边文件进行hash处理，以便快速查询。对于点文件，因为不同社区的聚合度为0，所以这里按照社区进行。一个一个考虑。
    这里分为整个网络和部分社区。整个网络需要将变文件hash存储。部分社区不需要。
    :param file_path:节点文件所在的路径，如果是针对部分社区的，也是边文件所在的路径
    :param node_file_name:节点文件名称，第一行是列名。无论整个网络还是部分社区，都是统一格式(user_id,community_id, out_degree,in_degree,degree)
    :param cohesion_type:  处理的类型，是针对整个网络（’whole‘），还是针对部分社区（’community‘）。如果是针对的部分社区，需要对应的社区文件。
    :param edge_file_name:如果针对的是部分社区，该文件为边文件。且第一行为列名(source, target, number_of_interaction, weight)
    :param edge_file_path: 如果是针对整个网络，该路径为存储hash边文件的路径。
    :param edge_file_key_word_list: 如果是针对整个网络的话，
    :return:返回计算的结果。
    """

    node_dataFrame = pd.read_csv(file_path + node_file_name, dtype={'user_id': np.str})
    node_dataFrame.index = node_dataFrame.user_id
    if cohesion_type == 'hash':
        edge_file_list = get_dirlist(path=edge_file_path, key_word_list=edge_file_key_word_list)
        print len(edge_file_list)
        print edge_file_list
        # time.sleep(20)
        edge_dataFrame_dict = {}
        for edge_file_name in edge_file_list:
            number = int((edge_file_name.split('hash_')[-1]).split('.')[0])
            edge_dataFrame_dict[number] = pd.read_csv(edge_file_path + edge_file_name,
                                                      dtype={'source': np.str, 'target': np.str})
    else:
        edge_dataFrame_dict = pd.read_csv(file_path + edge_file_name, header=0,
                                          dtype={'source': np.str, 'target': np.str})

    lock = threading.Lock()

    community_id_list = list(set(list(node_dataFrame.community_id)))
    print 'number 0f community:', len(community_id_list)
    # time.sleep(10)
    thread_list = []
    for community_id in community_id_list:
        community_node_dataFrame = node_dataFrame[node_dataFrame.community_id == community_id]
        thread = cohesionThread(edge_dataFrame=edge_dataFrame_dict, node_dataFrame=community_node_dataFrame,
                                community_id=community_id,cohesion_file = cohesion_file,lock=lock)
        thread.start()
        thread_list.append(thread)
        # break

    for thread in thread_list:
        thread.join()


def main():
    file_path = 'D:/LiuQL/eHealth/twitter/visualization/node_edge/'
    node_file_name = 'labeled_community_nodes_2000.csv'
    cohesion_type = 'hash'
    edge_file_name = 'community_edges_2000.csv'
    edge_file_path = 'D:/test/edge_hash/'
    edge_file_key_word_list = ['edge_hash_', '.csv']

    # Dubai server
    # file_path ='/pegasus/harir/Qianlong/data/network/'
    # node_file_name ='total_node_degree.csv'
    # cohesion_type = 'hash'
    # edge_file_name = 'total_edge_weight.csv'
    # edge_file_path = '/pegasus/harir/Qianlong/data/network/edge_hash/'
    # edge_file_key_word_list = ['edge_hash_','.csv']

    write_log(log_file_name='network_cohesion_community_thread.log', log_file_path=os.getcwd(),
              information='*' * 50 + 'Starting Calculate cohesion' + '*' * 50)
    cohesion_file = 'cohesion_community_thread.json'
    file = open(os.getcwd() + '/' + cohesion_file, 'wb')
    file.close()
    calculate_cohesion_whole_network(file_path=file_path, node_file_name=node_file_name,
                                                 cohesion_type=cohesion_type, cohesion_file = cohesion_file,edge_file_name=None,
                                                 edge_file_path=edge_file_path,
                                                 edge_file_key_word_list=edge_file_key_word_list)
    write_log(log_file_name='network_cohesion_community_thread.log', log_file_path=os.getcwd(),
              information='*' * 20 + 'Program Done' + '*' * 20 + '\n' * 4)


main()

