# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
过滤掉认证的用户，筛选需要进行可视化的社区，去掉与认证用户相关的边，
需要两个文件:
所有边文件（source, target, weight)。csv格式，中间用逗号隔开，无header
所有节点所属社区的文件(user_id, community_id)。csv格式，目前是用空格隔开。以后改为逗号隔开，无header。
"""
import pandas as pd
import numpy as np
from utility.functions import get_dirlist
from utility.functions import read_csv_as_dataFrame_by_chunk
import csv

class communityNetwork(object):
    def __init__(self,userId_communityId_path_file, total_edge_path_file,community_size, community_number,sep = ',',header = None):
        self.user_community_path_file = userId_communityId_path_file
        self.total_edge_path_file = total_edge_path_file
        self.community_size = community_size
        self.community_number = community_number
        self.sep = sep
        self.header = header



    def get_community_nodes(self):
        # self.nodes_dataFrame = pd.read_csv(self.user_community_path_file, index_col=False, sep=self.sep, header=self.header,names=['id', 'community_id'], dtype={'id': np.str})
        self.nodes_dataFrame = pd.read_csv(self.user_community_path_file, index_col=False, sep=' ', header=self.header,names=['id', 'community_id'], dtype={'id': np.str})
        print self.nodes_dataFrame.head()
        community_list = set(list(self.nodes_dataFrame.community_id))
        keep_community_list = []
        for community in community_list:
            if len(self.nodes_dataFrame[self.nodes_dataFrame.community_id == community]) > self.community_size:
                keep_community_list.append(community)
            else:
                pass
            if len(keep_community_list) == self.community_number:
                break
            else:
                pass
        print 'number of community:', len(keep_community_list)
        keep_nodes_dataFrame = pd.DataFrame()
        for community in keep_community_list:
            keep_nodes_dataFrame = keep_nodes_dataFrame.append(
                self.nodes_dataFrame[self.nodes_dataFrame.community_id == community], ignore_index=False)
        print 'number of users:', len(keep_nodes_dataFrame)
        print keep_community_list
        self.community_nodes_dataFrame = keep_nodes_dataFrame
        return keep_nodes_dataFrame


    def filter_verified_user(self,verified_user_path_file,sep = ',',header = None):
        """
        根据已经认证的用户文件，过滤到保留社区中的认证用户。
        :return: 过滤掉认证用户之后的pandas数据框，格式与community_user_dataFrame相同。列名（user_id, community_id）。
        """
        print 'fileter verified user'
        verified_user_dataFrame = pd.read_csv(verified_user_path_file, names=['id', 'is_verified', 'name'],
                                              dtype={'id': np.str}, sep=sep, header=header)
        verified_user_dataFrame = verified_user_dataFrame[verified_user_dataFrame.is_verified == True]
        del verified_user_dataFrame['is_verified']
        del verified_user_dataFrame['name']

        dataFrame = pd.DataFrame()
        user_id_list = set(list(self.community_nodes_dataFrame.id))
        verified_user_id_list = list(verified_user_dataFrame.id)
        for user_id in user_id_list:
            if user_id not in verified_user_id_list:
                dataFrame = dataFrame.append(self.community_nodes_dataFrame[self.community_nodes_dataFrame.id == user_id],
                                             ignore_index=False)
                print 'keep user: ', user_id
            else:
                print 'delete user: ', user_id
                pass
        self.community_nodes_dataFrame = dataFrame
        return dataFrame


    def get_community_edges(self,wether_hash = True, hash_size = 100):
        """
        根据过滤掉的社区用户，对原始的社区边进行过滤，即：如果社区中的一条边的source和target被过滤掉了，那这一条边也就要被过滤掉。
        :return:经过过滤后与社区相对应的网络边的DataFrame，格式为(source, target, weight).
        """
        self.edge_dataFrame = pd.read_csv(self.total_edge_path_file, index_col=False, sep = self.sep,names = ['source','target','weight'],dtype = {'source':np.str,'target':np.str})
        if wether_hash:
            self.edge_dataFrame = self.__hash_dataFrame__(column= 'source', hash_size=hash_size)
        else:
            pass
        community_edges_dataFrame = pd.DataFrame()
        user_id_list = list(set(list(self.community_nodes_dataFrame.id)))
        for source in user_id_list:
            if type(self.edge_dataFrame) == dict:
                hash_number = hash(str(source)) % hash_size
                condidate_edges = self.edge_dataFrame[hash_number][self.edge_dataFrame[hash_number].source == source]
            else:
                condidate_edges = self.edge_dataFrame[self.edge_dataFrame.source == source]
            for target in condidate_edges.target:
                if target in user_id_list:
                    community_edges_dataFrame = community_edges_dataFrame.append(
                        condidate_edges[condidate_edges.target == target])
                    print 'get community_edges', 'keep', source, target
                else:
                    print 'get community_edges', 'filter', source, target
        self.community_edges_dataFrame = community_edges_dataFrame
        return community_edges_dataFrame

    def __calculate_degree__(self):
        """
        计算网络节点的出度、入度、度，根据与该节点文件对应的边文件。
        """
        self.community_nodes_dataFrame.index = self.community_nodes_dataFrame.id
        self.community_nodes_dataFrame['out_degree'] = 0
        self.community_nodes_dataFrame['in_degree'] = 0
        self.community_nodes_dataFrame['degree'] = 0
        print self.community_nodes_dataFrame.head()
        user_id_list = list(set(list(self.community_nodes_dataFrame['id'])))
        index = 0
        for user_id in user_id_list:
            index = index + 1
            out_degree = len(self.community_edges_dataFrame[self.community_edges_dataFrame.source == user_id])
            in_degree = len(self.community_edges_dataFrame[self.community_edges_dataFrame.target == user_id])
            self.community_nodes_dataFrame.loc[[user_id], ['out_degree']] = out_degree
            self.community_nodes_dataFrame.loc[[user_id], ['in_degree']] = in_degree
            self.community_nodes_dataFrame.loc[[user_id], ['degree']] = in_degree + out_degree
            print '***', index, user_id, 'degree:', in_degree + out_degree, 'out_degree:', out_degree, 'in_degree:', in_degree

    def label_nodes(self,top_node_size,label_path_file, sep=',',header = None):
        """
        根据节点的出入度，对度为top的节点进行打标签。
        :param label_path_file:节点标签文件路径及其名称。格式为(user_id, verify, name),csv文件，以sep分隔，无header
        :param sep:标签文件中的分隔符。
        :param header:标签文件第一行是否是列名。
        """
        if 'out_degree' not in self.community_nodes_dataFrame.columns:
            self.__calculate_degree__()
        else:
            pass

        self.community_nodes_dataFrame['label'] = None
        id_label_dataFrame = pd.read_csv(label_path_file, names=['id', 'is_verified', 'label'], header=header,
                                         dtype={'id': np.str}, sep=sep)
        id_label_dataFrame.index = id_label_dataFrame.id
        community_id_list = set(list(self.community_nodes_dataFrame.community_id))
        for community_id in community_id_list:
            print 'community id:', community_id, 'is being label...'
            single_community_node_dataFrame = (self.community_nodes_dataFrame[self.community_nodes_dataFrame.community_id == community_id]).sort_values(by=['out_degree'], ascending=[0])
            top_id_list = list(single_community_node_dataFrame.id)[0:top_node_size]
            for id in top_id_list:
                self.community_nodes_dataFrame.loc[[id], ['label']] = id_label_dataFrame.label[id]
                print id, id_label_dataFrame.label[id]

    def __hash_dataFrame__(self, column, hash_size):
        """
        对一个节点的数据框进行hash处理，以提高在过滤网络边的速度。
        :param column:指定以哪一列进行hash处理。
        :param hash_size:最终想保存的份数是多少。
        :return:返回一个字典的类型
        """
        hash_dataFrame_dict = {}
        for index in range(0, hash_size, 1):
            hash_dataFrame_dict[index] = pd.DataFrame()
        index_list = set(list(self.edge_dataFrame[column]))
        for index in index_list:
            print 'hashing', index
            hash_number = hash(str(index)) % 100
            hash_dataFrame_dict[hash_number] = hash_dataFrame_dict[hash_number].append(
                self.edge_dataFrame[self.edge_dataFrame[column] == index])
        return hash_dataFrame_dict


if __name__ == '__main__':
    print 'yes'

    path = 'D:/LiuQL/eHealth/twitter/visualization/network/'
    userId_communityId_file = 'userId_communityId_2016-11-25.txt'
    total_edge_file = 'total_edge_weight.csv'
    verified_user_file = 'user_verified_long.csv'
    id_label_file = 'user_all_yang.csv'
    community_size = 2000
    commnnity_number = 8

    community_network = communityNetwork(userId_communityId_path_file= path + userId_communityId_file,total_edge_path_file= path + total_edge_file,community_size=community_size,community_number=commnnity_number)
    community_network.get_community_nodes()
    community_network.get_community_edges()
    community_network.filter_verified_user(verified_user_path_file= path + verified_user_file)
    community_network.label_nodes(top_node_size=20,label_path_file= path + id_label_file)
    community_network.community_nodes_dataFrame.to_csv(path + 'community_nodes.csv',index = False, header = True, columns = ['id','community_id','label'])
    community_network.community_edges_dataFrame.to_csv(path + 'community_edges.csv', index = False, header= True, columns= ['source','target','weight'])
