# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
过滤掉认证的用户，筛选需要进行可视化的社区，去掉与认证用户相关的边，
需要两个文件:
所有边文件（source, target, number_of_interaction, weight)。csv格式，中间用逗号隔开，无header
所有节点所属社区的文件(user_id, community_id)。csv格式，目前是用空格隔开。以后改为逗号隔开，无header。
"""
import pandas as pd
import numpy as np
from utility.functions import get_dirlist
from utility.functions import read_csv_as_dataFrame_by_chunk
import csv


def get_community_nodes(path, file_name,community_size,number_of_community, names,sep = ',',header = None):
    """
    根据(user_id, community_id)文件来进行社区的过滤与筛选。
    :param path: (user_id, community_id)文件的保存路径。
    :param file_name: (user_id, community_id)文件的名称。需要以CSV的格式进行存储。
    :param community_size: 需要保留的社区的人数
    :param number_of_community:需要保留的社区的个数
    :param sep:(user_id, community_id)文件之间的分隔符，用来将数据读取到pandas的DataFrame中。
    :param names:(user_id, community_id)文件数据读取到pandas中列的名称。
    :return:一个pandas的DataFrame，格式为(user_id, community_id）。
    """
    nodes_dataFrame = pd.read_csv(path + file_name,index_col=False, sep = sep,header=header, names = ['user_id', 'community_id'],dtype = {'user_id':np.str,'community_id':np.int32})
    print 'get it'
    print nodes_dataFrame
    community_list = set(list(nodes_dataFrame.community_id))
    keep_community_list = []
    for community in community_list:
        if len(nodes_dataFrame[nodes_dataFrame.community_id == community]) > community_size:
            keep_community_list.append(community)
        else:
            pass

        if len(keep_community_list) == number_of_community:
            break
        else:
            pass
    print len(keep_community_list)
    keep_nodes_dataFrame = pd.DataFrame()
    for community in keep_community_list:
        keep_nodes_dataFrame = keep_nodes_dataFrame.append(nodes_dataFrame[nodes_dataFrame.community_id == community],ignore_index=False)
    print len(keep_nodes_dataFrame)
    print keep_community_list
    return keep_nodes_dataFrame


def filter_verified_user(path, community_user_dataFrame,verified_user_file,sep = ',',header = None):
    """
    根据已经认证的用户文件，过滤到保留社区中的认证用户。
    :param path:认证用户文件的保存路径。
    :param community_user_dataFrame:社区用户数据框，两列，列名（user_id, community_id）。
    :param verified_user_file:认证用户的文件，为CSV文件，格式为(user_id, is_verified, name)，分隔符为逗号。
    :return: 过滤掉认证用户之后的pandas数据框，格式与community_user_dataFrame相同。列名（user_id, community_id）。
    """
    print 'fileter verified user'
    verified_user_dataFrame = pd.read_csv(path + verified_user_file, names=['user_id', 'is_verified', 'name'],dtype={'user_id': np.str},sep = sep,header = header)
    verified_user_dataFrame = verified_user_dataFrame[verified_user_dataFrame.is_verified == True]
    del verified_user_dataFrame['is_verified']
    del verified_user_dataFrame['name']

    dataFrame = pd.DataFrame()
    user_id_list = set(list(community_user_dataFrame.user_id))
    verified_user_id_list = list(verified_user_dataFrame.user_id)
    for user_id in user_id_list:
        if user_id not in verified_user_id_list:
            dataFrame = dataFrame.append(community_user_dataFrame[community_user_dataFrame.user_id == user_id],ignore_index=False)
            print 'keep user: ', user_id
        else:
            print 'delete user: ', user_id
            pass
    return dataFrame


def get_community_edges(path ,edge_file, node_dataFrame,sep = ','):
    """
    根据过滤掉的社区用户，对原始的社区边进行过滤，即：如果社区中的一条边的source和target被过滤掉了，那这一条边也就要被过滤掉。
    :param path:网络边文件的存储路径。
    :param edge_file:网络边文件。格式为(source, target, number_of_interaction,weight)，其中以sep进行分割。这里也就是所有边数据的文件，也即total_network。
    :param node_dataFrame:经过hash之后的节点DataFrame。Note：这实际上是一个字典，其中key值是hash可能出现的值，每一个元素是一个DataFrame。
    :param sep:网络边edge_file文件的分隔符。
    :return:经过过滤后与社区相对应的网络边的DataFrame，格式为(source, target, weight).
    """
    print node_dataFrame
    total_edges_dataFrame = pd.read_csv(path + edge_file,sep = sep,names = ['source','target','number_of_interaction','weight'],dtype = {'source':np.str,'target':np.str})
    print total_edges_dataFrame

    community_edges_dataFrame = pd.DataFrame()
    if type(node_dataFrame) == dict:
        for index in total_edges_dataFrame.index:
            if check_community_edges(source=total_edges_dataFrame.source[index],
                                     target=total_edges_dataFrame.target[index],
                                     node_dataFrame=node_dataFrame, hash_size=100, column_name='user_id'):
                community_edges_dataFrame = community_edges_dataFrame.append(total_edges_dataFrame.loc[index],
                                                                             ignore_index=False)
                print 'get community_edges', 'keep', index
            else:
                print 'get community_edges', 'filter', index
        pass
    else:
        user_id_list = list(set(list(node_dataFrame.user_id)))
        for source in user_id_list:
            condidate_edges = total_edges_dataFrame[total_edges_dataFrame.source == source]
            for target in condidate_edges.target:
                if target in user_id_list:
                    community_edges_dataFrame = community_edges_dataFrame.append(condidate_edges[condidate_edges.target == target])
                    print 'get community_edges', 'keep', source, target
                else:
                    print 'get community_edges', 'filter', source,target

    return community_edges_dataFrame



def hash_dataFrame(dataFrame,column,hash_size):
    """
    对一个节点的数据框进行hash处理，以提高在过滤网络边的速度。
    :param dataFrame:有节点信息的pandas的DataFrame。
    :param column:指定以哪一列进行hash处理。
    :param hash_size:最终想保存的份数是多少。
    :return:返回一个字典的类型
    """
    hash_dataFrame_dict = {}
    for index in range(0,hash_size,1):
        hash_dataFrame_dict[index] = pd.DataFrame()
    index_list = set(list(dataFrame[column]))
    for index in index_list:
        print 'hashing',index
        hash_number = hash(str(index)) % 100
        hash_dataFrame_dict[hash_number] = hash_dataFrame_dict[hash_number].append(dataFrame[dataFrame[column] == index])
    return hash_dataFrame_dict


def check_community_edges(source, target, node_dataFrame,hash_size,column_name):
    """
    检验一条有向边(source, target)边的两端节点是不是在保留的社区中。
    :param source:有向边的起始点。
    :param target:有向边的终点。
    :param node_dataFrame:经过hash存储之后的节点数据。
    :param hash_size:经过hash存储之后的hash数量。
    :param column_name:   node_dataFrame中节点所在的列名。
    :return:两个节点都在社区节点中：True， 否则：False。
    """
    source_hash_number = hash(str(source)) % hash_size
    target_hash_number = hash(str(target)) % hash_size
    if source in list(node_dataFrame[source_hash_number][column_name]) and target in list(node_dataFrame[target_hash_number][column_name]):
        return True
    else:
        return False



def main(path, node_file, edge_file,verified_user_file,community_size, number_of_community):
    """
    调用上面函数，完成社区的筛选，完成认证用户及其相关边的过滤。
    :param path: 相应文件保存的路径。
    :param node_file:节点社区文件，格式(user_id, community_id)。
    :param edge_file:边文件，(source,target,number_of_interaction, weight).逗号分隔
    :param verified_user_file:经过认证的用户文件(user_id, verify, name)，逗号分隔
    :param community_size:保留社区的大小
    :param number_of_community:保留社区的个数
    :return:无返回内容。
    """
    community_nodes_dataFrame = get_community_nodes(path = path,file_name = node_file,community_size=community_size,number_of_community=number_of_community,sep = ' ',names=['user_id','community'],header=None)
    community_nodes_dataFrame = filter_verified_user(path = path,community_user_dataFrame=community_nodes_dataFrame,verified_user_file=verified_user_file,header=None)
    # community_nodes_dataFrame = hash_dataFrame(dataFrame=community_nodes_dataFrame,column='user_id', hash_size = 100)
    community_edges_dataFrame =get_community_edges(path = path,sep = ',',edge_file = edge_file,node_dataFrame=community_nodes_dataFrame)
    community_nodes_dataFrame.to_csv(path + 'community_nodes.csv',index = False, header=False,columns=['user_id', 'community_id'])
    # community_nodes_dataFrame.to_csv(path + 'community_nodes.csv',index = False, header=False)
    community_edges_dataFrame.to_csv(path + 'community_edges.csv',index = False, header=False, columns=['source','target','number_of_interaction','weight'])
    # community_edges_dataFrame.to_csv(path + 'community_edges.csv',index = False, header=False)


main('D:/LiuQL/eHealth/twitter/visualization/network/',
     node_file='userId_communityId_2016-11-25.txt',
     edge_file='total_edge_weight.csv',
     verified_user_file='user_verified_long.csv',
     community_size=2000,
     number_of_community=8)