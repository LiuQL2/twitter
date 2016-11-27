# !/usr/bin/env python
# -*- encode: utf-8 -*-

"""

"""
import pandas as pd
import numpy as np
from utility.functions import get_dirlist
from utility.functions import read_csv_as_dataFrame_by_chunk
import csv


def get_community_nodes(path, file_name,community_size,sep, names):
    # nodes_dataFrame = read_csv_as_dataFrame_by_chunk(path = path, file_name=file_name,sep= ' ')
    nodes_dataFrame = pd.read_csv(path + file_name,index_col=False, sep = sep,header=None, names = names,dtype = {'user_id':np.str,'community':np.int32})
    print 'get it'
    print nodes_dataFrame
    community_list = set(list(nodes_dataFrame.community))
    keep_community_list = []
    for community in community_list:
        if len(nodes_dataFrame[nodes_dataFrame.community == community]) > community_size:
            keep_community_list.append(community)
        else:
            pass
    print len(keep_community_list)
    keep_nodes_dataFrame = pd.DataFrame()
    for community in keep_community_list:
        keep_nodes_dataFrame = keep_nodes_dataFrame.append(nodes_dataFrame[nodes_dataFrame.community == community],ignore_index=False)
    print len(keep_nodes_dataFrame)
    print keep_community_list
    return keep_nodes_dataFrame

def filter_verified_user(path, community_user_dataFrame,verified_user_file):
    print 'fileter verified user'
    verified_user_dataFrame = pd.read_csv(path + verified_user_file, names=['user_id', 'is_verified', 'name'],dtype={'user_id': np.str})
    del verified_user_dataFrame['is_verified']
    del verified_user_dataFrame['name']
    dataFrame = pd.DataFrame()
    community_user_list = set(list(community_user_dataFrame.user_id))
    for user_id in community_user_list:
        if user_id not in list(verified_user_dataFrame.user_id):
            dataFrame = dataFrame.append(community_user_dataFrame[community_user_dataFrame.user_id == user_id],ignore_index=False)
    return dataFrame



def get_community_edges(path ,edge_file, hash_node_dataFrame):
    print hash_node_dataFrame
    total_edges_dataFrame = pd.read_csv(path + edge_file,names = ['source','target','weight'],dtype = {'source':np.str,'target':np.str,'weight':np.int32}, sep = '\t')
    print total_edges_dataFrame
    community_edges_dataFrame = pd.DataFrame()
    for index in total_edges_dataFrame.index:
        if check_community_edges(source=total_edges_dataFrame.source[index], target=total_edges_dataFrame.target[index],hash_node_dataFrame=hash_node_dataFrame,hash_size=100,column_name='user_id'):
            community_edges_dataFrame = community_edges_dataFrame.append(total_edges_dataFrame.loc[index],ignore_index=False)
            print 'get community_edges', 'keep', index
        else:
            print 'get community_edges', 'filter', index
    return community_edges_dataFrame



def hash_dataFrame(dataFrame,column,hash_size):
    hash_dataFrame_dict = {}
    for index in range(0,hash_size,1):
        hash_dataFrame_dict[index] = pd.DataFrame()
    index_list = set(list(dataFrame[column]))
    for index in index_list:
        print 'hashing',index
        hash_number = hash(str(index)) % 100
        hash_dataFrame_dict[hash_number] = hash_dataFrame_dict[hash_number].append(dataFrame[dataFrame[column] == index])
    return hash_dataFrame_dict


def check_community_edges(source, target, hash_node_dataFrame,hash_size,column_name):
    source_hash_number = hash(str(source)) % hash_size
    target_hash_number = hash(str(target)) % hash_size
    if source in list(hash_node_dataFrame[source_hash_number][column_name]) and target in list(hash_node_dataFrame[target_hash_number][column_name]):
        return True
    else:
        return False



def main(path, node_file, edge_file,verified_user_file):
    community_size = 2000
    community_nodes_dataFrame = get_community_nodes(path = path,file_name = node_file,community_size=community_size,sep = ' ',names=['user_id','community'])
    # community_nodes_dataFrame = filter_verified_user(path = path,community_user_dataFrame=community_nodes_dataFrame,verified_user_file=verified_user_file)
    hash_node_dataFrame_dict = hash_dataFrame(dataFrame=community_nodes_dataFrame,column='user_id', hash_size = 100)
    community_edges_dataFrame =get_community_edges(path = path,edge_file = edge_file,hash_node_dataFrame=hash_node_dataFrame_dict)
    community_nodes_dataFrame.to_csv(path + 'community_nodes_contain_verified_'+ str(community_size) + '.csv',index = False, header=False)
    # community_nodes_dataFrame.to_csv(path + 'community_nodes.csv',index = False, header=False)
    community_edges_dataFrame.to_csv(path + 'community_edges_contain_verified_'+ str(community_size) + '.csv',index = False, header=False)
    # community_edges_dataFrame.to_csv(path + 'community_edges.csv',index = False, header=False)


main('D:/LiuQL/eHealth/twitter/visualization/',node_file='total_nodes.txt',edge_file='total_network',verified_user_file='user_verified.txt')