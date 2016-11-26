# !/usr/bin/env python
# -*- encode: utf-8 -*-
import pandas as pd
import numpy as np
from utility.functions import get_dirlist
from utility.functions import read_csv_as_dataFrame_by_chunk
import csv


def read_nodes_file(path, file_name,community_size):
    # nodes_dataFrame = read_csv_as_dataFrame_by_chunk(path = path, file_name=file_name,sep= ' ')
    nodes_dataFrame = pd.read_csv(path + file_name,index_col=False, sep = ' ',header=None, names = ['user_id','community'],dtype = {'user_id':np.str,'community':np.int32})
    print 'get it'
    print nodes_dataFrame
    community_list = set(list(nodes_dataFrame.community))
    keep_community_list = []
    for community in community_list:
        if len(nodes_dataFrame[nodes_dataFrame.community == community]) > community_size:
            keep_community_list.append(community)
        else:
            pass
    keep_nodes_dataFrame = pd.DataFrame()
    for community in keep_community_list:
        keep_nodes_dataFrame = keep_nodes_dataFrame.append(nodes_dataFrame[nodes_dataFrame.community == community],ignore_index=False)
    keep_nodes_dataFrame.to_csv(path + 'community_nodes.csv',index = False,header=False)
    print len(keep_nodes_dataFrame)
    print keep_community_list
    return keep_nodes_dataFrame



def read_edges_file(path ,edge_file, hash_node_dataFrame):
    print hash_node_dataFrame
    total_edges_dataFrame = pd.read_csv(path + edge_file,names = ['source','target','weight'],dtype = {'source':np.str,'target':np.str,'weight':np.int32}, sep = '\t')
    print total_edges_dataFrame
    community_edges_dataFrame = pd.DataFrame()
    for index in total_edges_dataFrame.index:
        if check_community_edges(source=total_edges_dataFrame.source[index], target=total_edges_dataFrame.target[index], hash_node_dataFrame=hash_node_dataFrame,hash_size=100,column_name='user_id'):
            community_edges_dataFrame = community_edges_dataFrame.append(total_edges_dataFrame.loc[index],ignore_index=False)

    community_edges_dataFrame.to_csv(path + 'community_edges.csv')




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



def filter_nodes_edges(path, node_file, edge_file):
    node_dataFrame = read_nodes_file(path = path,file_name = node_file,community_size=8)
    hash_node_dataFrame_dict = hash_dataFrame(dataFrame=node_dataFrame,column='user_id', hash_size = 100)
    read_edges_file(path = path,edge_file = edge_file,hash_node_dataFrame=hash_node_dataFrame_dict )
# read_file('D:/LiuQL/eHealth/twitter/')
# read_nodes_file('D:/LiuQL/eHealth/twitter/visualization/',file_name='total_nodes.txt',community_size = 2000)
# read_edges_file('D:/LiuQL/eHealth/twitter/visualization/',file_name='total_nodes.txt',node_dataFrame=0)


filter_nodes_edges('D:/LiuQL/eHealth/twitter/visualization/',node_file='total_nodes.txt',edge_file='total_network')