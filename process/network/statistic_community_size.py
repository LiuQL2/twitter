# !/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utility.functions import get_dirlist
from utility.functions import read_csv_as_dataFrame_by_chunk
import csv


def get_community_nodes(userId_communityId_file_path_name,community_size_file,path_save_to,sep, names):
    """
    根据(user_id, community_id)格式的文件统计每一个社区里面的人数，保存的文件格式为（community_id, number_of_user).
    :param path:(user_id, community_id)w文件的存储位置，以及（community_id, number_of_user)文件的保存位置。
    :param file_name:(user_id, community_id)文件的名称。
    :param community_size_file:(community_id number_of_user)文件的保存名称。
    :param sep:(user_id, community_id)数据之间的分隔符。
    :param names:以pandas数据框的格式读取上述文件，其中列的名称。
    :return:无返回内容。
    """
    nodes_dataFrame = pd.read_csv(userId_communityId_file_path_name,index_col=False, sep = sep,header=None, names = names,dtype = {'user_id':np.str,'community':np.int32})
    print 'get it'
    print nodes_dataFrame
    community_list = set(list(nodes_dataFrame.community))
    community_file = open(path_save_to + community_size_file, 'wb')
    writer = csv.writer(community_file)
    writer.writerow(['community_id', 'number_of_user'])
    for community in community_list:
        number =  len(nodes_dataFrame[nodes_dataFrame.community == community])
        writer.writerow([community,number])
    community_file.close()



userId_communityId_file_path = '/pegasus/harir/yangjinfeng/commitresult/community2/'
path_save_to = '/pegasus/harir/Qianlong/data/network/community_size/'
userId_communityId_file_list = get_dirlist(userId_communityId_file_path,key_word_list=['node-com.txt'])
for file in userId_communityId_file_list:
    community_size_file_name = file.replace('.icpm.node-com.txt', '_communityId_size.csv')
    get_community_nodes(userId_communityId_file_path_name=userId_communityId_file_path+file,path_save_to=path_save_to,community_size_file = community_size_file_name,sep = ',', names = ['user_id','community'])