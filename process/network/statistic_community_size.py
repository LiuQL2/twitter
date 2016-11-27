# !/usr/bin/env python
# -*- encode: utf-8 -*-
import pandas as pd
import numpy as np
from utility.functions import get_dirlist
from utility.functions import read_csv_as_dataFrame_by_chunk
import csv


def get_community_nodes(path, file_name,community_size_file,sep, names):
    nodes_dataFrame = pd.read_csv(path + file_name,index_col=False, sep = sep,header=None, names = names,dtype = {'user_id':np.str,'community':np.int32})
    print 'get it'
    print nodes_dataFrame
    community_list = set(list(nodes_dataFrame.community))
    community_file = open(path + community_size_file, 'wb')
    writer = csv.writer(community_file)
    writer.writerow(['community_id', 'number_of_user'])
    for community in community_list:
        number =  len(nodes_dataFrame[nodes_dataFrame.community == community])
        writer.writerow([community,number])
    community_file.close()


get_community_nodes(path ='D:/LiuQL/eHealth/twitter/visualization/' , file_name = 'userID_communityID_2016-11-25.txt',community_size_file = 'community_size.scv',sep = ' ', names = ['user_id','community'])