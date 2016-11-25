# !/usr/bin/env python
# -*- encode: utf-8 -*-
import pandas as pd
from utility.functions import get_dirlist
import csv

def read_file(path):
    file_name_list = get_dirlist(path = path,key_word_list=['.icpm'])
    df_list = []
    for file_name in file_name_list:
        file = open(path + file_name,'r')
        reader = csv.reader(file)
        index = 0
        for line in reader:
            index = index + 1
            if index > 2:
                break
            user_id_list = line[0].split(' ')
            user_id_list.pop()
            df_list.append(user_id_list)
            print len(user_id_list)
        file.close()
    df = pd.DataFrame(data=df_list).T
    df.columns = ['community_1', 'community_2']
    print df[df.community_1 == df.community_2]
    print file_name_list



def total_network_hash(toal_network_path,file_name):
    total_network_file = open(toal_network_path + file_name, 'r')
    reader = csv.reader(total_network_file)




read_file('D:/LiuQL/eHealth/twitter/')