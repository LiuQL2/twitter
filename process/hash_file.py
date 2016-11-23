#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
按照杨师兄的思路进行，为每一条记录寻找其最原始的root的tweet。
"""

import pandas as pd
import time
import os
import sys
import csv
reload(sys)
sys.setdefaultencoding('utf-8')

def hash_file():
    data_path = 'G:/Twitter/data/data_yang/'
    save_path = 'D:/LiuQL/eHealth/twitter/data/data_hash/'
    # data_path = '/pegasus/harir/yangjinfeng/hash_tracker_2/'
    # save_path = '/pegasus/harir/Qianlong/data/hash/'
    name_list = os.listdir(data_path)
    file_name_list = []
    print name_list
    for name in name_list:
        if 'hash_tracker' in name:
            file_name_list.append(name)
    print file_name_list
    writer_list = []
    file_list = []
    for index in range(0,100, 1):
        file = open(save_path + 'hash_qianlong_'+ str(index)+ '.csv','wb' )
        file_list.append(file)
        writer_list.append(csv.writer(file))
    file_index = 0
    for file_name in file_name_list:
        index = 0
        file_index = file_index + 1
        file = open(data_path + file_name, 'r')
        reader = csv.reader(file)
        for line in reader:
            index = index + 1

            number = hash(line[0]) % 100
            print file_index,index,line
            writer_list[number].writerow(line)

    for file in file_list:
        file.close()


hash_file()