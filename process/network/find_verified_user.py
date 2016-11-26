#!/usr/bin/python env
# -*- coding: utf-8 -*-

import json
import csv
import os
import numpy as np
import time
import pandas as pd
from collections import OrderedDict
from utility.functions import get_dirlist
from utility.functions import write_log


def find_user(path_data,path_save_to,file_save_to):
    file_name_list = get_dirlist(path = path_data, key_word_list=['part-r','.json','33b34d49'],no_key_word_list=['crc'])
    print len(file_name_list)
    time.sleep(40)
    file_save = open(path_save_to + file_save_to,'wb')
    file_writer = csv.writer(file_save)
    print file_name_list
    file_index = 0
    for file_name in file_name_list:
        file_index = file_index + 1
        file = open(path_data + file_name, 'r')
        write_log(log_file_name='find_verified_user.log',log_file_path=os.getcwd(),information='file index:' + str(file_index) + ' is being processing.')
        for line in file:
            try:
                print len(line)
                row = json.loads(line,object_pairs_hook=OrderedDict)
                actor = [row['actor']['id'], row['actor']['verified'], row['actor']['preferredUsername']]
                file_writer.writerow(actor)
                print 'file index:', file_index, actor
                if row['type'] == 'retweet':
                    origin_actor = [row['originActor']['id'], row['originActor']['verified'], row['originActor']['preferredUsername']]
                    file_writer.writerow(origin_actor)
                else:
                    pass
            except:
                print file_index, '*' * 100
                pass
        file.close()
    file_save.close()

def drop_duplicate_user(path_data,path_save_to, actor_file,all_user_file,verified_user_file):
    user_dataFrame = pd.read_csv(path_data + actor_file,names = ['user_id','isverified','preferred_username'],dtype={'user_id':np.str},header=None)
    user_dataFrame = user_dataFrame.drop_duplicates()
    user_verified_dataFrame = user_dataFrame[user_dataFrame.isverified == True]
    user_dataFrame.to_csv(path_save_to + all_user_file,index = False,header=False)
    user_verified_dataFrame.to_csv(path_save_to + verified_user_file,index=False,header=False)
    print 'all user:\n',user_dataFrame
    print 'verified user:\n',user_verified_dataFrame


def main():
    write_log(log_file_name='find_verified_user.log', log_file_path=os.getcwd(),
              information='###################program start#####################.')
    path_data = 'D:/LiuQL/eHealth/twitter/data/data_origin/'
    path_save_to = 'D:/LiuQL/eHealth/twitter/data/data_origin/'
    # path_data = '/pegasus/twitter-p-or-t-uae-201603.json.dxb/'
    # path_save_to ='/pegasus/harir/Qianlong/data/network/'

    duplicate_user_file = 'user_contain_duplicates.txt'
    all_user_file = 'user_all.txt'
    verified_user_file = 'user_verified.txt'
    find_user(path_data = path_data, path_save_to = path_save_to,file_save_to=duplicate_user_file)
    drop_duplicate_user(path_data = path_save_to, path_save_to = path_save_to, actor_file=duplicate_user_file,all_user_file=all_user_file,verified_user_file=verified_user_file)
    write_log(log_file_name='find_verified_user.log', log_file_path=os.getcwd(),
              information='###################program finished#####################.' + '\n' * 5)

main()