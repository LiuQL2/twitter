#!/usr/bin/python env
# -*- coding: utf-8 -*-

import json
import csv
import os
import numpy as np
import pandas as pd
from collections import OrderedDict
from utility.functions import get_dirlist
from utility.functions import write_log


def find_user(path,file_save_to):
    file_name_list = get_dirlist(path = path, key_word_list=['part-r','.json','33b34d49','00000'])
    file_save = open(path + file_save_to,'wb')
    file_writer = csv.writer(file_save)
    print file_name_list
    file_index = 0
    for file_name in file_name_list:
        file_index = file_index + 1
        file = open(path + file_name, 'r')
        write_log(log_file_name='find_verified_user.log',log_file_path=os.getcwd(),information='file index:' + str(file_index) + ' is being processing.')
        for line in file:
            row = json.loads(line,object_pairs_hook=OrderedDict)
            actor = [row['actor']['id'], row['actor']['verified'], row['actor']['preferredUsername']]
            file_writer.writerow(actor)
            print 'file index:', file_index, actor
            if row['type'] == 'retweet':
                origin_actor = [row['originActor']['id'], row['originActor']['verified'], row['originActor']['preferredUsername']]
                file_writer.writerow(origin_actor)
            else:
                pass
        file.close()
    file_save.close()

def drop_duplicate_user(path, actor_file,all_user_file,verified_user_file):
    user_dataFrame = pd.read_csv(path + actor_file,names = ['user_id','isverified','preferred_username'],dtype={'user_id':np.str},header=None)
    user_dataFrame = user_dataFrame.drop_duplicates()
    user_verified_dataFrame = user_dataFrame[user_dataFrame.isverified == True]
    user_dataFrame.to_csv(path + all_user_file,index = False,header=False)
    user_verified_dataFrame.to_csv(path + verified_user_file,index=False,header=False)
    print 'all user:\n',user_dataFrame
    print 'verified user:\n',user_verified_dataFrame


def main():
    write_log(log_file_name='find_verified_user.log', log_file_path=os.getcwd(),
              information='###################program start#####################.')
    path = 'D:/LiuQL/eHealth/twitter/data/data_origin/'
    duplicate_user_file = 'duplicate_user.txt'
    all_user_file = 'all_user.txt'
    verified_user_file = 'verified_user.txt'
    find_user(path = path,file_save_to=duplicate_user_file)
    drop_duplicate_user(path = path, actor_file=duplicate_user_file,all_user_file=all_user_file,verified_user_file=verified_user_file)
    write_log(log_file_name='find_verified_user.log', log_file_path=os.getcwd(),
              information='###################program finished#####################.' + '\n' * 5)

main()