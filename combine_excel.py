# !/usr/bin/env python
#  -*- coding: utf-8 -*-
import time
import os
import sys
import json
import pandas as pd
reload(sys)
sys.setdefaultencoding('utf-8')



def get_dirlist(path,key_word_list):
    file_name_list = os.listdir(path)#获得原始json文件所在目录里面的所有文件名称
    if key_word_list == None:
        temp_file_list = file_name_list
    else:
        temp_file_list = []
        for file_name in file_name_list:
            have_key_words = True
            for key_word in key_word_list:
                if key_word not in file_name:
                    have_key_words = False
                    break
                else:
                    pass
            if have_key_words == True:
                temp_file_list.append(file_name)
    return temp_file_list


def combine_excel(path,key_word_list,file_name_save_to):
    file_name_list = get_dirlist(path = path, key_word_list=key_word_list)
    print file_name_list

    data_list = []
    for file_name in file_name_list:
        data = pd.read_excel(path + file_name,encoding = 'gb2312')
        data['file_name'] = file_name.split('.')[0]
        print data
        data_list.append(data)
    dataFrame = pd.concat(data_list, ignore_index=False)
    dataFrame.to_csv(path + file_name_save_to,index = False,encoding = 'gb2312')

combine_excel(path= 'D:/',key_word_list=['test'],file_name_save_to='combined_data.csv')
