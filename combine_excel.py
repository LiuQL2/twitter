# !/usr/bin/env python
#  -*- coding: utf-8 -*-
import time
import os
import sys
import json
import pandas as pd
reload(sys)
sys.setdefaultencoding('utf-8')




def get_dirlist(path,key_word_list = None, no_key_word_list = None):
    file_name_list = os.listdir(path)#获得原始json文件所在目录里面的所有文件名称
    if key_word_list == None and no_key_word_list == None:
        temp_file_list = file_name_list
    elif key_word_list != None and no_key_word_list == None:
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
    elif key_word_list == None and no_key_word_list != None:
        temp_file_list = []
        for file_name in file_name_list:
            have_no_key_word = False
            for no_key_word in no_key_word_list:
                if no_key_word in file_name:
                    have_no_key_word = True
                    break
            if have_no_key_word == False:
                temp_file_list.append(file_name)
    elif key_word_list != None and no_key_word_list != None:
        temp_file_list = []
        for file_name in file_name_list:
            have_key_words = True
            for key_word in key_word_list:
                if key_word not in file_name:
                    have_key_words = False
                    break
                else:
                    pass
            have_no_key_word = False
            for no_key_word in no_key_word_list:
                if no_key_word in file_name:
                    have_no_key_word = True
                    break
                else:
                    pass
            if have_key_words == True and have_no_key_word == False:
                temp_file_list.append(file_name)

    return temp_file_list


def combine_excel(path,key_word_list,file_name_save_to,no_key_word_list):
    file_name_list = get_dirlist(path = path, key_word_list=key_word_list,no_key_word_list=no_key_word_list)

    print file_name_list
    print len(file_name_list)

    data_list = []
    file_index = 0
    for file_name in file_name_list:
        file_index += 1
        print '*****', file_index,file_name
        data = pd.read_excel(path + file_name,encoding = 'gbk')
        data['file_name'] = file_name.split('.')[0]
        print data
        data_list.append(data)
    dataFrame = pd.concat(data_list, ignore_index=False)
    print len(dataFrame)
    dataFrame.to_csv(path + file_name_save_to,index = False,encoding = 'gbk')
    print len(dataFrame)
    # dataFrame.to_csv(path + file_name_save_to,index = False,encoding = 'gb2312')

combine_excel(path= 'F:/1022/',key_word_list=['.xlsx',],no_key_word_list=['combin','TT'],file_name_save_to='combined_data.csv')
