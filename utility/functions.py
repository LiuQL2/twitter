# !/usr/bin/env python
#  -*- coding: utf-8 -*-
import time
import os
import sys
import json
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


def pandas_dataFrame_to_file(operation_type, file_path, dataFrame_dict):

    for key in dataFrame_dict.keys():
        file_name = key.replace('.json', '.csv')
        dataFrame_dict[key].to_csv(file_path + operation_type + '/' + file_name)



def write_log(log_file_name, log_file_path, information):
    """
    write the log file that could be used to check the information of process.
    :param log_file_name: the name of log file.
    :param log_file_path: the path of log file.
    :param information: the info that needed to be wrote into log file.
    :return: nothing to return.
    """
    if '\\' in log_file_path:
        path_list = log_file_path.split('\\')
        temp_log_file_path = path_list.pop(0)
        for  temp_path in path_list:
            temp_log_file_path = temp_log_file_path + '/' + temp_path
    else:
        temp_log_file_path = log_file_path
        pass
    log_file_path = temp_log_file_path
    log_file = open(log_file_path +'/'+ log_file_name, 'a+')
    temp_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    log_file.write('[' + temp_time + ']' + ': ' + information + '\n')
    log_file.close()


def read_csv_as_dataFrame_by_chunk(path, file_name,chunk_size = 10000, header = None,sep = ','):
    import pandas as pd
    if header == None:
        data = pd.read_csv(path + file_name, sep = sep,header=None, iterator=True)
    else:
        data = pd.read_csv(path + file_name, sep = sep,iterator=True)
    chunks = []
    loop = True
    while loop:
        try:
            chunk = data.get_chunk(chunk_size)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print "Iteration is stopped."
    dataFrame = pd.concat(chunks, ignore_index=True)
    return dataFrame



def time_timestamp(tweet_time):
    """
    将时间日期格式为'%Y-%m-%d %H:%M:%S'的和时间戳格式相互转化。
    :param tweet_time:时间
    :return:转化之后的时间格式
    """
    import time
    if type(tweet_time) == str:
        temp_time = time.mktime(time.strptime(tweet_time, '%Y-%m-%d %H:%M:%S'))
        # print temp_time,type(temp_time)
    elif type(tweet_time) == float:
        temp_time = time.localtime(tweet_time)
        temp_time = time.strftime('%Y-%m-%d %H:%M:%S', temp_time)
        # print temp_time
    return temp_time
