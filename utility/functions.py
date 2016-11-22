# !/usr/bin/env python
#  -*- coding: utf-8 -*-
import time
import os
import sys
import json
reload(sys)
sys.setdefaultencoding('utf-8')

def get_dirlist(path):
    file_name_list = os.listdir(path)#获得原始json文件所在目录里面的所有文件名称
    temp_file_list = []
    for file_name in file_name_list:
        if 'part-r' in file_name and '.json' in file_name and 'crc' not in file_name:
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
    log_file = open(log_file_path + log_file_name, 'a+')
    temp_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    log_file.write('[' + temp_time + ']' + ': ' + information + '\n')
    log_file.close()