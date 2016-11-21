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


def pandas_dataFrame_to_file(dataFrame,file_name, file_path):
    file = open(file_path + file_name, 'wb')
    for index in dataFrame.index:
        line_dict = dict(dataFrame.loc[index])
        line = json.dumps(line_dict) + '\n'
        file.write(line)
    file.close()


def write_log(log_file, log_file_path, information):
    """
    write the log file that could be used to check the information of process.
    :param information: the info that needed to be wrote into log file.
    :return: nothing to return.
    """
    log_file = open(log_file_path + log_file, 'a+')
    temp_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    log_file.write('[' + temp_time + ']' + ': ' + information + '\n')
    log_file.close()
