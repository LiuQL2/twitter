# !/usr/bin/env python
#  -*- coding: utf-8 -*-
import time
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def get_dirlist(path):
    file_name_list = os.listdir(path)#获得原始json文件所在目录里面的所有文件名称
    temp_file_list = []
    for file_name in file_name_list:
        if 'part-r' in file_name and '.json' in file_name and 'crc' not in file_name:
            temp_file_list.append(file_name)
    return temp_file_list