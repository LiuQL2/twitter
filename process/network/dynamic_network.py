# !/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
from utility.functions import get_dirlist
import pandas as pd
import time
import numpy as np

file_path = 'D:/node_edge/'
file_name_list = get_dirlist(path = file_path,key_word_list=['nodes','2016-'],no_key_word_list=['total'])
print len(file_name_list)
print file_name_list
time.sleep(10)
node_dataFrame = pd.DataFrame()
node_dataFrame['id'] = None
node_dataFrame['label'] = None
node_dataFrame['2016-03-23'] = None
node_dataFrame['2016-03-24'] = None
node_dataFrame['2016-03-25'] = None
node_dataFrame['2016-03-26'] = None
node_dataFrame['2016-03-27'] = None
node_dataFrame['2016-03-28'] = None
node_dataFrame['2016-03-29'] = None
node_dataFrame['2016-03-30'] = None
node_dataFrame['2016-03-31'] = None

day_list = ['2016-03-23','2016-03-24','2016-03-25','2016-03-26','2016-03-27','2016-03-28','2016-03-29','2016-03-30','2016-03-31']

def check_day(day_list, file_name):
    day = None
    for day_name in day_list:
        if day_name in file_name:
            day = day_name
            break
        else:
            pass
    return day

for file_name in file_name_list:
    file = open(file_path + file_name, 'r')
    reader = csv.reader(file)
    reader.next()
    day_name = check_day(day_list,file_name)
    for line in reader:
        if line[0] not in list(node_dataFrame.id):
            node = pd.DataFrame(data=[[line[0],line[2],None,None,None,None,None,None,None,None,None]],index = [line[0]], columns=['id','label','2016-03-23','2016-03-24','2016-03-25','2016-03-26','2016-03-27','2016-03-28','2016-03-29','2016-03-30','2016-03-31'])
            node_dataFrame = node_dataFrame.append(node,ignore_index=False)
            node_dataFrame.loc[[line[0]],[day_name]] = int(line[1])
            print 'add node:', line[0], 'community:',line[1],'day:',day_name
        else:
            node_dataFrame.loc[[line[0]], [day_name]] = int(line[1])
            print 'modify node:', line[0], 'community:', line[1],'day:',day_name
    file.close()
node_dataFrame.to_csv('D:/day_nodes.csv', index=False, header=True)
