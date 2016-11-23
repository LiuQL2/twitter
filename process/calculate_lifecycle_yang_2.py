#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
按照杨师兄的思路进行，为每一条记录寻找其最原始的root的tweet。
这个是先将数据进行hash存储，然后按照一条文件作为一个dataFrame中，最终所有的dataFrame放入一个字典中，在查询的时候根据查询对象利用hash，确定该对象在哪一个dataFrame中， 然后在进行查询。
但是在根据每一进行查询的时候是在总表里面进行遍历为其寻找root tweet
"""

import pandas as pd
import time
import os
import sys
import csv
from utility.functions import write_log
reload(sys)
sys.setdefaultencoding('utf-8')


def calculate_lifecycle():
    write_log(log_file_name='calculate_lifecycle_yang.log', log_file_path=os.getcwd(),
              information='############################## start program ################################')
    data_path =  'D:/LiuQL/eHealth/twitter/data/data_hash/'
    path_save_to =  'D:/LiuQL/eHealth/twitter/data/data_hash/'
    file_name_save_to = 'tweet_originTweet.csv'
    # data_path = '/pegasus/harir/Qianlong/data/hash/'
    # path_save_to = '/pegasus/harir/Qianlong/data/project_data/twitter_hash_dataFrame_2/'
    file_name_save_to = 'tweet_originTweet.csv'
    tweet_dataFrame,dataFrame_dict = get_all_dataFrame(data_path=data_path)
    print 'tweet_dataFrame has been built.'
    build_tweet(tweet_dataFrame=tweet_dataFrame,dataFrame_dict=dataFrame_dict,path_save_to=path_save_to, file_name_save_to=file_name_save_to)
    write_log(log_file_name='calculate_lifecycle_yang.log', log_file_path=os.getcwd(),
              information='############################## program end ################################' + '\n' * 4)


def get_all_dataFrame(data_path):
    name_list = os.listdir(data_path)
    file_name_list = []
    for name in name_list:
        if 'hash_qianlong' in name:
            file_name_list.append(name)
    dataFrame_list = []
    dataFrame_dict = {}
    index = 0
    for file_name in file_name_list:
        index += 1
        write_log(log_file_name='calculate_lifecycle_yang.log',log_file_path=os.getcwd().replace('process',''),information=str(index) + ': Reading file to dataFrame:' + file_name + ' is being reading...')
        print time.ctime(), str(index) + ': Reading file to dataFrame:' + file_name + ' is being reading...'
        data = pd.read_csv(data_path + file_name, header = None)
        data.columns = ['tweet_id', 'origin_tweet_id', 'from_user','from_user_id','to_user','to_user_id', 'tweet_time', 'origin_tweet_time', 'type']
        data = data[data.origin_tweet_time != 'null']
        data = data[data.type != 'mention']
        del data['from_user']
        del data['from_user_id']
        del data['to_user']
        del data['to_user_id']
        dataFrame_list.append(data)
        data.index = data.tweet_id
        dataFrame_dict[file_name] = data
    tweet_dataFrame = pd.concat(dataFrame_list, ignore_index = False)
    tweet_dataFrame.index = tweet_dataFrame.tweet_id
    print tweet_dataFrame
    write_log(log_file_name='calculate_lifecycle_yang.log', log_file_path=os.getcwd(),information='tweet_dataFrame has been built, total number:' + str(len(tweet_dataFrame)))

    return tweet_dataFrame,dataFrame_dict


def find_root_tweet(dataFrame_dict, tweet_id,depth):

    hash_number = hash(str(tweet_id)) % 100
    file_name = 'hash_qianlong_'+ str(hash_number)+ '.csv'
    tweet_dataFrame = dataFrame_dict[file_name]

    depth = depth + 1
    query_data = tweet_dataFrame[tweet_dataFrame.tweet_id == tweet_id]
    if len(query_data) > 0:
        if query_data.tweet_id[tweet_id] == query_data.origin_tweet_id[tweet_id]:
            origin_tweet_id = query_data.tweet_id[tweet_id]
            origin_tweet_time = query_data.tweet_time[tweet_id]
        else:
            origin_tweet_id, origin_tweet_time, depth = find_root_tweet(dataFrame_dict=dataFrame_dict,tweet_id=query_data.origin_tweet_id[tweet_id],depth=depth)
    else:
        origin_tweet_id = None
        origin_tweet_time = None
    return origin_tweet_id, origin_tweet_time,depth


def build_tweet(tweet_dataFrame,dataFrame_dict,path_save_to,file_name_save_to):
    # lifecycle_dataFrame = pd.DataFrame()
    write_log(log_file_name='calculate_lifecycle_yang.log', log_file_path=os.getcwd(),information='Finding root tweet for each tweet')
    column = ['tweet_id', 'tweet_time', 'origin_tweet_id', 'origin_tweet_id']
    file = open(path_save_to + file_name_save_to,'wb')
    writer = csv.writer(file)
    count = 0
    temp_count = 0
    total_number = len(tweet_dataFrame)
    for index in tweet_dataFrame.index:
        tweet_id = tweet_dataFrame.tweet_id[index]
        tweet_time = tweet_dataFrame.tweet_time[index]
        origin_tweet_id, origin_tweet_time,depth= find_root_tweet(dataFrame_dict=dataFrame_dict,tweet_id=tweet_dataFrame.origin_tweet_id[index],depth=0)
        # if origin_tweet_id != None and tweet_id != origin_tweet_id:
        if origin_tweet_id != None:
            # line = pd.DataFrame(data = [[tweet_id,tweet_time,origin_tweet_id, origin_tweet_time]],index = [index],columns=column)
            # lifecycle_dataFrame = lifecycle_dataFrame.append(line,ignore_index=False)
            writer.writerow([tweet_id,tweet_time,origin_tweet_id, origin_tweet_time])
            print 'number:',count, 'total_number:', total_number, 'depth:',depth, tweet_id,tweet_time,origin_tweet_id, origin_tweet_time

        count += 1
        if count - temp_count >= 10000:
            write_log(log_file_name='calculate_lifecycle_yang.log', log_file_path=os.getcwd(),information='Finding root tweet, total_number:'+str(total_number)+',finished_number:'+str(count) + '   Finding root tweet for each tweet')
            temp_count = count
    file.close()
    # lifecycle_dataFrame = lifecycle_dataFrame[lifecycle_dataFrame.tweet_id != lifecycle_dataFrame.origin_tweet_id]
    # lifecycle_dataFrame.to_csv(path_save_to + file_name_save_to.replace('.csv', '_dataFrame.csv'))

calculate_lifecycle()

