#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
按照杨师兄的思路进行，为每一条记录寻找其最原始的root的tweet。
这个是将所有文件的所有数据读入一个dataFrame里面，然后在进行查询。
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
    write_log(log_file_name='calculate_lifecycle_yang_2.log', log_file_path=os.getcwd(),
              information='############################## start program ################################')
    data_path = 'G:/Twitter/data/data_yang/'
    path_save_to = 'G:/Twitter/data/data_yang/'
    file_name_save_to = 'tweet_originTweet.csv'
    # data_path = '/pegasus/harir/yangjinfeng/hash_tracker_2/'
    # path_save_to = '/pegasus/harir/Qianlong/data/project_data/twitter_hash_dataFrame/'
    file_name_save_to = 'tweet_originTweet.csv'
    tweet_dataFrame = get_all_dataFrame(data_path=data_path)
    print 'tweet_dataFrame has been built.'
    build_tweet(tweet_dataFrame=tweet_dataFrame,path_save_to=path_save_to, file_name_save_to=file_name_save_to)
    write_log(log_file_name='calculate_lifecycle_yang_2.log', log_file_path=os.getcwd(),
              information='############################## program end ################################' + '\n' * 4)


def get_all_dataFrame(data_path):
    name_list = os.listdir(data_path)
    file_name_list = []
    for name in name_list:
        if 'hash_tracker' in name:
            file_name_list.append(name)
    dataFrame_list = []
    index = 0
    for file_name in file_name_list:
        index += 1
        write_log(log_file_name='calculate_lifecycle_yang_2.log',log_file_path=os.getcwd().replace('process',''),information=str(index) + ': Reading file to dataFrame:' + file_name + ' is being reading...')
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
    tweet_dataFrame = pd.concat(dataFrame_list, ignore_index = False)
    tweet_dataFrame.index = tweet_dataFrame.tweet_id
    print tweet_dataFrame
    write_log(log_file_name='calculate_lifecycle_yang_2.log', log_file_path=os.getcwd(),information='tweet_dataFrame has been built, total number:' + str(len(tweet_dataFrame)))
    return tweet_dataFrame


def find_root_tweet(tweet_id, tweet_dataFrame,depth):
    depth = depth + 1
    query_data = tweet_dataFrame[tweet_dataFrame.tweet_id == tweet_id]
    if len(query_data) > 0:
        if query_data.tweet_id[tweet_id] == query_data.origin_tweet_id[tweet_id]:
            origin_tweet_id = query_data.tweet_id[tweet_id]
            origin_tweet_time = query_data.tweet_time[tweet_id]
        else:
            origin_tweet_id, origin_tweet_time, depth = find_root_tweet(tweet_id=query_data.origin_tweet_id[tweet_id], tweet_dataFrame= tweet_dataFrame,depth=depth)
    else:
        origin_tweet_id = None
        origin_tweet_time = None
    return origin_tweet_id, origin_tweet_time,depth


def build_tweet(tweet_dataFrame,path_save_to,file_name_save_to):
    # lifecycle_dataFrame = pd.DataFrame()
    write_log(log_file_name='calculate_lifecycle_yang_2.log', log_file_path=os.getcwd(),information='Finding root tweet for each tweet')
    column = ['tweet_id', 'tweet_time', 'origin_tweet_id', 'origin_tweet_id']
    file = open(path_save_to + file_name_save_to,'wb')
    writer = csv.writer(file)
    count = 0
    temp_count = 0
    total_number = len(tweet_dataFrame)
    for index in tweet_dataFrame.index:
        tweet_id = tweet_dataFrame.tweet_id[index]
        tweet_time = tweet_dataFrame.tweet_time[index]
        origin_tweet_id, origin_tweet_time,depth= find_root_tweet(tweet_id=tweet_dataFrame.origin_tweet_id[index], tweet_dataFrame=tweet_dataFrame,depth=0)
        # if origin_tweet_id != None and tweet_id != origin_tweet_id:
        if origin_tweet_id != None:
            # line = pd.DataFrame(data = [[tweet_id,tweet_time,origin_tweet_id, origin_tweet_time]],index = [index],columns=column)
            # lifecycle_dataFrame = lifecycle_dataFrame.append(line,ignore_index=False)
            writer.writerow([tweet_id,tweet_time,origin_tweet_id, origin_tweet_time])
            print 'number:',count, 'total_number:', total_number, 'depth:',depth, tweet_id,tweet_time,origin_tweet_id, origin_tweet_time

        count += 1
        if count - temp_count >= 10000:
            write_log(log_file_name='calculate_lifecycle_yang_2.log', log_file_path=os.getcwd(),information='Finding root tweet, total_number:'+str(total_number)+',finished_number:'+str(count) + '   Finding root tweet for each tweet')
            temp_count = count
    file.close()
    # lifecycle_dataFrame = lifecycle_dataFrame[lifecycle_dataFrame.tweet_id != lifecycle_dataFrame.origin_tweet_id]
    # lifecycle_dataFrame.to_csv(path_save_to + file_name_save_to.replace('.csv', '_dataFrame.csv'))

calculate_lifecycle()

