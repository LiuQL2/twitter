#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
按照杨师兄的思路进行，为每一条记录寻找其最原始的root的tweet。
源文件必须为根据tweet_id进行hash分别存储之后的文件，且格式必须为['tweet_id', 'origin_tweet_id', 'from_user','from_user_id','to_user','to_user_id', 'tweet_time', 'origin_tweet_time', 'type']
这个是先将数据进行hash存储，然后按照一个文件作为一个dataFrame中，最终所有的dataFrame放入一个字典中，在查询的时候根据查询对象利用hash，确定该对象在哪一个dataFrame中， 然后在进行查询。
在为每一条寻找root tweet的时候，是对一块一块里面的记录进行遍历。即两层循环。
另外，文件是分别存储的。速度最快。

为每一条tweet找到其root tweet，并将结果保存成（tweet_id, tweet_time, root_tweet_id, root_tweet_time）格式。而且为dataFrame_dict中每一个dataFrame保存一个文件，文件名称和其key相同，也即和源文件相同。
"""

import pandas as pd
import time
import os
import sys
import csv
from utility.functions import write_log,get_dirlist
reload(sys)
sys.setdefaultencoding('utf-8')


def calculate_lifecycle():
    write_log(log_file_name='find_root_tweet.log', log_file_path=os.getcwd(),
              information='############################## start program ################################')
    data_path =  'D:/LiuQL/eHealth/twitter/data/data_hash/'
    path_save_to =  'D:/LiuQL/eHealth/twitter/data/data_hash/result/'
    path_save_to =  'D:/LiuQL/eHealth/twitter/data/'
    file_name_save_to = 'tweet_originTweet_error.csv'
    # data_path = '/pegasus/harir/Qianlong/data/hash/'
    # path_save_to = '/pegasus/harir/Qianlong/data/project_data/twitter_hash_dataFrame/'
    dataFrame_dict = get_all_dataFrame(data_path=data_path)
    print 'tweet_dataFrame has been built.'
    build_tweet(dataFrame_dict=dataFrame_dict,path_save_to=path_save_to, file_name_save_to=file_name_save_to)
    write_log(log_file_name='find_root_tweet.log', log_file_path=os.getcwd(),
              information='############################## program end ################################' + '\n' * 4)


def get_all_dataFrame(data_path):
    """
    根据所有的hash之后的文件进行为每一个文件构建一个dataFrame，最后所有的dataFrame放入一个dict中，key名即为文件的名称
    :param data_path: hash文件存储的路径。
    :return:保存dataFrame的dict
    """
    file_name_list = get_dirlist(path=data_path, key_word_list=['hash_qianlong'])
    dataFrame_dict = {}
    index = 0
    for file_name in file_name_list:
        index += 1
        write_log(log_file_name='find_root_tweet.log',log_file_path=os.getcwd(),information=str(index) + ': Reading file to dataFrame:' + file_name + ' is being reading...')
        print time.ctime(), str(index) + ': Reading file to dataFrame:' + file_name + ' is being reading...'
        data = pd.read_csv(data_path + file_name, header = None)
        data.columns = ['tweet_id', 'origin_tweet_id', 'from_user','from_user_id','to_user','to_user_id', 'tweet_time', 'origin_tweet_time', 'type']
        data = data[data.origin_tweet_time != 'null']
        data = data[data.type != 'mention']
        del data['from_user']
        del data['from_user_id']
        del data['to_user']
        del data['to_user_id']
        data.index = data.tweet_id
        dataFrame_dict[file_name] = data
    write_log(log_file_name='find_root_tweet.log', log_file_path=os.getcwd(),information='tweet_dataFrame has been built, total number:')

    return dataFrame_dict


def find_root_tweet(dataFrame_dict, tweet_id,depth):
    """
    运用递归的方法，为每一个tweet寻找最终的root tweet。
    :param dataFrame_dict: 包含所有dataFrame的dataFrame。
    :param tweet_id:需要为其查找父tweet的tweet id
    :param depth:递归的层数。
    :return:根据不同情况返回不同结果，能够找到root，返回root的信息；不能找到的返回None，出错的返回False。
    """
    hash_number = hash(str(tweet_id)) % 100
    file_name = 'hash_qianlong_'+ str(hash_number)+ '.csv'
    tweet_dataFrame = dataFrame_dict[file_name]

    depth = depth + 1
    try:
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
    except:
        origin_tweet_id = False
        origin_tweet_time = False
    return origin_tweet_id, origin_tweet_time,depth


def build_tweet(dataFrame_dict,path_save_to,file_name_save_to):
    """
    为每一条tweet找到其root tweet，并将结果保存成（tweet_id, tweet_time, root_tweet_id, root_tweet_time）格式。而且为dataFrame_dict中每一个dataFrame保存一个文件，文件名称和其key相同，也即和源文件相同。
    :param dataFrame_dict:包含初始文件数据的dataFrame_dict
    :param path_save_to:需要将结果保存的路径。
    :param file_name_save_to:出错数据保存的名称。
    :return:无返回内容。
    """
    # lifecycle_dataFrame = pd.DataFrame()
    write_log(log_file_name='find_root_tweet.log', log_file_path=os.getcwd(),information='Finding root tweet for each tweet')
    column = ['tweet_id', 'tweet_time', 'origin_tweet_id', 'origin_tweet_id']
    count = 0
    temp_count = 0
    total_number = 0
    for key in dataFrame_dict.keys():
        total_number = total_number + len(dataFrame_dict[key])
    file_dict = {}
    file_list = []
    for key in dataFrame_dict.keys():
        file = open(path_save_to + key,'wb')
        file_dict[key] = csv.writer(file)
        file_list.append(file)
    file_error = open(path_save_to + file_name_save_to,'wb')
    error_writer  = csv.writer(file_error)
    file_list.append(file_error)
    key_number = 0
    for key in dataFrame_dict.keys():
        key_number = key_number + 1
        tweet_dataFrame = dataFrame_dict[key]
        for index in tweet_dataFrame.index:
            tweet_id = tweet_dataFrame.tweet_id[index]
            tweet_time = tweet_dataFrame.tweet_time[index]
            origin_tweet_id, origin_tweet_time,depth= find_root_tweet(dataFrame_dict=dataFrame_dict,tweet_id=tweet_dataFrame.origin_tweet_id[index],depth=0)
            # if origin_tweet_id != None and tweet_id != origin_tweet_id:
            if origin_tweet_id != None and origin_tweet_id != False:
                # line = pd.DataFrame(data = [[tweet_id,tweet_time,origin_tweet_id, origin_tweet_time]],index = [index],columns=column)
                # lifecycle_dataFrame = lifecycle_dataFrame.append(line,ignore_index=False)
                file_dict[key].writerow([tweet_id,tweet_time,origin_tweet_id, origin_tweet_time])
                print 'key_number:',key_number,'number:',count, 'total_number:', total_number, 'depth:',depth, tweet_id,tweet_time,origin_tweet_id, origin_tweet_time
            elif origin_tweet_id == False:
                error_writer.writerow([tweet_id, tweet_time, origin_tweet_id, origin_tweet_time])
                print  'key_number:', key_number, 'Error!! number:', count, 'total_number:', total_number, 'depth:', depth, tweet_id, tweet_time, origin_tweet_id, origin_tweet_time
            count += 1
            if count - temp_count >= 10000:
                write_log(log_file_name='find_root_tweet.log', log_file_path=os.getcwd(),information='key_number:'+ str(key_number) + 'Finding root tweet, total_number:'+str(total_number)+',finished_number:'+str(count) + '   Finding root tweet for each tweet')
                temp_count = count
    for file in file_list:
        file.close()
    # lifecycle_dataFrame = lifecycle_dataFrame[lifecycle_dataFrame.tweet_id != lifecycle_dataFrame.origin_tweet_id]
    # lifecycle_dataFrame.to_csv(path_save_to + file_name_save_to.replace('.csv', '_dataFrame.csv'))

calculate_lifecycle()

