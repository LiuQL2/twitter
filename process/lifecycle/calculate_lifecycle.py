#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
根据生成的（tweet_id, tweet_time, root_tweet_id, root_tweet_time）格式的文件计算每一个origin_tweet的生命周期。并将结果hash之后保存到一百个文件里面。
包含两种情况，一个是去掉root_tweet是其本身的计算结果，另外一个是全部计算，分别保存到一个文件里面，格式为（tweet_id, lifecycle).
"""

import pandas as pd
import time
import os
import sys
import csv
from utility.functions import write_log
from utility.functions import get_dirlist
reload(sys)
sys.setdefaultencoding('utf-8')

def calculate():
    path_data = 'D:/LiuQL/eHealth/twitter/data/data_hash/result/'
    path_save_to = 'D:/LiuQL/eHealth/twitter/data/data_hash/result/'
    file_save_to = 'life_cycle.csv'
    file_filter_save_to = 'life_cycle_filter.csv'
    #path_data = '/pegasus/harir/Qianlong/data/project_data/twitter_hash_dataFrame/root_tweet/'
    #path_save_to = '/pegasus/harir/Qianlong/data/project_data/twitter_hash_dataFrame/root_tweet/'
    dataFrame_dict = read_csv(data_path=path_data)
    hash_dataFrame_dict = hash_tweet_dataFrame(dataFrame_dict=dataFrame_dict)
    calculate_lifecycle(tweet_dataFrame_dict=hash_dataFrame_dict,file_save_to=file_save_to,file_filter_save_to=file_filter_save_to,path_save_to=path_save_to)

    describe_lifecycle(path_save_to=path_save_to,file_save_to=file_save_to,file_filter_save_to=file_filter_save_to)


def read_csv(data_path):
    """
    根据（tweet_id, tweet_time, root_tweet_id, root_tweet_time）格式的文件构建dataFrame_dict，其中字典中每一个dataFrame的名称和文件名称相同，一个文件的数据存储在一个dataFrame中。
    :param data_path:文件路径
    :return:构建之后的dataFrame_dict.
    """
    file_name_list = get_dirlist(path = data_path, key_word_list=['hash_qianlong'])
    dataFrame_dict = {}
    index = 0
    for file_name in file_name_list:
        index += 1
        # write_log(log_file_name='calculate_lifecycle.log',log_file_path=os.getcwd().replace('process',''),information=str(index) + ': Reading file to dataFrame:' + file_name + ' is being reading...')
        print time.ctime(), str(index) + ': Reading file to dataFrame:' + file_name + ' is being reading...'
        data = pd.read_csv(data_path + file_name, header = None)
        data.columns = ['tweet_id', 'tweet_time', 'origin_tweet_id', 'origin_tweet_time']
        data = data[data.origin_tweet_time != 'null']

        data['lifecycle'] = data.tweet_time.apply(time_timestamp) - data.origin_tweet_time.apply(time_timestamp)
        # data =  data[data.lifecycle != 0.0]

        del data['tweet_id']
        data.columns = ['end_time', 'tweet_id', 'start_time', 'lifecycle']

        # print data
        data = data.drop_duplicates()
        dataFrame_dict[file_name] = data
    # write_log(log_file_name='calculate_lifecycle.log', log_file_path=os.getcwd(),information='tweet_dataFrame has been built, total number:')

    return dataFrame_dict


def hash_tweet_dataFrame(dataFrame_dict):
    """
    根据每一条记录的origin_tweet_id将所有的tweet重新hash一下，这样就能保证一个相同的origin_tweet都放在一个dataFrame中，不用在进行夸dataFrame查找。
    :param dataFrame_dict:原始按照文件存储的dataFrame。
    :return:hash之后的dataFrame。
    """
    tweet_hash_dict = {}
    for index in range(0,100,1):
        tweet_hash_dict['hash_dataFrame_' + str(index)] = pd.DataFrame()
    key_index = 0
    for key in dataFrame_dict.keys():
        key_index = key_index + 1
        print time.ctime(), str(key_index) + ': hashing tweet dataFrame:' + key + ' is being reading...'
        # write_log(log_file_name='calculate_lifecycle.log', log_file_path=os.getcwd(),information= str(key_index) + ': hashing tweet dataFrame:' + key + ' is being reading...')
        id_list = set(list(dataFrame_dict[key].tweet_id))
        for tweet_id in id_list:
            hash_number = hash(str(tweet_id)) % 100
            tweet_hash_dict['hash_dataFrame_' + str(hash_number)] = tweet_hash_dict['hash_dataFrame_' + str(hash_number)].append(dataFrame_dict[key][dataFrame_dict[key].tweet_id == tweet_id],ignore_index=False)

    for key in tweet_hash_dict.keys():
        tweet_hash_dict[key] = tweet_hash_dict[key].drop_duplicates()
    return tweet_hash_dict


def time_timestamp(tweet_time):
    """
    将时间日期格式为'%Y-%m-%d %H:%M:%S'的和时间戳格式相互转化。
    :param tweet_time:时间
    :return:转化之后的时间格式
    """
    if type(tweet_time) == str:
        temp_time = time.mktime(time.strptime(tweet_time, '%Y-%m-%d %H:%M:%S'))
        # print temp_time,type(temp_time)
    elif type(tweet_time) == float:
        temp_time = time.localtime(tweet_time)
        temp_time = time.strftime('%Y-%m-%d %H:%M:%S', temp_time)
        # print temp_time
    return temp_time


def calculate_lifecycle(tweet_dataFrame_dict,file_save_to,file_filter_save_to, path_save_to):
    """
    为每一个tweet寻找其生命周期的最大值。
    :param tweet_dataFrame_dict:hash之后的dataFrame_dict.
    :param file_save_to:不过滤数据的保存文件名称
    :param file_filter_save_to:将生命为0的tweet去掉之后保存的位置。
    :param path_save_to:以上两个文件保存的路径
    :return:Nothing to return.
    """
    file_all = open(path_save_to + file_save_to,'wb')
    file_filter = open(path_save_to + file_filter_save_to,'wb')
    all_writer = csv.writer(file_all)
    filter_writer = csv.writer(file_filter)
    key_index = 0
    for key in tweet_dataFrame_dict.keys():
        key_index = key_index + 1
        id_list = set(list(tweet_dataFrame_dict[key].tweet_id))
        for tweet_id in id_list:
            lifecycle = max(list(tweet_dataFrame_dict[key][tweet_dataFrame_dict[key].tweet_id == tweet_id].lifecycle))
            all_writer.writerow([tweet_id, lifecycle])
            if float(lifecycle) != 0.0:
                filter_writer.writerow([tweet_id, lifecycle])
            print 'key_number:',  key_index, 'calculating lifecycle,', tweet_id, lifecycle

    file_all.close()
    file_filter.close()


def describe_lifecycle(path_save_to, file_save_to, file_filter_save_to):
    """
    根据保存为（tweet_id, lifecycle)格式的文件进行求平均生命周期。必须所有需要求生命周期的推文放在一个文件中。并且将结果保存到一个文件中。
    :param path_save_to:保存结果的位置
    :param file_save_to:（tweet_id, lifecycle)格式的文件，包含生命为0 的数据
    :param file_filter_save_to:（tweet_id, lifecycle)格式的文件，不包含生命为0的数据。
    :return:nothing to return.
    """
    all_tweet = pd.read_csv(path_save_to + file_save_to, header=None,iterator=True)
    filter_tweet = pd.read_csv(path_save_to + file_filter_save_to, header=None,iterator=True)
    chunk_size = 10000
    chunks = []
    loop = True
    while loop:
        try:
            chunk = all_tweet.get_chunk(chunk_size)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print "Iteration is stopped."
    all_tweet_data = pd.concat(chunks, ignore_index=True)
    all_tweet_data.columns =['tweet_id', 'lifecycle']


    loop = True
    chunks = []
    while loop:
        try:
            chunk = filter_tweet.get_chunk(chunk_size)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print "Iteration is stopped."
    filter_tweet_data = pd.concat(chunks, ignore_index=True)
    filter_tweet_data.columns =['tweet_id', 'lifecycle']


    all_describe = all_tweet_data.describe()
    filter_describe = filter_tweet_data.describe()

    all_describe.to_csv(path_save_to + 'all_describe.csv')
    filter_describe.to_csv(path_save_to + 'filter_describe.csv')
    print 'all describe\n',all_describe
    print 'filter describer\n', filter_describe



# time_timestamp('2015-01-20 23:23:34')
# time_timestamp(1421767414.0)

calculate()