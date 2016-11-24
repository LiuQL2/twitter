#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import time
import os
import sys
import csv
# from utility.functions import write_log
from functions import write_log
reload(sys)
sys.setdefaultencoding('utf-8')

def calculate():
    path_data = 'D:/LiuQL/eHealth/twitter/data/data_hash/result/'
    path_save_to = 'D:/LiuQL/eHealth/twitter/data/data_hash/result/'
    file_save_to = 'life_cycle.csv'
    file_filter_save_to = 'life_cycle_filter.csv'
    path_data = '/pegasus/harir/Qianlong/data/project_data/twitter_hash_dataFrame/root_tweet/'
    path_save_to = '/pegasus/harir/Qianlong/data/project_data/twitter_hash_dataFrame/root_tweet/'
    dataFrame_dict = read_csv(data_path=path_data)
    hash_dataFrame_dict = hash_tweet_dataFrame(dataFrame_dict=dataFrame_dict)
    calculate_lifecycle(tweet_dataFrame_dict=hash_dataFrame_dict,file_save_to=file_save_to,file_filter_save_to=file_filter_save_to,path_save_to=path_save_to)

    describe_lifecycle(path_save_to=path_save_to,file_save_to=file_save_to,file_filter_save_to=file_filter_save_to)


def read_csv(data_path):
    name_list = os.listdir(data_path)
    file_name_list = []
    for name in name_list:
        if 'hash_qianlong' in name:
            file_name_list.append(name)
    dataFrame_dict = {}
    index = 0
    for file_name in file_name_list:
        index += 1
        # write_log(log_file_name='calculate.log',log_file_path=os.getcwd().replace('process',''),information=str(index) + ': Reading file to dataFrame:' + file_name + ' is being reading...')
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
    # write_log(log_file_name='calculate.log', log_file_path=os.getcwd(),information='tweet_dataFrame has been built, total number:')

    return dataFrame_dict


def hash_tweet_dataFrame(dataFrame_dict):
    tweet_hash_dict = {}
    for index in range(0,100,1):
        tweet_hash_dict['hash_dataFrame_' + str(index)] = pd.DataFrame()
    key_index = 0
    for key in dataFrame_dict.keys():
        key_index = key_index + 1
        print time.ctime(), str(key_index) + ': hashing tweet dataFrame:' + key + ' is being reading...'
        # write_log(log_file_name='calculate.log', log_file_path=os.getcwd(),information= str(key_index) + ': hashing tweet dataFrame:' + key + ' is being reading...')
        id_list = set(list(dataFrame_dict[key].tweet_id))
        for tweet_id in id_list:
            hash_number = hash(str(tweet_id)) % 100
            tweet_hash_dict['hash_dataFrame_' + str(hash_number)] = tweet_hash_dict['hash_dataFrame_' + str(hash_number)].append(dataFrame_dict[key][dataFrame_dict[key].tweet_id == tweet_id],ignore_index=False)

    for key in tweet_hash_dict.keys():
        tweet_hash_dict[key] = tweet_hash_dict[key].drop_duplicates()
    return tweet_hash_dict


def time_timestamp(tweet_time):
    if type(tweet_time) == str:
        temp_time = time.mktime(time.strptime(tweet_time, '%Y-%m-%d %H:%M:%S'))
        # print temp_time,type(temp_time)
    elif type(tweet_time) == float:
        temp_time = time.localtime(tweet_time)
        temp_time = time.strftime('%Y-%m-%d %H:%M:%S', temp_time)
        # print temp_time
    return temp_time


def calculate_lifecycle(tweet_dataFrame_dict,file_save_to,file_filter_save_to, path_save_to):
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