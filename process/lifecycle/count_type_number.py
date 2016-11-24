#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
计算各个类型记录的个数。文件的格式一定要为这个：['tweet_id', 'origin_tweet_id', 'from_user','from_user_id','to_user','to_user_id', 'tweet_time', 'origin_tweet_time', 'type']
"""

import pandas as pd
import time
import os
import sys
import csv
from utility.functions import get_dirlist
reload(sys)
sys.setdefaultencoding('utf-8')


def count_number():
    data_path =  'D:/LiuQL/eHealth/twitter/data/data_hash/'
    data_path = '/pegasus/harir/Qianlong/data/hash/'
    # path_save_to = '/pegasus/harir/Qianlong/data/project_data/twitter_hash_dataFrame/'
    tweet_dataFrame = get_all_dataFrame(data_path=data_path)
    print 'tweet_dataFrame has been built.'

    pseudo_retweet_dataFrame = tweet_dataFrame[tweet_dataFrame.type == 'pseudo_retweet']
    print 'number of all:' ,len(tweet_dataFrame)
    print 'number of "tweet":' ,len(tweet_dataFrame[tweet_dataFrame.type == 'tweet'])
    print 'number of "mention":' ,len(tweet_dataFrame[tweet_dataFrame.type == 'mention'])
    print 'number of "retweet":' ,len(tweet_dataFrame[tweet_dataFrame.type == 'retweet'])
    print 'number of "reply":' ,len(tweet_dataFrame[tweet_dataFrame.type == 'reply'])
    print 'number of "pseudo-retweet":' ,len(pseudo_retweet_dataFrame)
    print 'number of "pseudo-retweet" and origin_tweet_time is null:' ,len((pseudo_retweet_dataFrame[pseudo_retweet_dataFrame.origin_tweet_time == 'null']))


def get_all_dataFrame(data_path):
    """
    根据所有的文件构建一个dataFrame。文件格式为['tweet_id', 'origin_tweet_id', 'from_user','from_user_id','to_user','to_user_id', 'tweet_time', 'origin_tweet_time', 'type']
    :param data_path:存储文件的位置。
    :return:构建之后的dataFrame。
    """
    file_name_list = get_dirlist(path=data_path, key_word_list=['hash_qianlong'])
    dataFrame_list = []
    tweet_id_dict = {}
    index = 0
    for file_name in file_name_list:
        index += 1
        print time.ctime(), str(index) + ': Reading file to dataFrame:' + file_name + ' is being reading...'

        data = pd.read_csv(data_path + file_name, header = None)
        data.columns = ['tweet_id', 'origin_tweet_id', 'from_user','from_user_id','to_user','to_user_id', 'tweet_time', 'origin_tweet_time', 'type']
        del data['from_user']
        del data['from_user_id']
        del data['to_user']
        del data['to_user_id']
        data.index = data.tweet_id
        tweet_id_dict[file_name] = list(data.tweet_id)
        dataFrame_list.append(data)
    tweet_dataFrame = pd.concat(dataFrame_list, ignore_index = False)
    tweet_dataFrame.index = tweet_dataFrame.tweet_id
    print tweet_dataFrame
    return tweet_dataFrame


count_number()