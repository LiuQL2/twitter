#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import sys
import time
import json
from collections import OrderedDict
reload(sys)
sys.setdefaultencoding('utf-8')
from utility.functions import get_dirlist

def calculate_lifecycle():
    # path_data = raw_input('Please input the path of directory where the FILES NEEDED to be CLASSIFIED are:')
    # path_save_to = raw_input('Please input the path of directory where you want the RESULT FILE saves to:')
    # file_save_to_name = raw_input('Please input the file name that you want the result saved to (eg:result.json):')

    path_data = '/Volumes/LIUQL/Twitter/total_data_samll.json'
    path_save_to = '/Volumes/LIUQL/Twitter/'
    file_save_to_name = 'tweet_lifecycle.json'


    # file_save_to = open(path_save_to + file_save_to_name, 'wb')

    tweet_dataFrame = build_tweet_dataFrame(file_name = path_data)
    tweet_dataFrame = process_reply(file_name = path_data, tweet_dataFrame=tweet_dataFrame)

    df = tweet_dataFrame[tweet_dataFrame['reply_count'] > 0].head(20)
    df['lifecycle'] = change_time(df['end_time'],'%Y-%m-%dT%H:%M:%S.000Z') - change_time(df['start_time'],'%Y-%m-%dT%H:%M:%S.000Z')
    print df
    # file_save_to.close()
    # print 'The result file has been saved to: ', path_save_to + file_save_to_name


def build_tweet_dataFrame(file_name):
    tweet_dataFrame = pd.DataFrame()
    data_file = open(file_name, 'r')
    columns = ['tweet_id', 'start_time', 'end_time', 'reply_count', 'retweet_count']
    index = 0
    for line in data_file:
        index = index + 1
        row = json.loads(line, object_pairs_hook=OrderedDict)
        if row['type'] == 'tweet' and row['tweet']['id'] not in tweet_dataFrame.index :
            new_line = pd.DataFrame(data = [[row['tweet']['id'], row['tweet']['postedTime'], row['tweet']['postedTime'], 0,0]], index=[row['tweet']['id']], columns=columns)
            tweet_dataFrame = tweet_dataFrame.append(new_line)
            print index ,'new row', row['tweet']['id']
        else:
            print index, 'exits in dataFrame'
    data_file.close()
    return tweet_dataFrame


def process_reply(file_name, tweet_dataFrame):
    data_file = open(file_name, 'r')
    for line in data_file:
        row = json.loads(line)
        if row['type'] == 'reply' and "00" + row['tweet']['inReplyTo'] in tweet_dataFrame.index:
            tweet_dataFrame.loc[["00" + row['tweet']['inReplyTo']],['end_time']] = row['tweet']['postedTime']
            tweet_dataFrame.loc[["00" + row['tweet']['inReplyTo']],['reply_count']] += 1

    data_file.close()
    return tweet_dataFrame

def change_time(time_series, time_format):
    time_list = list(time_series)
    timestamp_list = []
    for time_point in time_list:
        print time.mktime(time.strptime(time_point,time_format))
        timestamp_list.append(time.mktime(time.strptime(time_point,time_format)) / 60)

    return pd.Series(timestamp_list)

calculate_lifecycle()
a = '2016-03-23T08:53:10.000Z'
print time.mktime(time.strptime(a,'%Y-%m-%dT%H:%M:%S.000Z'))