#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import sys
import time
import json
import os
from collections import OrderedDict
reload(sys)
sys.setdefaultencoding('utf-8')


def calculate_lifecycle():
    """
    calculate lifecycle for each tweet.
    :return: Nothing to return.
    """
    # path_data = raw_input('Please input the FILES which contain the data:')
    # path_save_to = raw_input('Please input the path of directory where you want the RESULT FILE saves to:')
    # file_save_to_name = raw_input('Please input the file name that you want the result saved to (eg:result.json):')

    path_data = 'D:\LiuQL\eHealth\\twitter\\total_data.json'
    # path_data = 'D:\LiuQL\eHealth\\twitter\\total_data_small.json'
    path_save_to = 'D:\LiuQL\eHealth\\twitter\\'
    file_save_to_name = 'tweet_lifecycle.json'

    #calculate lifecycle for each tewwt.
    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print 'the dataFrame of tweet is being building......,,please wait for a moment.'
    tweet_dataFrame = build_tweet_dataFrame(file_name = path_data)
    print 'updating the "end time, retweet count, reply count" of each tweet.....,please wait for a moment.'
    tweet_dataFrame = process_tweet(file_name = path_data, tweet_dataFrame=tweet_dataFrame)
    print 'claculating the lifecycle of each tweet......,please wait for a moment.'
    tweet_dataFrame = get_lifecycle(tweet_dataFrame,file_save_to_name=file_save_to_name,path_save_to=path_save_to)

    #output the result.
    describe_dataFrame = tweet_dataFrame.describe()
    print '=================================================================\ndescribe of the result'
    print describe_dataFrame
    print '=================================================================\nlifecycle > 0:'
    print tweet_dataFrame[tweet_dataFrame['lifecycle'] > 0]
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print '================================================================='
    print 'start_time:',start_time
    print 'end_time:',end_time
    print "total number of tweets:", str(len(tweet_dataFrame.index))
    print "total number of tweets that been replied:" + str(len(tweet_dataFrame[tweet_dataFrame['reply_count'] > 0].index))
    print "total number of tweets that been retweeded:" + str(len(tweet_dataFrame[tweet_dataFrame['retweet_count'] > 0].index))
    print "average reply count:", str(describe_dataFrame.reply_count['mean'])
    print "average retweet count:", str(describe_dataFrame.retweet_count['mean'])
    print "average lifecycle of tweets (seconds):", str(describe_dataFrame.lifecycle['mean'])
    print '================================================================='

    # save the result into file.
    info_file = open(os.getcwd().replace('process', '') + 'calculate_lifecycle_info.txt', 'wb')
    info_file.write("start time:" + str(start_time) + '\n')
    info_file.write("end time:" + str(end_time) + '\n')
    info_file.write("total number of tweets:" + str(len(tweet_dataFrame.index)) + '\n')
    info_file.write("total number of tweets that been replied:" + str(len(tweet_dataFrame[tweet_dataFrame['reply_count'] > 0].index)) + '\n')
    info_file.write("total number of tweets that been retweeded:" + str(len(tweet_dataFrame[tweet_dataFrame['retweet_count'] > 0].index)) + '\n')
    info_file.write("average reply count:" + str(describe_dataFrame.reply_count['mean']) + '\n')
    info_file.write("average retweet count:" + str(describe_dataFrame.retweet_count['mean']) + '\n')
    info_file.write("average lifecycle of tweets:" + str(describe_dataFrame.lifecycle['mean']) + ' seconds\n')
    info_file.close()
    print '##############the result has been saved in:',os.getcwd().replace('process', '') + 'calculate_lifecycle_info.txt'


def build_tweet_dataFrame(file_name):
    """
    build dataFrame of pandas for each tweet, no duplicate records in this dataFrame.
    :param file_name: The file that contain all the data, path + file_name.
    :return: dataFram of pandas.
    """
    tweet_dataFrame = pd.DataFrame()
    data_file = open(file_name, 'r')
    columns = ['tweet_id', 'start_time', 'end_time', 'reply_count', 'retweet_count']
    index = 0
    for line in data_file:
        index = index + 1
        row = json.loads(line, object_pairs_hook=OrderedDict)
        # if row['type'] == 'tweet' and row['tweet']['id'] not in tweet_dataFrame.index :
        if row['type'] == 'tweet':
            new_line = pd.DataFrame(data = [[row['tweet']['id'], row['tweet']['postedTime'], row['tweet']['postedTime'], 0.0, 0.0]], index=[row['tweet']['id']], columns=columns)
            tweet_dataFrame = tweet_dataFrame.append(new_line)
            print index ,'BUILDING DATAFRAME... new row', row['tweet']['id']
        else:
            print index, 'BUILDING DATAFRAME... exits in dataFrame'
            pass
    data_file.close()
    return tweet_dataFrame.drop_duplicates()


def process_tweet(file_name, tweet_dataFrame):
    """
    update the info of each tweet in the dataFrame accroedig to other tweets.
    :param file_name: the file that contains the infomation that can be used to update records.
    :param tweet_dataFrame: dataFrame of each tweet.
    :return: updated dataFrame
    """
    data_file = open(file_name, 'r')
    index = 0
    for line in data_file:
        index += 1
        row = json.loads(line)
        tweet_body = row['tweet']['body']

        # 'reply' type, update info of tweet that the reply reply to.
        if row['type'] == 'reply' and "00" + row['tweet']['inReplyTo'] in tweet_dataFrame.index:
            tweet_dataFrame.loc[["00" + row['tweet']['inReplyTo']],['end_time']] = row['tweet']['postedTime']
            tweet_dataFrame.loc[["00" + row['tweet']['inReplyTo']],['reply_count']] += 1
            print index, 'PROCESSING TWEET... tweet type:', row[ 'type'], 'inReplyTo in the dataFrame and update "reply_count and end_time', '00' + row['tweet']['inReplyTo']


        # 'tweet' type.
        # the condition that the user retweet someone's tweet and attached his own words: update info of the tweet that be retweeted if it is included in dataFrame.
        # the condition that a user posts a new tweet just contains his own origin content: do nothing.
        elif row['type'] == 'tweet' and  '://twitter.com/' in tweet_body and '/status/' in tweet_body:
            tweet_body_content_list = tweet_body.split('://twitter.com/')
            tweet_id_content = [content.split('/status/')[1] for content in tweet_body_content_list if '/status/' in content][0]
            tweet_id = '00' + tweet_id_content[:18]
            if tweet_id in tweet_dataFrame.index:
                tweet_dataFrame.loc[[tweet_id],['end_time']] = row['tweet']['postedTime']
                tweet_dataFrame.loc[[tweet_id],['retweet_count']] += 1
                print index, 'PROCESSING TWEET... tweet type:', row['type'], 'update "end_time and retweet_count" of tweet:', tweet_id
            else:
                print index , 'PROCESSING TWEET... tweet type:', row['type'], 'tweet:', tweet_id,'not in the dataFrame'
        # 'retwet' type
        elif row['type'] == 'retweet':
            origin_tweet_id = row['originTweet']['id']
            if origin_tweet_id in tweet_dataFrame.index:
                tweet_dataFrame.loc[[origin_tweet_id],['end_time']] = row['tweet']['postedTime']
                tweet_dataFrame.loc[[origin_tweet_id],['retweet_count']] += 1
                print index, 'PROCESSING TWEET... tweet type:', row['type'], 'originweet in the dataFrame and update "end_time and retweet_count" of tweet:', tweet_id
            else:
                print index , 'PROCESSING TWEET... tweet type:', row['type'], 'originTweet not in the dataFrame'
            if '://twitter.com/' in tweet_body and '/status/' in tweet_body:
                tweet_body_content_list = tweet_body.split('://twitter.com/')
                tweet_id_content = [content.split('/status/')[1] for content in tweet_body_content_list if '/status/' in content][0]
                tweet_id = '00' + tweet_id_content[:18]
                if tweet_id in tweet_dataFrame.index:
                    tweet_dataFrame.loc[[tweet_id],['end_time']] = row['tweet']['postedTime']
                    tweet_dataFrame.loc[[tweet_id],['retweet_count']] += 1
                    print index, 'PROCESSING TWEET... tweet type:', row['type'], 'body has twitter url, and updata "end_time and retweet_count" of tweet:', tweet_id
                else:
                    print index,  'PROCESSING TWEET... tweet type:', row['type'], 'body has twitter url, but not in the dataFrmae '

    data_file.close()
    return tweet_dataFrame


def get_lifecycle(tweet_dataFrmae,file_save_to_name,path_save_to):
    """
    calculate lifecycle for each tweet in dataFrame acrooding to end time and start time.
    :param tweet_dataFrmae: dataFrame
    :param file_save_to_name: the file that result of each tweet that saved to.
    :param path_save_to: the path of the file_save_to_name
    :return: updated tweet_dataFrame
    """
    file_save = open(path_save_to + file_save_to_name, 'wb')
    tweet_dataFrmae['lifecycle'] = 0
    index = 0
    for tweet_id in tweet_dataFrmae.index:
        index += 1
        start_time = tweet_dataFrmae.start_time[tweet_id]
        end_time = tweet_dataFrmae.end_time[tweet_id]
        tweet_dataFrmae.loc[[tweet_id], ['lifecycle']] = time.mktime(time.strptime(end_time,'%Y-%m-%dT%H:%M:%S.000Z')) - time.mktime(time.strptime(start_time,'%Y-%m-%dT%H:%M:%S.000Z'))
        line = json.dumps(dict(tweet_dataFrmae.loc[tweet_id])) + '\n'
        print index, 'CALCULATING LIFECYCLE...', index, 'were calculated and writen to file'
        file_save.write(line)
    file_save.close()
    return tweet_dataFrmae

calculate_lifecycle()