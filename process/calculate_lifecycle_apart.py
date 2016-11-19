#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import sys
import time
import json
import os
from collections import OrderedDict
from utility.functions import get_dirlist
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

    path_data = 'D:/LiuQL/eHealth/twitter/data_dubai/'
    path_save_to = 'D:/LiuQL/eHealth/twitter/'
    file_save_to_name = 'tweet_lifecycle_apart.json'

    write_log('########################resatrt the program of Claculating lifecycle.########################')

    #calculate lifecycle for each tewwt.
    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print 'the dataFrame of tweet is being building......,,please wait for a moment.'
    tweet_dataFrame_list, tweet_dataFrame_index_list = build_tweet_dataFrame_list(file_path=path_data)
    print 'updating the "end time, retweet count, reply count" of each tweet.....,please wait for a moment.'
    tweet_dataFrame_list = update_tweet(file_path=path_data, tweet_dataFrame_list=tweet_dataFrame_list, tweet_dataFrame_index_list=tweet_dataFrame_index_list)
    print 'claculating the lifecycle of each tweet......,please wait for a moment.'
    tweet_dataFrame_list = get_lifecycle(tweet_dataFrmae_list=tweet_dataFrame_list,file_save_to_name=file_save_to_name,path_save_to=path_save_to)
    tweet_dataFrame = merge_tweet_dataFrame(tweet_dataFrame_list=tweet_dataFrame_list)

    # delete variables that not be used for longer
    del tweet_dataFrame_list
    del tweet_dataFrame_index_list

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
    info_file = open(os.getcwd().replace('process', '') + 'calculate_lifecycle_info_apart.txt', 'wb')
    info_file.write("start time:" + str(start_time) + '\n')
    info_file.write("end time:" + str(end_time) + '\n')
    info_file.write("total number of tweets:" + str(len(tweet_dataFrame.index)) + '\n')
    info_file.write("total number of tweets that been replied:" + str(len(tweet_dataFrame[tweet_dataFrame['reply_count'] > 0].index)) + '\n')
    info_file.write("total number of tweets that been retweeded:" + str(len(tweet_dataFrame[tweet_dataFrame['retweet_count'] > 0].index)) + '\n')
    info_file.write("average reply count:" + str(describe_dataFrame.reply_count['mean']) + '\n')
    info_file.write("average retweet count:" + str(describe_dataFrame.retweet_count['mean']) + '\n')
    info_file.write("average lifecycle of tweets:" + str(describe_dataFrame.lifecycle['mean']) + ' seconds\n')
    info_file.close()

    # write the result into log file.
    write_log("start time:" + str(start_time))
    write_log("end time:" + str(end_time))
    write_log("total number of tweets:" + str(len(tweet_dataFrame.index)))
    write_log("total number of tweets that been replied:" + str(len(tweet_dataFrame[tweet_dataFrame['reply_count'] > 0].index)))
    write_log("total number of tweets that been retweeded:" + str(len(tweet_dataFrame[tweet_dataFrame['retweet_count'] > 0].index)))
    write_log("average reply count:" + str(describe_dataFrame.reply_count['mean']))
    write_log("average retweet count:" + str(describe_dataFrame.retweet_count['mean']) )
    write_log("average lifecycle of tweets:" + str(describe_dataFrame.lifecycle['mean']) + ' seconds')

    print '##############the result has been saved in:',os.getcwd().replace('process', '') + 'calculate_lifecycle_info_apart.txt'
    write_log('The result has been saved in:' + os.getcwd().replace('process', '') + 'calculate_lifecycle_info_apart.txt')
    write_log('************************ Successfully calculated the lifecycle for tweet.*********************\n' + '*' * 100 + '\n' + '*' * 100 + '\n' + '*' * 100 + '\n\n')


def build_tweet_dataFrame_list(file_path):
    """
    build tweet_dataFrame_list, that a dataFrame contains the data of a file, and all dataFrames put into one list.
    :param file_path: the path of directory that files in.
    :return: the tweet_dataFrame_list
    """
    tweet_dataFrame_list = []
    tweet_dataFrame_index_list = []
    file_name_list = get_dirlist(file_path)
    index = 0
    for file_name in file_name_list:
        index = index + 1
        print index,'BUILDING DATAFRAME according to file:',index, file_name
        write_log(str(index) + ': BUILDING DATAFRAME according to file: ' + str(file_name))
        tweet_dataFrame, tweet_dataFrame_index = build_tweet_dataFrame(file_path + file_name)
        tweet_dataFrame_list.append(tweet_dataFrame)
        tweet_dataFrame_index_list.append(tweet_dataFrame_index)
    return tweet_dataFrame_list, tweet_dataFrame_index_list


def build_tweet_dataFrame(file_name):
    """
    build dataFrame of pandas for each tweet, no duplicate records in this dataFrame.
    :param file_name: The file that contain all the data of one file: path + file_name.
    :return: dataFrame of tweets, a list contains the index of the dataFrame
    """
    tweet_dataFrame = pd.DataFrame()
    data_file = open(file_name, 'r')
    columns = ['tweet_id', 'start_time', 'end_time', 'reply_count', 'retweet_count']
    index = 0
    for line in data_file:
        index = index + 1
        row = json.loads(line, object_pairs_hook=OrderedDict)
        if row['type'] == 'tweet':
            new_line = pd.DataFrame(data = [[row['tweet']['id'], row['tweet']['postedTime'], row['tweet']['postedTime'], 0.0, 0.0]], index=[row['tweet']['id']], columns=columns)
            tweet_dataFrame = tweet_dataFrame.append(new_line)
            # print index ,'BUILDING DATAFRAME... new row', row['tweet']['id']
        else:
            # print index, 'BUILDING DATAFRAME... exits in dataFrame'
            pass
    data_file.close()
    tweet_dataFrame = tweet_dataFrame.drop_duplicates()
    return tweet_dataFrame, list(tweet_dataFrame.index)


def update_tweet(file_path, tweet_dataFrame_list, tweet_dataFrame_index_list):
    """
    update the info of each tweet in the dataFrame accroedig to other tweets.
    :param file_path: The path of directory that all files in.
    :param tweet_dataFrame_list: the list containing the tweet-dataFrame
    :param tweet_dataFrame_index_list: the list containing the index of each dataFrame in tweet_dataFrame_list.
    :return: updated tweet_dataFrame_list.
    """
    file_name_list = get_dirlist(file_path)
    file_index = 0
    for file_name in file_name_list:
        file_index = file_index + 1
        print file_index, 'UPDATING INFO OF TWEET...',file_name, 'is processing......'
        write_log(str(file_index) + ': UPDATING INFO OF TWEET...' + str(file_name) +  'is being processed......')
        data_file = open(file_path + file_name, 'r')
        index = 0
        for line in data_file:
            index += 1
            row = json.loads(line)
            tweet_body = row['tweet']['body']

            # 'reply' type, update info of tweet that the reply reply to.
            if row['type'] == 'reply':
                tweet_id = "00" + row['tweet']['inReplyTo']
                tweet_id_index =  get_tweet_id_index(tweet_id=tweet_id,tweet_dataFrame_index_list=tweet_dataFrame_index_list)
                if tweet_id_index != None:
                    tweet_dataFrame_list[tweet_id_index].loc[[tweet_id],['end_time']] = row['tweet']['postedTime']
                    tweet_dataFrame_list[tweet_id_index].loc[[tweet_id],['reply_count']] += 1
                    # print index, 'PROCESSING TWEET... tweet type:', row[ 'type'], 'inReplyTo in the dataFrame and update "reply_count and end_time', '00' + row['tweet']['inReplyTo']
                else:
                    pass

            # 'tweet' type.
            # the condition that the user retweet someone's tweet and attached his own words: update info of the tweet that be retweeted if it is included in dataFrame.
            # the condition that a user posts a new tweet just contains his own origin content: do nothing.
            elif row['type'] == 'tweet' and  '://twitter.com/' in tweet_body and '/status/' in tweet_body:
                tweet_body_content_list = tweet_body.split('://twitter.com/')
                tweet_id_content = [content.split('/status/')[1] for content in tweet_body_content_list if '/status/' in content][0]
                tweet_id = '00' + tweet_id_content[:18]
                tweet_id_index = get_tweet_id_index(tweet_id=tweet_id, tweet_dataFrame_index_list=tweet_dataFrame_index_list)

                if tweet_id_index != None:
                    tweet_dataFrame_list[tweet_id_index].loc[[tweet_id],['end_time']] = row['tweet']['postedTime']
                    tweet_dataFrame_list[tweet_id_index].loc[[tweet_id],['retweet_count']] += 1
                    # print index, 'PROCESSING TWEET... tweet type:', row['type'], 'update "end_time and retweet_count" of tweet:', tweet_id
                else:
                    # print index , 'PROCESSING TWEET... tweet type:', row['type'], 'tweet:', tweet_id,'not in the dataFrame'
                    pass
            # 'retwet' type
            elif row['type'] == 'retweet':
                origin_tweet_id = row['originTweet']['id']
                origin_tweet_id_index = get_tweet_id_index(tweet_id=origin_tweet_id, tweet_dataFrame_index_list=tweet_dataFrame_index_list)
                if origin_tweet_id_index != None:
                    tweet_dataFrame_list[origin_tweet_id_index].loc[[origin_tweet_id],['end_time']] = row['tweet']['postedTime']
                    tweet_dataFrame_list[origin_tweet_id_index].loc[[origin_tweet_id],['retweet_count']] += 1
                    # print index, 'PROCESSING TWEET... tweet type:', row['type'], 'originweet in the dataFrame and update "end_time and retweet_count" of tweet:', tweet_id
                else:
                    # print index , 'PROCESSING TWEET... tweet type:', row['type'], 'originTweet not in the dataFrame'
                    pass
                if '://twitter.com/' in tweet_body and '/status/' in tweet_body:
                    tweet_body_content_list = tweet_body.split('://twitter.com/')
                    tweet_id_content = [content.split('/status/')[1] for content in tweet_body_content_list if '/status/' in content][0]
                    tweet_id = '00' + tweet_id_content[:18]
                    tweet_id_index = get_tweet_id_index(tweet_id=tweet_id,tweet_dataFrame_index_list=tweet_dataFrame_index_list)
                    if tweet_id_index != None:
                        tweet_dataFrame_list[tweet_id_index].loc[[tweet_id],['end_time']] = row['tweet']['postedTime']
                        tweet_dataFrame_list[tweet_id_index].loc[[tweet_id],['retweet_count']] += 1
                        # print index, 'PROCESSING TWEET... tweet type:', row['type'], 'body has twitter url, and updata "end_time and retweet_count" of tweet:', tweet_id
                    else:
                        # print index,  'PROCESSING TWEET... tweet type:', row['type'], 'body has twitter url, but not in the dataFrmae '
                        pass

        data_file.close()
    return tweet_dataFrame_list


def get_tweet_id_index(tweet_id, tweet_dataFrame_index_list):
    """
    find a certain tweet_id is in which element of the tweet_dataFrame_index_list.
    :param tweet_id: the certain tweet_id
    :param tweet_dataFrame_index_list: the list that containing the index of all dataFrame.
    :return: index: if the tweet_id in the index_list, None:the tweet_id not in the Index_list.
    """
    index = 0
    for list in tweet_dataFrame_index_list:
        if tweet_id in list:
            position = index
            break
        else:
            position = None
        index = index + 1
    return position


def get_lifecycle(tweet_dataFrmae_list,file_save_to_name,path_save_to):
    """
    calculate lifecycle for each tweet in dataFrame acrooding to end time and start time.
    :param tweet_dataFrmae_list: the list that having all tweet-dataFrame.
    :param file_save_to_name: the file that result of each tweet that saved to.
    :param path_save_to: the path of the file_save_to_name
    :return: updated tweet_dataFrame
    """
    file_save = open(path_save_to + file_save_to_name, 'wb')
    dataFrame_index = 0
    for tweet_dataFrmae in tweet_dataFrmae_list:
        dataFrame_index = dataFrame_index + 1
        print dataFrame_index, ': CALCULATING LIFECYCLE...', dataFrame_index, 'dataFrame is being calculated......'
        write_log( str(dataFrame_index) + ': CALCULATING LIFECYCLE...   ' + str(dataFrame_index) + ':dataFrame is being calculated......')
        tweet_dataFrmae['lifecycle'] = 0
        index = 0
        for tweet_id in tweet_dataFrmae.index:
            index += 1
            start_time = tweet_dataFrmae.start_time[tweet_id]
            end_time = tweet_dataFrmae.end_time[tweet_id]
            tweet_dataFrmae.loc[[tweet_id], ['lifecycle']] = time.mktime(time.strptime(end_time,'%Y-%m-%dT%H:%M:%S.000Z')) - time.mktime(time.strptime(start_time,'%Y-%m-%dT%H:%M:%S.000Z'))
            line = json.dumps(dict(tweet_dataFrmae.loc[tweet_id])) + '\n'
            # print index, 'CALCULATING LIFECYCLE...', index, 'were calculated and writen to file'
            file_save.write(line)
    file_save.close()
    return tweet_dataFrmae_list

def merge_tweet_dataFrame(tweet_dataFrame_list):
    """
    merge all tweet dataFrame into one dataFrame.
    :param tweet_dataFrame_list: the list that having all tweet-dataFrame.
    :return: the total dataFrame.
    """
    tweet_dataFrame = pd.DataFrame()
    index = 0
    for dataFrame in tweet_dataFrame_list:
        index = index + 1
        print index, 'MERGERING DATAFRAME...',index, 'dataFrame is being merged...'
        print dataFrame.describe()
        write_log( str(index) + ': MERGERING DATAFRAME... ' + str(index) +  'dataFrame is being merged...')
        tweet_dataFrame = tweet_dataFrame.append(dataFrame)
    print '*********************'
    print tweet_dataFrame.head()
    return tweet_dataFrame


def write_log(information):
    log_file = open(os.getcwd().replace('process','') + 'calculate_lifecycle_apart.log', 'a+')
    temp_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    log_file.write('[' + temp_time + ']' + ': ' + information + '\n')
    log_file.close()


calculate_lifecycle()