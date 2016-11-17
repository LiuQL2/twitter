# !/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import json
from utility.functions import get_dirlist
from collections import OrderedDict
reload(sys)
sys.setdefaultencoding('utf-8')


def classify_data():
    path_data = raw_input('Please input the path of directory where the FILES NEEDED to be CLASSIFIED are:')
    path_save_to = raw_input('Please input the path of directory where you want the RESULT FILE saves to:')
    file_save_to = open(path_save_to + 'total_data.json', 'wb')
    file_name_list = get_dirlist(path=path_data)
    index = 0
    for file_name in file_name_list:
        index = index + 1
        print index, file_name, 'is being classifing......'
        read_file(file_name = file_name, path_data = path_data, file_save_to = file_save_to)
    file_save_to.close()

    print 'The result file has been saved to: ', path_save_to + 'total_data.json'

def read_file(file_name, path_data, file_save_to):
    data_file = open(path_data + file_name, 'r')
    for line in data_file:
        line = parse_line(line=line) + '\n'
        file_save_to.write(line)
    data_file.close()


def parse_line(line):
    row = json.loads(line, object_pairs_hook=OrderedDict)
    tweet = {}
    tweet['actor'] = {}
    tweet['tweet'] = {}

    tweet['type'] = row['type']
    tweet['actor']['id'] = row['actor']['id']
    tweet['tweet']['id'] = row['tweet']['id']
    tweet['tweet']['postedTime'] = row['tweet']['postedTime']
    tweet['tweet']['retweetCount'] = row['tweet']['retweetCount']

    tweet_type_list = ['"type":"reply"','"type":"tweet"','"type":"retweet"']
    if tweet['type'] == 'tweet':
        pass
    elif tweet['type'] == 'retweet':
        tweet['originTweet'] = {}
        tweet['originActor'] = {}
        tweet['originTweet']['id'] = row['originTweet']['id']
        tweet['originActor']['id'] = row['originActor']['id']
        tweet['originTweet']['retweetCount'] = row['originTweet']['retweetCount']
    elif tweet['type'] == 'reply':
        tweet['tweet']['inReplyTo'] = row['tweet']['inReplyTo']

    return json.dumps(tweet)


classify_data()