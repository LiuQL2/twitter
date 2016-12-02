# !/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import json
from wordcloud import WordCloud
from utility.functions import get_dirlist
import chardet
import sys
import unicodedata
reload(sys)
sys.setdefaultencoding('utf-8')


class wordCloud(object):
    def __init__(self, top_words_file_path, key_word_list = list(), no_key_words_list = list()):
        self.top_words_filename_list = get_dirlist(top_words_file_path,key_word_list=key_word_list,no_key_word_list=no_key_words_list)
        self.top_words_file_path = top_words_file_path
        self.community_topics = {}
        self.community_file = {}
        community_id_list = []
        for file_name in self.top_words_filename_list:
            community_id = int(file_name.split('-')[1])
            community_id_list.append(community_id)
        self.community_id_list = list(set(community_id_list))

        for community_id in community_id_list:
            self.community_file[community_id] = []

        for file_name in self.top_words_filename_list:
            community_id = int(file_name.split('-')[1])
            self.community_file[community_id].append(file_name)



    def plot_word_cloud(self, image_save_to,community_id_list = list()):
        import matplotlib.pyplot as plt
        if len(community_id_list) == 0:
            community_id_list = self.community_id_list
        else:
            pass
        self.__get_community_topics__(community_id_list)



        for community_id in self.community_topics.keys():
            community_topics = self.community_topics[community_id]
            print community_topics
            print community_id
            cloud_number = len(community_topics)
            if cloud_number == 4:
                index = 1
                for topic_id, topic in community_topics.items():
                    plt.subplot(2,2,index)
                    temp_word_cloud = WordCloud(background_color="white").generate(topic['word_list'])
                    plt.imshow(temp_word_cloud)
                    # plt.title(str(topic['topic_id'] + ':' + topic['topic_probability']))
                    plt.axis('off')
                    index = index + 1
                plt.savefig( image_save_to + 'word_cloud_' + str(community_id) + '.png')
            else:
                index = 1
                for topic_id, topic in community_topics.items():
                    plt.subplot(cloud_number,1,index)
                    temp_word_cloud = WordCloud(background_color="white").generate(topic['word_list'])
                    plt.imshow(temp_word_cloud)
                    # plt.title(str(topic['topic_id'] + ':' + topic['topic_probability']))
                    plt.axis('off')
                    index = index + 1
                plt.savefig( image_save_to + 'word_cloud_' + str(community_id) + '.png')



    def __get_community_topics__(self,community_id_list ):
        for community_id in community_id_list:
            community_id = int(community_id)
            self.community_topics[community_id] = self.__parse_top_words_for_each_community__(community_id=community_id)
            if len(self.community_topics[community_id]) > 4:
                id_probability = {}
                for topic_id, value in self.community_topics[community_id].items():
                    # id_probability[topic_id] = value['topic_probability']
                    self.community_topics[community_id].pop(topic_id)
                    if len(self.community_topics[community_id]) == 4:
                        break
                pass
            else:
                pass


    def __parse_top_words_for_each_community__(self, community_id):
        community_top_words = {}
        topic_index = 0
        for file_name in self.community_file[community_id]:
            words_file = open(self.top_words_file_path + file_name, 'r')
            for line in words_file:
                row = json.loads(line)
                temp_line = line
                temp_line = temp_line[1:len(temp_line) - 1]
                print 'line', line
                print chardet.detect(line)
                print 'temp', temp_line
                print 'row',row
                community_top_words[topic_index] = self.__parse_topic__(topic_index=topic_index,row = row)
                topic_index = topic_index + 1
            words_file.close()
        return community_top_words

    def __parse_topic__(self, topic_index, row):
        topic = {}
        topic['topic_id'] = row['topicId']
        topic['topic_probability'] = row['topicProb']
        topic['word_list'] = {}

        for word in row['words']:

            print chardet.detect(word['wordStr'].encode("UTF-8"))
            print type(word['wordStr'])
            # word_name =  unicodedata.normalize('NFKD',word['wordStr']).encode('ascii','ignore')
            word_name =  word['wordStr'].encode("ISO-8859-6",'ignore')
            word_name =  word['wordStr'].encode("ISO-8859-6")
            # print word_name
            topic['word_list'][word_name] = word['weight']
        return topic



if __name__ == '__main__':
    top_words_file_path = 'D:/LiuQL/eHealth/twitter/wordCloud/top_words/'
    cloud = wordCloud(top_words_file_path=top_words_file_path)
    cloud.plot_word_cloud(community_id_list=[5],image_save_to= 'D:/')
    # print cloud.community_topics
