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


class communityTopicWordCloud(object):
    def __init__(self, topic_words_file_path, font_path = None, key_word_list = list(), no_key_words_list = list()):
        self.top_words_filename_list = get_dirlist(topic_words_file_path,key_word_list=key_word_list,no_key_word_list=no_key_words_list)
        self.topic_words_file_path = topic_words_file_path
        self.community_topics = {}
        self.community_file = {}
        self.font_path = font_path
        self.error_community_id_list = []
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
                    if self.font_path == None:
                        temp_word_cloud = WordCloud(background_color="white").generate(topic['word_list'])
                    else:
                        temp_word_cloud = WordCloud(background_color="white",font_path=self.font_path).generate(topic['word_list'])
                    plt.imshow(temp_word_cloud)
                    plt.axis('off')
                    plt.title('Probability:' + str(round(float(topic['topic_probability']),3)))
                    index = index + 1
                    plt.savefig(image_save_to + 'community_' + str(community_id) + '_topic.png')
            else:
                index = 1
                for topic_id, topic in community_topics.items():
                    plt.subplot(cloud_number,1,index)
                    if self.font_path == None:
                        temp_word_cloud = WordCloud(background_color="white").generate(topic['word_list'])
                    else:
                        temp_word_cloud = WordCloud(background_color="white",font_path=self.font_path).generate(topic['word_list'])
                    plt.imshow(temp_word_cloud)
                    # plt.title(str(topic['topic_id'] + ':' + topic['topic_probability']))
                    plt.axis('off')
                    plt.title('Probability:' + str(round(float(topic['topic_probability']),3)))
                    index = index + 1
                plt.savefig( image_save_to + 'community_' + str(community_id) + '_topic.png')



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
            words_file = open(self.topic_words_file_path + file_name, 'r')
            for line in words_file:
                row = json.loads(line)
                temp_line = line
                temp_line = temp_line[1:len(temp_line) - 1]
                print 'line', line
                print 'temp', temp_line
                print 'row',row
                temp_topic = self.__parse_topic__(topic_index=topic_index,row = row)
                if temp_topic == None:
                    pass
                else:
                    community_top_words[topic_index] = temp_topic
                    topic_index = topic_index + 1
            words_file.close()
        return community_top_words

    def __parse_topic__(self, topic_index, row):
        topic = {}
        topic['topic_id'] = row['topicId']
        topic['topic_probability'] = row['topicProb']
        topic['word_list'] = {}
        for word in row['words']:
            word_name =  word['wordStr']
            topic['word_list'][word_name] = word['weight']
        if len(topic) == 0:
            return topic
        else:
            return topic



if __name__ == '__main__':
    topic_words_file_path = 'D:/LiuQL/eHealth/twitter/wordCloud/community_topic_words/'
    path_save_to = 'D:/LiuQL/eHealth/twitter/wordCloud/word_cloud_image/community_topic_image/'
    font_path = 'C:/Windows/fonts/Arial/arial.ttf'

    # topic_words_file_path = '/pegasus/harir/yangjinfeng/community/topic/'
    # font_path = None
    # path_save_to = '/pegasus/harir/Qianlong/data/word_cloud_image/community_topic_image/'
    cloud = communityTopicWordCloud(topic_words_file_path=topic_words_file_path,font_path=font_path)
    cloud.plot_word_cloud(community_id_list=[2],image_save_to= path_save_to)
