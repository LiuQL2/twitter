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
        """
        用来初始化一个主题词云的实例
        :param topic_words_file_path: 主题top词文件所在的目录。
        :param font_path: 字体路径，默认为空，在windows系统上需要赋值。
        :param key_word_list: 在目录中读取主题词文件的时候文件名所需要的关键词
        :param no_key_words_list:在目录中读取主题词文件的时候文件名不包含的关键词
        :return:Nothing to return。
        """
        self.topic_words_filename_list = get_dirlist(topic_words_file_path,key_word_list=key_word_list,no_key_word_list=no_key_words_list)
        self.topic_words_file_path = topic_words_file_path
        self.community_topics = {}#用来存储每一个社区的主题，只有但选择画某个社区的图的时候才会进行读取操作
        self.community_file = {}#每一个社区对应的主题词文件，因为一个社区可能由多个文件，所以在进行保存某一个社区对应哪些文件。一遍画图时直接读取。
        self.font_path = font_path#字体路径，有些文字需要制定字体，如阿拉伯语。
        self.error_community_id_list = []
        community_id_list = []
        for file_name in self.topic_words_filename_list:
            community_id = int(file_name.split('-')[1])
            community_id_list.append(community_id)
        self.community_id_list = list(set(community_id_list))#社区id的列表，所有社区的id 都在这里。
        for community_id in community_id_list:
            self.community_file[community_id] = []
        for file_name in self.topic_words_filename_list:
            community_id = int(file_name.split('-')[1])
            self.community_file[community_id].append(file_name)


    def plot_word_cloud(self, image_save_to,community_id_list = list()):
        """
        用来画社区的主题词云。
        :param image_save_to:词云图片保存的位置路径。
        :param community_id_list: 需要画图的社区id列表，如果为空，则画出所有社区的词云。
        :return: 无返回内容。
        """
        import matplotlib.pyplot as plt
        if len(community_id_list) == 0:
            community_id_list = self.community_id_list
        else:
            pass
        self.__get_community_topics__(community_id_list)

        #为社区进行画图操作。一个社区最多只画四个主题的词云。
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
                    plt.axis('off')
                    plt.title('Probability:' + str(round(float(topic['topic_probability']),3)))
                    index = index + 1
                plt.savefig( image_save_to + 'community_' + str(community_id) + '_topic.png')



    def __get_community_topics__(self,community_id_list ):
        """
        用来获取某一个社区的所有主题top词汇。
        :param community_id_list:需要获取的社区id。 
        :return: 没有返回
        """
        for community_id in community_id_list:
            community_id = int(community_id)
            self.community_topics[community_id] = self.__parse_topic_words_for_each_community__(community_id=community_id)
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


    def __parse_topic_words_for_each_community__(self, community_id):
        """
        为某一个社区进行读取文件，提取主题词。
        :param community_id:需要提取的社区id
        :return:返回一个字典，里面包含一个社区所有的主题。
        """
        community_topic_words = {}
        topic_index = 0
        for file_name in self.community_file[community_id]:
            words_file = open(self.topic_words_file_path + file_name, 'r')
            for line in words_file:
                row = json.loads(line)
                print 'row',row
                temp_topic = self.__parse_topic__(row = row)
                if temp_topic == None:
                    pass
                else:
                    community_topic_words[topic_index] = temp_topic
                    topic_index = topic_index + 1
            words_file.close()
        return community_topic_words

    def __parse_topic__(self, row):
        """
        处理一个主题的词汇
        :param row: 文件中对应的一行，也就是一个主题所对应的词。
        :return: 一个字典，包含这个topic的概率，对应的词及其评论，该topic的id。如果里面没有词汇，则返回一个空值。
        """
        topic = {}
        topic['topic_id'] = row['topicId']
        topic['topic_probability'] = row['topicProb']
        topic['word_list'] = {}
        for word in row['words']:
            word_name =  word['wordStr']
            topic['word_list'][word_name] = word['weight']
        if len(topic['word_list']) == 0:
            return None
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
    cloud.plot_word_cloud(community_id_list=[3004],image_save_to= path_save_to)
