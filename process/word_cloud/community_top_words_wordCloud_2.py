# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
用来处理社区的推文内容的关键词，生成词云。
"""
import pandas as pd
import numpy as np
import json
from wordcloud import WordCloud
from utility.functions import get_dirlist
import chardet
import sys
import time
import unicodedata
reload(sys)
sys.setdefaultencoding('utf-8')


class commmunityTopWordCloud(object):
    def __init__(self, top_words_path_file, background_color = 'white', font_path = None):
        """
        初始化一个实例，用来处理社区的keywords
        :param top_words_path_file: 文件的路径。
        :param background_color:背景色，默认白色。
        :param font_path:字体路径，默认为空。
        """
        self.font_path = font_path
        self.background_color = background_color
        self.top_words_path_file = top_words_path_file
        self.community_id_list = []
        self.community_top_words = {}
        top_words_file = open(top_words_path_file,'r')
        for line in top_words_file:
            row = json.loads(line)
            community_id = int(row.keys()[0])
            if len(row.values()[0]) == 0:
                pass
            else:
                self.community_id_list.append(community_id)
                self.community_top_words[community_id] = row.values()[0]
        top_words_file.close()

    def plot_word_cloud(self,image_save_to,community_id_list = list(),file_name_key_word = ''):
        """
        画出社区hashtags和keywords。当传入的id_list为空时，表示输出所有的社区。
        :param image_save_to: 照片需要保存的位置
        :param community_id_list: 需要输出词云的社区。
        :param file_name_key_word: 保存图片时，图片的名字中含有的关键字
        :return: 无返回内容。
        """
        import matplotlib.pyplot as plt
        temp_community_id_list = []
        community_id_list = list(set(community_id_list))
        for community_id in community_id_list:
            temp_community_id_list.append(int(community_id))
        community_id_list = temp_community_id_list
        if len(community_id_list) == 0:
            community_id_list = self.community_id_list
        else:
            pass
        for community_id in community_id_list:
            word_cloud = WordCloud(font_path=self.font_path,background_color=self.background_color).generate(self.community_top_words[community_id])
            plt.imshow(word_cloud)
            plt.axis('off')
            # plt.title('community id: ' + str(community_id) + ','+ file_name_key_word)
            plt.savefig(image_save_to + 'community_' + str(community_id) + '_' +  file_name_key_word +  '.png')
            print 'community id:', community_id, file_name_key_word,' word cloud has been saved to', image_save_to

if __name__ == '__main__':
    directory_list = ['2016_03_24','2016_03_25','2016_03_26','2016_03_27','2016_03_28','2016_03_29','2016_03_30','2016_03_31','total']
    directory_list = ['2016_03_30','2016_03_31','total']
    top_words_path = 'D:/LiuQL/eHealth/twitter/wordCloud/community_top_words/'
    background_color = 'white'
    font_path = 'C:/Windows/fonts/Arial/arial.ttf'
    image_save_to = 'D:/LiuQL/eHealth/twitter/wordCloud/word_cloud_image/community_top_words_image/'

    words_type_list = ['hashtags','keywords']
    words_type_list = ['hashtags']
    # top_words_path_file = ''
    # background_color = 'white'
    # font_path = None
    # image_save_to = '/pegasus/harir/Qianlong/data/word_cloud_image/community_top_words_image/'
    # cloud = commmunityTopWordCloud(top_words_path_file=top_words_path_file,background_color=background_color,font_path=font_path)
    # cloud.plot_word_cloud(image_save_to=image_save_to,community_id_list=[])

    for directory_name in directory_list:
        path = top_words_path + directory_name + '/'

        print path

        for words_type in words_type_list:
            words_file = path + get_dirlist(path,key_word_list=[words_type])[0]
            print words_file
            save_to = image_save_to + directory_name + '/' + words_type + '/'
            cloud = commmunityTopWordCloud(top_words_path_file=words_file,background_color=background_color,font_path=font_path)
            print 'number of communities:', len(cloud.community_id_list)
            time.sleep(5)
            cloud.plot_word_cloud(image_save_to=save_to,file_name_key_word=words_type,community_id_list=[])