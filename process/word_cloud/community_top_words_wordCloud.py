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


class commmunityTopWordCloud(object):
    def __init__(self, top_words_path_file, background_color = 'white', font_path = None):
        self.font_path = font_path
        self.background_color = background_color
        self.top_words_path_file = top_words_path_file
        self.community_id_list = []
        self.community_top_words = {}
        top_words_file = open(top_words_path_file,'r')
        for line in top_words_file:
            row = json.loads(line)
            community_id = int(row.keys()[0])
            self.community_id_list.append(community_id)
            self.community_top_words[community_id] = row.values()[0]
        top_words_file.close()

    def plot_word_cloud(self,image_save_to,community_id_list = list()):
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
            plt.title('community id: ' + str(community_id) + ', top words')
            plt.savefig(image_save_to + 'community_' + str(community_id) + '_topic.png')

if __name__ == '__main__':

    top_words_path_file = ''
    background_color = 'white'
    font_path = 'C:/Windows/fonts/Arial/arial.ttf'
    image_save_to = 'D:/LiuQL/eHealth/twitter/wordCloud/word_cloud_image/community_top_words_image/'


    # top_words_path_file = ''
    # background_color = 'white'
    # font_path = None
    # image_save_to = '/pegasus/harir/Qianlong/data/word_cloud_image/community_top_words_image/'
    cloud = commmunityTopWordCloud(top_words_path_file=top_words_path_file,background_color=background_color,font_path=font_path)
    cloud.plot_word_cloud(image_save_to=image_save_to,community_id_list=[])