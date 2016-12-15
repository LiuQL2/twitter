# !/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from wordcloud import WordCloud
from community_topic_wordCloud import communityTopicWordCloud
from utility.functions import get_dirlist

class CommunityTopicWordCloudApril(communityTopicWordCloud):


    def plot_word_cloud(self, image_save_to,number_of_community = 0,community_id_list = list(),full_width_community = False):
        """
        用来画社区的主题词云。
        :param image_save_to:词云图片保存的位置路径。
        :param community_id_list: 需要画图的社区id列表，如果为空，则画出所有社区的词云。
        :return: 无返回内容。
        """
        import matplotlib.pyplot as plt
        if number_of_community == 0:
            number_of_community = len(self.community_id_list)
        else:
            pass
        if len(community_id_list) == 0:
            community_id_list = self.community_id_list[0:number_of_community]
        else:
            pass
        self.__get_community_topics__(community_id_list)
        if full_width_community == True:
            community_id_list = self.full_width_community_id_list
        else:
            pass

        #为社区进行画图操作。一个社区最多只画四个主题的词云。
        # number_of_community = len(self.community_id_list)
        plt_index = 0
        # for community_id in self.community_topics.keys():
        for community_id in community_id_list:
            plt_index = plt_index + 1
            for key, community_topics in self.community_topics[community_id].items():
                # community_topics = self.community_topics[community_id]
                # print community_topics
                # print community_id
                cloud_number = len(community_topics)
                if cloud_number == self.max_topic_number or cloud_number == self.max_topic_number - 1:
                    index = 1
                    for topic_id, topic in community_topics.items():
                        plt.subplot(2,2,index)
                        if self.font_path == None:
                            temp_word_cloud = WordCloud(background_color="white").generate(topic['word_list'])
                        else:
                            temp_word_cloud = WordCloud(background_color="white",font_path=self.font_path).generate(topic['word_list'])
                        plt.imshow(temp_word_cloud)
                        plt.axis('off')
                        # plt.title('Probability:' + str(round(float(topic['topic_probability']),3)))
                        index = index + 1
                        plt.savefig(image_save_to + 'community_' + str(community_id) + '_topic_' + key + '.png')
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
                        # plt.title('Probability:' + str(round(float(topic['topic_probability']),3)))
                        index = index + 1
                    plt.savefig( image_save_to + 'community_' + str(community_id) + '_topic_' + key + '.png')
                print 'number of communities:', len(community_id_list), 'index:', plt_index,'community id:', 'language:',key, community_id,' has been saved to,',image_save_to


    def __get_community_topics__(self,community_id_list ):
        """
        用来获取某一个社区的所有主题top词汇。
        :param community_id_list:需要获取的社区id。
        :return: 没有返回
        """
        for community_id in community_id_list:
            community_id = int(community_id)
            self.community_topics[community_id] = self.__parse_topic_words_for_each_community__(community_id=community_id)
            for key, language_topic_value in self.community_topics[community_id].items():
                language_community_topics = self.community_topics[community_id][key]
                if len(language_community_topics) > self.max_topic_number:
                    community_topics = {}
                    id_probability = {}
                    for topic_id, value in self.community_topics[community_id].items():
                        id_probability[topic_id] = value['topic_probability']
                    id_probability_sorted_list = sorted(id_probability.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
                    for index in range(0, self.max_topic_number, 1):
                        community_topics[index] = self.community_topics[community_id][id_probability_sorted_list[index][0]]
                    self.community_topics[community_id][key] = community_topics
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
            language = file_name.split('-')[-1]
            language = language.replace('.json','')
            community_topic_words[language] = {}
            words_file = open(self.topic_words_file_path + file_name, 'r')
            for line in words_file:
                row = json.loads(line)
                temp_topic_full_width = self.__parse_topic__(row = row)
                if temp_topic_full_width == None:
                    pass
                else:
                    community_topic_words[language][topic_index] = temp_topic_full_width['topic']
                    topic_index = topic_index + 1
                    if temp_topic_full_width['full_width'] == True and community_id not in self.full_width_community_id_list:
                        self.full_width_community_id_list.append(community_id)
                    else:
                        pass
            words_file.close()
        return community_topic_words


if __name__ == '__main__':
    directory_list = ['22_28','29_30']
    topic_words_path = 'D:/LiuQL/eHealth/twitter/wordCloud/April/community_topic_words/'
    background_color = 'white'
    font_path = 'C:/Windows/fonts/Arial/arial.ttf'
    image_save_to = 'D:/LiuQL/eHealth/twitter/wordCloud/April/word_cloud_image/community_topic_words_image/'

    topic_type_list = ['without-verified-users','with-verified-users']
    # topic_type_list = ['without-verified-users']
    number_of_community = 500

    for directory_name in directory_list:
        for topic_type in topic_type_list:
            topic_words_file_path = topic_words_path + directory_name + '/' + topic_type + '/'
            save_to = image_save_to + directory_name + '/' + topic_type + '/'
            print topic_words_file_path
            print save_to
            cloud = CommunityTopicWordCloudApril(topic_words_file_path=topic_words_file_path, font_path=font_path)
            cloud.plot_word_cloud(community_id_list=[], image_save_to=save_to,number_of_community=number_of_community,full_width_community=False)

            # cloud.__get_community_topics__(community_id_list=cloud.community_id_list)
            print directory_name, topic_type,cloud.full_width_community_id_list
            print 'number of communities:', len(cloud.community_id_list)
            # time.sleep(5)