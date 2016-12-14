# !/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from community_topic_wordCloud import communityTopicWordCloud
from utility.functions import get_dirlist

class CommunityTopicWordCloudApril(communityTopicWordCloud):
    def __get_community_topics__(self,community_id_list ):
        """
        用来获取某一个社区的所有主题top词汇。
        :param community_id_list:需要获取的社区id。
        :return: 没有返回
        """
        for community_id in community_id_list:
            community_id = int(community_id)
            self.community_topics[community_id] = self.__parse_topic_words_for_each_community__(community_id=community_id)
            # if len(self.community_topics[community_id]) > self.max_topic_number:
            #     id_probability = {}
            #     for topic_id, value in self.community_topics[community_id].items():
            #         # id_probability[topic_id] = value['topic_probability']
            #         self.community_topics[community_id].pop(topic_id)
            #         if len(self.community_topics[community_id]) == self.max_topic_number:
            #             break
            #     pass
            # else:
            #     pass

            if len(self.community_topics[community_id]) > self.max_topic_number:
                community_topics = {}
                id_probability = {}
                for topic_id, value in self.community_topics[community_id].items():
                    id_probability[topic_id] = value['topic_probability']
                id_probability_sorted_list = sorted(id_probability.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
                for index in range(0, self.max_topic_number, 1):
                    community_topics[index] = self.community_topics[community_id][id_probability_sorted_list[index][0]]
                self.community_topics[community_id] = community_topics
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
                temp_topic_full_width = self.__parse_topic__(row = row)
                if temp_topic_full_width == None:
                    pass
                else:
                    community_topic_words[topic_index] = temp_topic_full_width['topic']
                    topic_index = topic_index + 1
                    if temp_topic_full_width['full_width'] == True and community_id not in self.full_width_community_id_list:
                        self.full_width_community_id_list.append(community_id)
                    else:
                        pass
            words_file.close()
        return community_topic_words