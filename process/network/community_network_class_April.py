# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
过滤掉认证的用户，筛选需要进行可视化的社区，去掉与认证用户相关的边，
需要两个文件:
所有边文件（source, target, weight)。csv格式，中间用逗号隔开，无header
所有节点所属社区的文件(user_id, community_id)。csv格式，目前是用空格隔开。以后改为逗号隔开，无header。
"""

import numpy as np
import pandas as pd
from community_network_class import CommunityNetwork
from utility.functions import get_dirlist
import time


class CommunityNetworkApril(CommunityNetwork):
    def label_nodes(self,top_node_size,label_path_file, sep=',',header = None):
        """
        根据节点的出入度，对度为top的节点进行打标签。
        :param label_path_file:节点标签文件路径及其名称。格式为(user_id, verify, name),csv文件，以sep分隔，无header
        :param sep:标签文件中的分隔符。
        :param header:标签文件第一行是否是列名。
        """
        if 'degree' not in self.community_nodes_dataFrame.columns:
            self.__calculate_degree__()
        else:
            pass

        self.community_nodes_dataFrame['label'] = None
        id_label_dataFrame = pd.read_csv(label_path_file, names=['id', 'is_verified', 'label','klout_score'], header=header,
                                         dtype={'id': np.str}, sep=sep)
        id_label_dataFrame.index = id_label_dataFrame.id
        community_id_list = set(list(self.community_nodes_dataFrame.community_id))
        for community_id in community_id_list:
            print 'community id:', community_id, 'is being label...'
            single_community_node_dataFrame = (self.community_nodes_dataFrame[self.community_nodes_dataFrame.community_id == community_id]).sort_values(by=['degree'], ascending=[0])
            top_id_list = list(single_community_node_dataFrame.id)[0:top_node_size]
            for id in top_id_list:
                self.community_nodes_dataFrame.loc[[id], ['label']] = id_label_dataFrame.label[id]
                print id, id_label_dataFrame.label[id]

    def find_overlap_users(self,overlap_path_file, sep = ',', header = None):
        overlap_user_list = []
        overlap_user_file = open(overlap_path_file, 'r')
        for line in overlap_user_file:
            user_id = line.split(' ')[0]
            overlap_user_list.append(user_id)

        for id in self.community_nodes_dataFrame.id:
            if id in overlap_user_list:
                self.community_nodes_dataFrame.loc[[id], ['community_id']] = self.community_number + 1
            else:
                pass





if __name__ == '__main__':
    print 'yes'
    cycle_list = ['01_07','08_14','15_21','22_28','29_30']

    community_file_path = '/pegasus/harir/yangjinfeng/commitresult4/community2/inoutOrder/'
    network_edge_file_path = '/pegasus/harir/yangjinfeng/commitresult4/network/'
    path_community_node_edge_save_to = '/pegasus/harir/Qianlong/data/April/network/node_edge/'
    id_label_file = network_edge_file_path + 'kloutScore_iDname.txt'
    verified_user_file = network_edge_file_path + 'kloutScore_iDname.txt'

    community_size = 2000
    community_number = 8
    number_of_top_users = 1000
    label_users_number = 20

    for cycle in cycle_list:
        community_user_ordered_file = get_dirlist(path = community_file_path,key_word_list=[cycle,'icpm_ordered'])[0]
        edge_file = get_dirlist(path = network_edge_file_path,key_word_list=[cycle,'-network_weighted'])[0]
        overlap_user_file = get_dirlist(path = community_file_path,key_word_list=[cycle,'icpm.overlap.txt'])[0]
        print community_user_ordered_file
        print edge_file
        print overlap_user_file
        time.sleep(20)


        print community_user_ordered_file + 'is being processing.'
        print '*' * 100
        save_node_file_name = community_user_ordered_file.replace('.icpm_ordered','') + '_nodes_top_' + str(number_of_top_users) + '_contain_verified' + '.csv'
        save_edge_file_name = community_user_ordered_file.replace('.icpm_ordered','') + '_edges_top_' + str(number_of_top_users) + '_contain_verified' + '.csv'

        community_network = CommunityNetworkApril(community_size=community_size,community_number=community_number)
        community_network.get_community_top_nodes(number_of_top_users=number_of_top_users,community_user_ordered_path_file=community_file_path + community_user_ordered_file,filter_verified_user=False,verified_user_path_file=verified_user_file)
        community_network.get_community_edges(total_edge_weight_path_file=network_edge_file_path + edge_file,sep = '\t',wether_hash=False)
        community_network.label_nodes(top_node_size=label_users_number,label_path_file= id_label_file)
        community_network.find_overlap_users(overlap_path_file= community_file_path + overlap_user_file)
        community_network.community_nodes_dataFrame.to_csv(path_community_node_edge_save_to + save_node_file_name,index = False, header = True, columns = ['id','community_id','label'])
        community_network.community_edges_dataFrame.to_csv(path_community_node_edge_save_to + save_edge_file_name, index = False, header= True, columns= ['source','target','weight'])
        print '\n' * 4