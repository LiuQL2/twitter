# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
过滤掉认证的用户，筛选需要进行可视化的社区，去掉与认证用户相关的边，
需要两个文件:
所有边文件（source, target, weight)。csv格式，中间用逗号隔开，无header
所有节点所属社区的文件(user_id, community_id)。csv格式，目前是用空格隔开。以后改为逗号隔开，无header。
"""

from community_network_class import communityNetwork

