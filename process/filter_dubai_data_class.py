# -*- coding: utf-8 -*-
"""
用于过滤迪拜用户和非迪拜用户的数据
"""
import json
import csv
import sys
import os
import time
from collections import OrderedDict
from utility.functions import get_dirlist

reload(sys)
sys.setdefaultencoding('utf-8')


class filterDubai(object):
    def __init__(self):
        self.dubai_data_number = 0
        self.no_dubai_data_number = 0
        pass

    def filter_data(self,origin_file_path, reserved_data_save_to, filtered_data_save_to):
        self.reserved_data_save_to =reserved_data_save_to
        self.filtered_data_save_to = filtered_data_save_to
        self.origin_file_path = origin_file_path
        file_name_list = get_dirlist(origin_file_path,key_word_list=['part-r','.json'],no_key_word_list=['crc'])  # 获得原始json文件所在目录里面的所有文件名称
        index = 0
        start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        for file_name in file_name_list:
            index = index + 1
            print index, file_name, 'is being parsing......'
            self.__read_json__(file_name)


        end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        parse_info_file = open(os.getcwd() + '/filter_data_info.log', 'wb')
        parse_info_file.write("start time:" + str(start_time) + '\n')
        parse_info_file.write("end time:" + str(end_time) + '\n')
        parse_info_file.write("total number of files that parsed:" + str(index) + '\n')
        parse_info_file.write("total number of Dubai tweet:" + str(self.dubai_data_number) + '\n')
        parse_info_file.write("total number of No Dubai tweet:" + str(self.no_dubai_data_number) + '\n')
        parse_info_file.close()

        print '================================================================='
        print 'start_time:', start_time
        print 'end_time:', end_time
        print "total number of Dubai tweet:", self.dubai_data_number
        print "total number of No Dubai tweet:", self.no_dubai_data_number
        print '================================================================='

    def __read_json__(self,file_name):
        """
        对于给定的一个json文件进行依次循环判断里面的所有记录
        """
        origin_data_json_file = open(self.origin_file_path + file_name, 'r')  # 读取json
        temp_filter_file = open(self.filtered_data_save_to + 'no_dubai_' + file_name, 'wb')  # 构建新文件，用户保存过滤掉的信息
        temp_dubai_file = open(self.reserved_data_save_to + 'dubai_' + file_name, 'wb')  # 构建新文件，用于保存迪拜地区的数据
        for line in origin_data_json_file:  # 循环判断json文件中的每一行
            # print type(line)#输出改行数据
            if len(line) > 10:
                self.__verify_data__(line, temp_dubai_file=temp_dubai_file,
                                 temp_filter_file=temp_filter_file)  # 调用函数对一条数据进行判断，并放入到对应的文件中
        origin_data_json_file.close()  # 依次关闭三个文件
        temp_filter_file.close()
        temp_dubai_file.close()

    def __verify_data__(self,line, temp_filter_file, temp_dubai_file):
        """
        用于判断一条数据是不是迪拜地区的
        :param line: 需要判断的数据
        :param temp_filter_file: 如果该条数据不是迪拜地区，或者无法判断时需要保存的位置
        :param temp_dubai_file: 如果该条数据是迪拜地区的，该条数据需要保存的位置
        :return: 无返回内容
        """
        print line
        row = json.loads(line, object_pairs_hook=OrderedDict)  # 提取json形式
        if 'enrichRegion' in row['actor']['location'].keys():  # enrichRegion字段在location里面
            enrich_region = row['actor']['location']['enrichRegion'].title()
            if 'Dubai' in enrich_region or 'دبي' in row['actor']['location']['enrichRegion']:  # 可能有阿拉伯语或者英语，所以都要判断
                temp_dubai_file.write(line)  # 保存到是迪拜数据的文件
                self.dubai_data_number = self.dubai_data_number + 1
                # print 'dubai_file: ', enrich_region
            else:
                line = self.__add_error_type__(line, error_type='"enrichRegion, no Dubai"')  # 追加过滤原因
                temp_filter_file.write(line)
                self.no_dubai_data_number = self.no_dubai_data_number + 1
                # print 'filer_file: ', enrich_region
        elif 'enrichRegion' not in row['actor']['location'].keys() and 'userDisplayName' in row['actor'][
            'location'].keys():  # enrichRegion不在location里面，但是userDisplayName在location里面
            user_display_name = row['actor']['location']['userDisplayName'].title()
            if 'Dubai' in user_display_name or 'دبي' in row['actor']['location']['userDisplayName']:
                temp_dubai_file.write(line)
                self.dubai_data_number = self.dubai_data_number + 1
                # print 'dubai_file: ', user_display_name
            else:  # userDisplayName里面没有迪拜信息
                line = self.__add_error_type__(line, error_type='"userDisplayName, no enrichRegion, no Dubai"')  # 追加过滤原因
                temp_filter_file.write(line)
                self.no_dubai_data_number = self.no_dubai_data_number + 1
                # print 'filer_file: ', user_display_name
        else:  # 两个字段都不在location里面，无法判断
            line = self.__add_error_type__(line, error_type='"no userDisplayName, no enrichRegion"')  # 追加过滤原因
            temp_filter_file.write(line)
            self.no_dubai_data_number = self.no_dubai_data_number + 1

    def __add_error_type__(self,line, error_type):
        """
        用来向数据中追加该条数据被过滤掉的原因
        :param line: 一条数据
        :param error_type: 过滤的原因
        :return: 追加过滤原因之后的一条数据
        """
        tweet_type_list = ['"type":"reply"', '"type":"tweet"', '"type":"retweet"']  # 推文的三种状态，每一条推文必包含其中一个，且只有一个
        for tweet_type in tweet_type_list:
            if tweet_type in line:
                line = line.replace(tweet_type + '}', tweet_type + ',"errorType":' + error_type + '}')
                break
        return line



if __name__ == '__main__':
    filtered_data_save_to = 'D:/LiuQL/eHealth/twitter/data/data_filter/'  # 过滤掉的文件所在目录
    origin_file_path = 'D:/LiuQL/eHealth/twitter/data/data_origin/'  # 原始json数据所在的目录
    reserved_data_save_to = 'D:/LiuQL/eHealth/twitter/data/data_dubai/'  # 迪拜地区数据文件所在的目录
    filter = filterDubai()
    filter.filter_data(origin_file_path=origin_file_path,reserved_data_save_to=reserved_data_save_to, filtered_data_save_to=filtered_data_save_to)