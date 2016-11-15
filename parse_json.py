# -*- coding: utf-8 -*-
#用来抓取课程的信息,里面可以同时下载照片,看看哪一种方法比较完整
import json
import csv
import sys
import os
from collections import OrderedDict

reload(sys)
sys.setdefaultencoding('utf-8')


def read_json(file_name, path_data, path_filter,path_dubai):
    """
    对于给定的一个json文件进行依次循环判断里面的所有记录
    :param file_name: 存储原数据的json文件名称
    :param path_data: 原json文件所在的目录
    :param path_filter: 过滤掉数据所在json文件将被存放的目录
    :param path_dubai: 保存迪拜用户文件所在的目录
    :return: 无返回内容
    """
    origin_data_json_file = open(path_data + file_name, 'r')#读取json
    temp_filter_file = open(path_filter + 'no_dubai_' + file_name, 'wb')#构建新文件，用户保存过滤掉的信息
    temp_dubai_file = open(path_dubai + 'dubai_' + file_name, 'wb')#构建新文件，用于保存迪拜地区的数据
    for line in origin_data_json_file:#循环判断json文件中的每一行
        print type(line)#输出改行数据
        verify_data(line, temp_dubai_file=temp_dubai_file,temp_filter_file=temp_filter_file)#调用函数对一条数据进行判断，并放入到对应的文件中
    origin_data_json_file.close()#依次关闭三个文件
    temp_filter_file.close()
    temp_dubai_file.close()


def verify_data(line, temp_filter_file, temp_dubai_file):
    """
    用于判断一条数据是不是迪拜地区的
    :param line: 需要判断的数据
    :param temp_filter_file: 如果该条数据不是迪拜地区，或者无法判断时需要保存的位置
    :param temp_dubai_file: 如果该条数据是迪拜地区的，该条数据需要保存的位置
    :return: 无返回内容
    """
    row = json.loads(line, object_pairs_hook=OrderedDict)  # 提取json形式
    if 'enrichRegion' in row['actor']['location'].keys():#enrichRegion字段在location里面
        enrich_region = row['actor']['location']['enrichRegion'].title()
        if 'Dubai' in enrich_region or 'دبي' in row['actor']['location']['enrichRegion']:#可能有阿拉伯语或者英语，所以都要判断
            temp_dubai_file.write(line)#保存到是迪拜数据的文件
            # print 'dubai_file: ', enrich_region
        else:
            line = add_error_type(line, error_type='"enrichRegion, no Dubai"')#追加过滤原因
            temp_filter_file.write(line)
            print 'filer_file: ', enrich_region
    elif 'enrichRegion' not in row['actor']['location'].keys() and 'userDisplayName' in row['actor']['location'].keys():#enrichRegion不在location里面，但是userDisplayName在location里面
        user_display_name = row['actor']['location']['userDisplayName'].title()
        if 'Dubai' in user_display_name or 'دبي' in row['actor']['location']['userDisplayName']:
            temp_dubai_file.write(line)
            print 'dubai_file: ', user_display_name
        else:#userDisplayName里面没有迪拜信息
            line = add_error_type(line, error_type='"userDisplayName, no enrichRegion, no Dubai"')#追加过滤原因
            temp_filter_file.write(line)
            print 'filer_file: ', user_display_name
    else:#两个字段都不在location里面，无法判断
        line = add_error_type(line, error_type='"no userDisplayName, no enrichRegion"')#追加过滤原因
        temp_filter_file.write(line)
        pass


def add_error_type(line, error_type):
    """
    用来向数据中追加该条数据被过滤掉的原因
    :param line: 一条数据
    :param error_type: 过滤的原因
    :return: 追加过滤原因之后的一条数据
    """
    tweet_type_list = ['"type":"reply"','"type":"tweet"','"type":"retweet"']#推文的三种状态，每一条推文必包含其中一个，且只有一个
    for tweet_type in tweet_type_list:
        if tweet_type in line:
            line = line.replace(tweet_type + '}', tweet_type + ',"errorType":' + error_type + '}')
            break
    return line


def get_data_file_name():
    """
    用于遍历所有保存原始数据的json文件，也只需要运行这一个文件即可
    :return:无返回内容
    """
    #下面三个路径可能需要改变。
    path_filter = 'D:/LiuQL/eHealth/twitter/data_filter/'#过滤掉的文件所在目录
    path_data = 'D:/LiuQL/eHealth/twitter/data/'#原始json数据所在的目录
    path_dubai = 'D:/LiuQL/eHealth/twitter/data_dubai/'#迪拜地区数据文件所在的目录
    file_name_list = os.listdir(path_data)#获得原始json文件所在目录里面的所有文件名称
    for file_name in file_name_list:
        print file_name
        if 'part-r' in file_name and '.json' in file_name:
            read_json(file_name,path_data, path_filter,path_dubai=path_dubai)

path = 'D:/LiuQL/eHealth/twitter/'
read_json('part-02.json',path_data=path,path_filter=path,path_dubai=path)
# read_json('no_enrichRegion.json')

# get_data_file_name()