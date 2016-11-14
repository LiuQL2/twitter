# -*- coding: utf-8 -*-
#用来抓取课程的信息,里面可以同时下载照片,看看哪一种方法比较完整
import json
import csv
import sys
import re
reload(sys)
sys.setdefaultencoding('utf-8')



path = '/Volumes/LIUQL/Twitter/'

def read_json(file_name):
    json_file = open(path + file_name, 'r')#读取json

    # csv_file = open(path + file_name.split('.')[0] + '.csv', 'w')#新建csv文件,同名
    # csv_writer = csv.writer(csv_file)


    keys = unicode_to_utf8(json.loads(json_file.next())).keys()#寻找key名称,为写入第一行做准备
    print '$$$$',keys

    # csv_writer.writerow(keys)#在csv中写入属性名
    print '*****',type(json.loads(json_file.next()))
    for line in json_file:#循环判断json文件中的每一行
        row = json.loads(line)#提取json形式
        line = unicode_to_utf8(row)#转换编码
        # write_to_csv(line, csv_writer)#写入一行数据
        print type(row),row.keys()

    json_file.close()
    # csv_file.close()

#把unicode编码的字典转化为utf-8的编码形式
def unicode_to_utf8(dict):
    result = {}
    for key in dict.keys():
        result[key.encode('utf-8')] = dict[key].encode('utf-8')
    return result

#在csv文件中写入一行数据
def write_to_csv(dict,writer):
    row = []
    print dict.keys()
    for key in dict.keys():
        row.append(dict[key])
    writer.writerow(row)



read_json('part-01.json')