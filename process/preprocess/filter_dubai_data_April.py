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


class FilterData(object):
    def __init__(self,origin_data_file):
        self.origin_data_file = origin_data_file
        self.total_number = 0
        self.dubai_strict_number = 0
        self.dubai_no_strict_number = 0
        self.no_dubai_number = 0

    def filter_data(self,no_dubai_file_name, dubai_strict_file_name,duai_no_strict_file_name):
        data_file = open(self.origin_data_file, 'r')
        no_dubai_file = open(no_dubai_file_name, 'wb')
        dubai_strict_file = open(dubai_strict_file_name, 'wb')
        dubai_no_strict_file = open(duai_no_strict_file_name, 'wb')
        index = 1
        for line in data_file:
            self.total_number = self.total_number + 1
            record = Record(line)
            verify_result = record.verify_data()
            if verify_result['total'] == True and verify_result['half'] == True:
                dubai_strict_file.write(line)
                self.dubai_strict_number = self.dubai_strict_number + 1
                print index, 'meet the condition: 1 or 2, and meet 3', line
            elif verify_result['total'] == False and verify_result['half'] == True:
                dubai_no_strict_file.write(line)
                self.dubai_no_strict_number = self.dubai_no_strict_number + 1
                print index,'meet the condition: 1 or 2, but not meet 3', line
            else:
                no_dubai_file.write(line)
                self.no_dubai_number = self.no_dubai_number + 1
                print index,'not meet the condition: 1 or 2', line
            index = index + 1

        data_file.close()
        no_dubai_file.close()
        dubai_no_strict_file.close()
        dubai_strict_file.close()

    def process_info(self):
        print 'number of total:', self.total_number
        print 'number of No Dubai (not meet 1 or 2):', self.no_dubai_number
        print 'number of Dubai (strict:meet the condition: 1 or 2, and meet 3):', self.dubai_strict_number
        print 'number of Dubai (no strict: meet the condition: 1 or 2, but not meet 3) :', self.dubai_no_strict_number





class Record(object):
    def __init__(self,string_data):
        self.data = json.loads(string_data)
        self.wether_dubai = True


    def verify_data(self):
        # if self.procedure_first() or self.procedure_second():
        if self.procedure_second():
            first_second = True
        else:
            first_second = False

        third = self.procedure_third()

        if first_second == True and third == True:
            total = True
            half = True
        elif first_second == True and third == False:
            total = False
            half = True
        else:
            total= False
            half = False

        return {'total':total, 'half':half}


    def procedure_first(self):
        if self.verify_enrichCountry() or self.verify_enrichCountryCode():
            return True
        else:
            return False

    def procedure_second(self):
        if self.verify_enrichDisplayName() or self.verify_enrichRegion() or self.verify_userDisplayName():
            return True
        else:
            return False

    def procedure_third(self):
        if self.verify_utcOffset() and self.verify_twitterTimeZone() and self.verify_tweetLocationName():
            return True
        else:
            return False



    def verify_enrichCountry(self):
        if self.data['actor']['location'] != None and 'enrichCountry' in self.data['actor']['location'].keys():
            enrichCountry = self.data['actor']['location']['enrichCountry']
            if enrichCountry != None:
                if 'united arab bmirates' not in enrichCountry.lower() and u'الإمارات العربية المتحدة' not in enrichCountry:
                    return False
                else:
                    return True
            else:
                return True
        else:
            return True

    def verify_enrichCountryCode(self):
        if self.data['actor']['location'] != None and  'enrichCountryCode' in self.data['actor']['location'].keys():
            enrichCountryCode = self.data['actor']['location']['enrichCountryCode']
            if enrichCountryCode != None:
                if 'ae' != enrichCountryCode:
                    return False
                else:
                    return True
            else:
                return True
        else:
            return True


    def verify_enrichDisplayName(self):
        if self.data['actor']['location'] != None and  'enrichDisplayName' in self.data['actor']['location'].keys():
            enrichDisplayName = self.data['actor']['location']['enrichDisplayName']
            if enrichDisplayName != None:
                if 'dubai' not in enrichDisplayName.lower() and u'دبي' not in enrichDisplayName:
                    return False
                else:
                    return True
            else:
                return False
        else:
            return False


    def verify_enrichRegion(self):
        if self.data['actor']['location'] != None and  'enrichRegion' in self.data['actor']['location'].keys():
            enrichRegion = self.data['actor']['location']['enrichRegion']
            if enrichRegion != None:
                if 'dubai' not in enrichRegion.lower() and u'دبي' not in enrichRegion:
                    return False
                else:
                    return True
            else:
                return False
        else:
            return False


    def verify_userDisplayName(self):
        if self.data['actor']['location'] != None and  'userDisplayName' in self.data['actor']['location'].keys():
            userDisplayName = self.data['actor']['location']['userDisplayName']
            if userDisplayName != None:
                if 'dubai' not in userDisplayName.lower() and u'دبي' not in userDisplayName:
                    return False
                else:
                    return True
            else:
                return False
        else:
            return False


    def verify_utcOffset(self):
        if 'utcOffset' in self.data['actor'].keys():
            utcOffset = self.data['actor']['utcOffset']
            if utcOffset == None:
                print 'utcoffset: None'
                time.sleep(5)
            if utcOffset != 0:
                if utcOffset != 14400:
                    return False
                else:
                    return True
            else:
                return True
        else:
            return True


    def verify_twitterTimeZone(self):
        if 'twitterTimeZone' in self.data['actor'].keys():
            twitterTimeZone = self.data['actor']['twitterTimeZone']
            if twitterTimeZone != None:
                if 'abu dhabi' not in twitterTimeZone.lower():
                    return False
                else:
                    return True
            else:
                return True
        else:
            return True



    def verify_tweetLocationName(self):
        if 'location' in self.data['tweet'].keys() and self.data['tweet']['location'] != None and 'name' in self.data['tweet']['location'].keys():
            name = self.data['tweet']['location']['name']
            if name != None:
                if 'dubai' not in name.lower() and u'دبي' not in name:
                    return False
                else:
                    return True
            else:
                return True
        else:
            return True


if __name__ == '__main__':
    origin_data_path = '/pegasus/twitter-p-or-t-uae-201603.json.dxb/'
    save_path = '/pegasus/harir/Qianlong/data/March/'

    # file_name_list = get_dirlist(origin_data_path,key_word_list=['201604','.json'])
    file_name_list = get_dirlist(origin_data_path,key_word_list=['f424-4f7c-b21c-33b34d491577','.json'],no_key_word_list=['.crc'])
    # file_name_list = [ 'no_dubai_twitter-p-or-t-uae-201604.json']
    print len(file_name_list)
    print file_name_list
    time.sleep(10)
    total_number = 0
    no_dubai_number = 0
    dubai_strict_number = 0
    dubai_no_strict_number = 0

    for file_name in file_name_list:
        no_dubai_file = 'no_dubai_' + file_name
        dubai_strict_file = 'dubai_strict_' + file_name
        dubai_no_strict_file = 'dubai_no_strict_' + file_name
        filter_data = FilterData(origin_data_file=origin_data_path + file_name)
        filter_data.filter_data(no_dubai_file_name=save_path + no_dubai_file,dubai_strict_file_name=save_path + dubai_strict_file, duai_no_strict_file_name=save_path + dubai_no_strict_file)
        filter_data.process_info()
        total_number = total_number + filter_data.total_number
        no_dubai_number = no_dubai_number + filter_data.no_dubai_number
        dubai_strict_number = dubai_strict_number + filter_data.dubai_strict_number
        dubai_no_strict_number = dubai_no_strict_number + filter_data.dubai_no_strict_number

    print 'number of total:', total_number
    print 'number of No Dubai (not meet 1 or 2):', no_dubai_number
    print 'number of Dubai (strict:meet the condition: 1 or 2, and meet 3):', dubai_strict_number
    print 'number of Dubai (no strict: meet the condition: 1 or 2, but not meet 3) :', dubai_no_strict_number
