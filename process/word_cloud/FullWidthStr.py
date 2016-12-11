# !/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class FullWidthStr(object):
    def __init__(self, string):
        self.string = string
        self.full_width = False
        self.one_full_width = False

    def Q2B(self, uchar):
        """全角转半角"""
        # uchar = self.string
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
            self.full_width = True
            self.one_full_width = True
        if inside_code < 0x0020 or inside_code > 0x7e:  # 转完之后不是半角字符返回原来的字符
        # if 0 < inside_code and 65535 > inside_code:
            self.full_width = False
            # print '*****',inside_code
            return uchar
            # return unichr(inside_code)
            # if self.one_full_width == True:
            #     return unichr(inside_code)
            # else:
            #     return uchar
        return unichr(inside_code)

    def stringQ2B(self):
        """把字符串全角转半角"""

        ustring = self.string
        for uchar in ustring:
            self.Q2B(uchar)
        return "".join([self.Q2B(uchar) for uchar in ustring])

