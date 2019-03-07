# -*- coding:utf-8 -*-
'''
Created on 2 Oct 2017

@author: eyaomai
'''


class SAObjectBase(object):
    '''
    SA对象基类 
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.channel_id = None
        self.statistics = None


class ObjectStatistics(object):
    def __init__(self, object_id, object_type, publish_datetime, update_datetime, read_count,
                 like_count, reply_count, forward_count, collect_count, original_count,
                 article_count=None):
        self.object_id = object_id
        self.object_type = object_type
        self.publish_datetime = publish_datetime
        self.update_datetime = update_datetime
        self.read_count = read_count
        self.reply_count = reply_count
        self.forward_count = forward_count
        self.like_count = like_count
        self.collect_count = collect_count
        self.original_count = original_count
        self.article_count = article_count
