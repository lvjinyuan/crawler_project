# -*- coding:utf-8 -*-
'''
Created on 7 Oct 2017

@author: eyaomai
'''
class ArticleIndicatorWarningSignal(object):
    '''2018.5.22 加了一个文章指标预警的类，原因是拆分了原表的sa_article_warning_signal为三个表
    方便修改和维护
    2018.6.11 加了一个字段event_id,文章指标预警还需对事件文章进行预警。
    '''
    def __init__(self, config_id, tid, channel_id, event_id, title, nature, level,
                 warning_type, warning_datetime, threshold_value,
                 data_value, author_name=None, author_id=None, channel_name=None,
                 forward=None, like=None, read=None, comment=None,
                 collection=None, content=None):
        '''
        Constructor
        '''
        self.config_id = config_id  # 20180624 Jondar
        self.tid = tid
        self.channel_id = channel_id
        self.event_id = event_id
        self.title = title
        self.nature = nature
        self.warning_type = warning_type
        self.warning_datetime = warning_datetime
        self.threshold_value = threshold_value
        self.data_value = data_value
        self.level = level
        self.author_name = author_name
        self.author_id = author_id
        self.channel_name = channel_name
        self.forward = forward
        self.like = like
        self.read = read
        self.comment = comment
        self.collection = collection
        self.content = content

class ArticleSensitiveWarningSignal(object):

    '''
    文章预警信号
    '''


    def __init__(self, tid, channel_id, nature, sid, level,
                 warning_type, warning_datetime, threshold_value,
                 data_value, author_name=None, author_id=None, channel_name=None,
                 forward=None, like=None, read=None, comment=None,
                 collection=None, content=None):
        '''
        Constructor
        '''
        self.tid = tid
        self.channel_id = channel_id
        #self.title = title
        self.nature = nature
        self.sid = sid
        self.level = level
        self.warning_type = warning_type
        self.warning_datetime = warning_datetime
        self.threshold_value = threshold_value
        self.data_value = data_value
        self.author_name = author_name
        self.author_id = author_id
        self.channel_name = channel_name
        self.forward = forward
        self.like = like
        self.read = read
        self.comment = comment
        self.collection = collection
        self.content = content



class EventIndicatorWarningSignal(object):
    def __init__(self, config_id, event_id, event_name, nature, level,
                 warning_type, warning_datetime, threshold_value, data_value,
                 author_name=None,author_id=None, art_title=None, content=None, 
                 channel=None, original=None, comment=None, read=None,max_hot=None):
        self.config_id = config_id  # 20180624 Jondar 预警ID
        self.event_id = event_id
        self.event_name = event_name
        self.nature = nature
        self.warning_type = warning_type
        self.warning_datetime = warning_datetime
        self.threshold_value = threshold_value
        self.data_value = data_value
        self.level = level
        self.idKey = str(level) +'_'+str(self.warning_type) + '_'+str(self.event_id)        
        self.author_name = author_name
        self.author_id = author_id
        self.channel = channel
        self.original = original
        self.max_hot = max_hot
        self.read = read
        self.comment = comment
        self.art_title = art_title
        self.content = content

    def __eq__(self, obj):
        if self.level == obj.level and self.event_id == obj.event_id and self.warning_type == obj.warning_type:
            return True
        return False

