# -*- coding:utf-8 -*-
'''
Created on 2 Oct 2017

@author: eyaomai
'''
from crawler.lib.objectmodel.saobject import SAObjectBase
# from com.naswork.sentiment.common.constant import Constants
class Article(SAObjectBase):
    '''
    classdocs
    '''


    def __init__(self, tid, channel_id, title=None, content=None,
                 publish_datetime=None, url=None, author_id=None, 
                 author_name=None, publish_method=None, digest=None, 
                 heat=None, eventId=None, meta_info=None, entity=None,
                 add_datetime=None, remove_datetime=None, extinct=None,
                 extinct_datetime=None, classified_nature=None, update_datetime=None):
        '''
        Constructor
        '''
        self.tid = tid
        self.title = title        
        self.content = content
        self.publish_datetime = publish_datetime
        self.channel_id = channel_id
        self.publish_method = publish_method
        self.digest = digest
        self.url = url
        self.author_id = author_id
        self.author_name = author_name
        self.annotated_nature = -2
        self.classified_nature = -2
        self.add_datetime = None
        self.statistics = ArticleStatistics(tid, channel_id, heat=heat)
        self.eventId = eventId
        self.meta_info = meta_info
        self.entity = entity
        self.add_datetime = add_datetime
        self.remove_datetime = remove_datetime
        self.extinct = extinct
        self.extinct_datetime = extinct_datetime
        self.classified_nature = classified_nature
        self.update_datetime = update_datetime

    def __str__(self):
        basic= '\nBasic:\n\ttid:%s\n\ttitle:%s\n\turl:%s\n\tpublish_datetime:%s\n\tchannelid:%d\n\tauthorid:%s\n\tauthorname:%s\n\tcotent len:%s' %(
                    self.tid, self.title, self.url, self.publish_datetime, self.channel_id, self.author_id, self.author_name, 
                    len(self.content) if self.content is not None else 0)
        return basic+str(self.statistics)

    def __eq__(self, obj):
        if self.tid == obj.tid and self.channel_id == obj.channel_id:
            return True
        else:
            return False


class ArticleStatistics(object):
    def __init__(self, tid, channel_id, event_id=None, warning_type=None, update_datetime=None, read_count=None, reply_count=None, forward_count=None, like_count=None, collect_count=None, heat=None, index=None):
        self.tid = tid
        self.channel_id = channel_id
        self.event_id = event_id
        self.warning_type = warning_type
        self.update_datetime = update_datetime
        self.read_count = read_count
        self.reply_count = reply_count
        self.forward_count = forward_count
        self.like_count = like_count
        self.collect_count = collect_count
        self.heat = heat
        self.index = index

    def __str__(self):
        return '\nStatistics:\n\treadcount:%s\n\treply_count:%s\n\tforward_count:%s\n\tlike_count:%s\n\tcollect_count:%s\n\t' %(
                    self.read_count, self.reply_count, self.forward_count, self.like_count,self.collect_count)


class RemoveArticle(object):
    def __init__(self, tid, event_id, url, add_datetime, period, publish_datetime, publish_method, extinct,
                 extinct_datetime, title, author_id, author_name, digest, content, read_count, like_count, reply_count,
                 forward_count, collect_count, heat, channel_id, annotated_nature, classified_nature, update_datetime,
                 remove_datetime=None, marked=None, remove_period=None):
        self.tid = tid
        self.event_id = event_id
        self.url = url
        self.add_datetime = add_datetime
        self.period = period
        self.publish_datetime = publish_datetime
        self.publish_method = publish_method
        self.extinct = extinct
        self.extinct_datetime = extinct_datetime
        self.title = title
        self.author_id = author_id
        self.author_name = author_name
        self.digest = digest
        self.content = content
        self.read_count = read_count
        self.like_count = like_count
        self.reply_count = reply_count
        self.forward_count = forward_count
        self.collect_count = collect_count
        self.heat = heat
        self.channel_id = channel_id
        self.annotated_nature = annotated_nature
        self.classified_nature = classified_nature
        self.update_datetime = update_datetime
        self.remove_datetime = remove_datetime
        self.remove_period = remove_period
        self.marked = marked


class User_info(object):

    def __init__(self,tid,channel_id,url,fans_count =None,Concern_count=None,logo_url=None,content = None,user_name = None):
        self.tid = tid
        self.channel_id = channel_id
        self.url =url
        self.content =content
        self.fans_count = fans_count
        self.logo_url = logo_url
        self.Concern_count =Concern_count
        self.user_name = user_name





