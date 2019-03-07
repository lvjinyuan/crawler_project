# -*- coding:utf-8 -*-
'''
Created on 3 Oct 2017

@author: eyaomai
'''
from crawler.lib.common.constant import Constants

class ChannelManager(object):
    '''
    媒体管理器
    '''


    def __init__(self, dbProxy):
        self.dbProxy = dbProxy
        self.channelDict = dict()
        self.channelTypeDict = dict()
        self.fetchValidChannel(True)
    
    def fetchValidChannel(self, refresh=False):
        if refresh is True or len(self.channelDict) == 0 or len(self.channelTypeDict) == 0:
            #获取所有合法channel
            sql = '''select c.channel_id, c.channel_name, c.url, 
            c.channel_type_id, ct.channel_type_name, crawler_code, search_ranges
            from %s as c, %s as ct 
            where c.enable="Y" and c.channel_type_id = ct.channel_type_id
            ''' % (Constants.TABLE_SA_CHANNEL, Constants.TABLE_SA_CHANNEL_TYPE)
            self.dbProxy.execute(sql)
            results = self.dbProxy.fetchall()
            for item in results:
                self.channelDict[item[0]] = Channel(item[0], item[1], item[2], item[3], item[4], True, item[5], item[6])
            
            #获取所有channeltype
            sql = 'select channel_type_id, channel_type_name from %s' % Constants.TABLE_SA_CHANNEL_TYPE
            self.dbProxy.execute(sql)
            results = self.dbProxy.fetchall()
            for item in results:
                self.channelTypeDict[item[0]] = ChannelType(item[0], item[1])

class ChannelType(object):
    def __init__(self, channel_type_id, channel_type_name):
        self.channel_type_id = channel_type_id
        self.channel_type_name = channel_type_name

class Channel(object): 
    def __init__(self, channel_id, channel_name, url, channel_type_id, channel_type_name, enable, crawler_code, search_ranges):
        self.channel_id = channel_id
        self.channel_name = channel_name
        self.url = url
        self.channel_type_id = channel_type_id
        self.channel_type_name = channel_type_name
        self.enable = enable
        self.crawler_code = crawler_code
        self.search_ranges = search_ranges