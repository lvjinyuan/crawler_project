# -*- coding:utf-8 -*-
'''
Created on 1 Oct 2017

@author: eyaomai
'''
import sys, os
import time, math, datetime
from crawler.lib.common.conf import SAConfiguration

from crawler.lib.analysis.analysisbase import AnalyticsBase
from crawler.lib.common.constant import Constants
from crawler.lib.common.globalvar import GlobalVariable

sys.path.append(sys.argv[0][:sys.argv[0].rfind(os.path.join('com','naswork'))])

TIME_DELTA_30_DAYS = 30 * 24 * 60 * 60


class HeatAnalytics(AnalyticsBase):
    '''
    热度分析，针对每一篇文章、事件或者媒体进行分析
    '''
    def __init__(self, dbProxy, entity_id='', logger=None):
        '''
        Constructor
        '''
        super(HeatAnalytics, self).__init__(dbProxy, entity_id, logger)
        self.version = GlobalVariable.getSAConfDict()[''].getConf(SAConfiguration.CONF_HEAT_ALGORITHM)
    
    def analysis(self, obj, objType, version=None):
        '''
        @param obj: 对象，article或者event的实例
        @param objType:对象类型类型
        '''
        if version is None:
            version = self.version
        if version == Constants.HEAT_ALGORITHM_VERSION1:
            return self.__heatAlgorithmV1(obj, objType)
        elif version == Constants.HEAT_ALGORITHM_VERSION2:
            return self.__heatAlgorithmV2(obj, objType)
        else:
            self.logger.error('Heat algorithm version %s not recognized:', version)
            return 0
    
    def __heatAlgorithmV1(self, obj, objType):
        replyCountFactor = 0
        likeCountFactor =  0
        forwardCountFactor = 0
        readCountFactor = 0
        articleCountFactor = 0
        objStatistics = obj.statistics

        channel_type_id = GlobalVariable.getChannelMgmt().channelDict['CHANNEL_TYPE_ID']
        if channel_type_id == Constants.CHANNEL_TYPE_NEWS:
            replyCountFactor = 0.235
            articleCountFactor = replyCountFactor
        elif channel_type_id == Constants.CHANNEL_TYPE_LUNTAN:
            replyCountFactor = 0.03
            readCountFactor = 0.85
            articleCountFactor = replyCountFactor + readCountFactor
        elif channel_type_id == Constants.CHANNEL_TYPE_TIEBA:
            replyCountFactor = 0.045
            articleCountFactor = replyCountFactor
        elif channel_type_id == Constants.CHANNEL_TYPE_ZHIHU:
            replyCountFactor = 0.085
            readCountFactor = 0.12
            articleCountFactor = replyCountFactor + readCountFactor
        elif channel_type_id == Constants.CHANNEL_TYPE_WEIBO:
            replyCountFactor = 0.08
            forwardCountFactor = 0.12
            articleCountFactor = replyCountFactor + forwardCountFactor
        elif channel_type_id == Constants.CHANNEL_TYPE_WECHAT:
            readCountFactor = 0.15
            likeCountFactor = 0.06
            articleCountFactor = likeCountFactor + readCountFactor
        reply_count = float(objStatistics.reply_count) if objStatistics.reply_count is not None else 0
        like_count = float(objStatistics.like_count) if objStatistics.like_count is not None else 0
        forward_count = float(objStatistics.forward_count) if objStatistics.forward_count is not None else 0
        read_count = float(objStatistics.read_count) if objStatistics.read_count is not None else 0

        # 通过判断ObjType，进行不同的热度分析
        if objType == Constants.OBJECT_TYPE_EVENT:
            article_count = float(objStatistics.article_count) if objStatistics.article_count is not None else 0
            heat = reply_count * replyCountFactor + like_count * likeCountFactor \
                   + forward_count * forwardCountFactor + read_count * readCountFactor \
                   + article_count * articleCountFactor

            return heat

        heat = reply_count * replyCountFactor + like_count * likeCountFactor \
               + forward_count * forwardCountFactor + read_count * readCountFactor
        
        return heat

    def __heatAlgorithmV2(self, obj, objType):
        '''
        @param obj: 对象：文章、事件或者媒体 
        '''
        
        replyCountFactor = 0.8
        likeCountFactor =  3.0
        forwardCountFactor =  1.0
        objStatistics = obj.statistics
        if objType == Constants.OBJECT_TYPE_ARTICLE:
            if type(obj.publish_datetime)!=datetime.datetime:
                publish_datetime = datetime.datetime.strptime(obj.publish_datetime, '%Y-%m-%d %H:%M:%S')
            else:
                publish_datetime = obj.publish_datetime
            timeDeltaInSeconds = int(time.mktime(publish_datetime.timetuple()) -time.time())
        else:
            timeDeltaInSeconds = TIME_DELTA_30_DAYS 
        reply_count = float(objStatistics.reply_count) if objStatistics.reply_count is not None else 0
        like_count = float(objStatistics.like_count) if objStatistics.like_count is not None else 0
        forward_count = float(objStatistics.forward_count) if objStatistics.forward_count is not None else 0
        temp = reply_count *replyCountFactor + like_count * likeCountFactor\
         + forward_count * forwardCountFactor
        if  temp != 0.0:
            heat = math.log10(temp) + timeDeltaInSeconds / 45000
        else:
            heat = timeDeltaInSeconds / 45000
        return heat
