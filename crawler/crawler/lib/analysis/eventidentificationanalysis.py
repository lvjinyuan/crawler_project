# -*- coding:utf-8 -*-
'''
Created on 3 March 2018

@author: Jondar
'''
import json
import platform
import jieba
import jieba.analyse
import os
import sys
import re
from datetime import timedelta
from datetime import datetime

from conf import os_file
from crawler.lib.analysis.analysisbase import AnalyticsBase, Tools
from crawler.lib.common.globalvar import GlobalVariable
from crawler.lib.common.constant import Constants
from crawler.lib.objectmodel.article import Article
from crawler.lib.analysis.heatanalysis import HeatAnalytics
from crawler.lib.objectmodel.saobject import ObjectStatistics,SAObjectBase
from crawler.lib.objectmodel.warningsignal import EventIndicatorWarningSignal

# reload(sys)
# sys.setdefaultencoding('utf8')


def encodeText(content):
    return content.replace('"','\\"').replace("%", "\%")


class SingleEventBackTrack(AnalyticsBase):
    '''
    判断新增事件和修改事件之后的分析
    '''
    def __init__(self, dbProxy, entity_id, event_id, logger=None):
        super(SingleEventBackTrack, self).__init__(dbProxy, entity_id, logger)
        self.event_id = event_id

    def analysis(self, back_track=True, **kwargs):
        # 文章识别新建事件
        if back_track:
            event_identify = SingleEventIdentificationAnalysis(self.dbProxy, self.entity_id, self.event_id)
            event_identify.analysis()

        # 媒体分析
        event_media = SingleEventMediaStatics(self.dbProxy, self.entity_id, self.event_id)
        event_media.analysis()

        # 友好度分析
        event_friendly = FriendlyAnalytics(self.dbProxy, self.entity_id, self.event_id)
        event_friendly.analysis()

        # 热词分析
        event_heatword = HeatWordsAnalytics(self.dbProxy, self.entity_id, self.event_id)
        event_heatword.analysis()

        # 事件预警
        event_warning = IndicatorWarningAnalytics(self.dbProxy, self.entity_id, self.event_id)
        event_warning.analysis()

        if os.path.exists('lock.log'):
            os.remove('lock.log')
        else:
            self.logger.debug('End all analyse')


class SingleEventIdentificationAnalysis(AnalyticsBase):
    '''
    文章识别新建事件
    '''
    def __init__(self, dbProxy, entity_id, event_id, logger=None):
        super(SingleEventIdentificationAnalysis, self).__init__(dbProxy, entity_id, logger)
        self.event_id = event_id

        stopWordsFile = self.saConf.getConf(self.saConf.CONF_STOPS_WORDS_FILE)
        userDictFile = self.saConf.getConf(self.saConf.CONF_USER_DICT_FILE)
        if 'window' in platform.system().lower():
            words = os_file.current_dir
            stopWordsFile = os.path.join(words, stopWordsFile)
            userDictFile = os.path.join(words, userDictFile)
            # stopWordsFile = stopWordsFile.replace('/','\\')
            # userDictFile = userDictFile.replace('/','\\')
        self.stopWords = Tools.fetchStopWords(stopWordsFile)
        jieba.load_userdict(userDictFile)       

    def analysis(self):
        entityEventDict = GlobalVariable.getEventMgmt().entityEventDict
        if entityEventDict is None or self.entity_id not in entityEventDict:
            self.logger.warn('Entity %d not found in system', self.entity_id)
        
        eventDict = entityEventDict[self.entity_id]

        self.__analysisEvent(eventDict[self.event_id])

    def __analysisEvent(self, event):
        articleList = self.__fetchArticle()
        self.logger.debug(len(articleList))
        hitArticleList = list()
        algorithm = event.algorithm
        algorithm_conf = event.algorithm_conf
        if algorithm == Constants.EVENT_IDENTIFICATION_ALGORITHM_KW:
            jo = json.loads(algorithm_conf)
            keywordList = jo[Constants.EVENT_IDENTIFICATION_ALGORITHM_KW_FIELD_KW]
            nonKeyword = jo[Constants.EVENT_IDENTIFICATION_ALGORITHM_KW_FIELD_NON_KW]
            for article in articleList:
                title = article.title
                content = article.content
                if self.__isExists(keywordList, title) or \
                    self.__isExists(keywordList, content):
                    if nonKeyword != "":
                        if not self.__isExists(nonKeyword, title) and \
                                not self.__isExists(nonKeyword, content):
                            hitArticleList.append(article)
                    else:
                        hitArticleList.append(article)
        elif algorithm == Constants.EVENT_IDENTIFICATION_ALGORITHM_SIMILARITY:
            jo = json.loads(algorithm_conf)
            for article in articleList:
                title = article.title
                titleWordList = Tools.segment(title, self.stopWords)
                keywordListStr = jo[Constants.EVENT_IDENTIFICATION_ALGORITHM_KW_FIELD_KW]
                keywordList = keywordListStr.split(',')
                keywordSetList = map(lambda x: set(x.split('|')), keywordList)
                keywordListList = list()
                for keywordSetItem in keywordSetList:
                    newList = list()
                    if len(keywordListList) > 0:
                        keywordItemList = list(keywordSetItem)
                        if len(keywordItemList)>1:
                            for keyword in keywordItemList[1:]:
                                for result in keywordListList:
                                    newItem = result[:]
                                    newItem.append(keyword)
                                    newList.append(newItem)
                        keyword = keywordItemList[0]
                        for result in keywordListList:
                            result.append(keyword)
                    else:
                        for keyword in keywordSetItem:
                            newList.append([keyword])
                    keywordListList.extend(newList)
                for keywordList in keywordListList:
                    if Tools.get_similarity(titleWordList, keywordList) > 0.8:
                        hitArticleList.append(article)
                        break
        self.logger.debug(len(hitArticleList))
        self.__updateToArticleTable(hitArticleList,
                                    Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id, self.event_id)
        self.__updateToArticleHistoryTable(hitArticleList,
                                           Constants.TABLE_SA_EVENT_ARTICLE_HISTORY + Constants.TABLE_NAME_DELIMITER + self.entity_id, self.event_id)

    def __fetchEventTime(self):
        '''
        获取事件回溯的起始时间和结束时间
        :param event:
        :return:
        '''
        selectSql = '''
            SELECT start_datetime, end_datetime from %s where event_id=%d 
        '''
        tableName = Constants.TABLE_SA_EVENT + Constants.TABLE_NAME_DELIMITER + self.entity_id

        sql = selectSql % (tableName, self.event_id)
        self.dbProxy.execute(sql)
        result = self.dbProxy.fetchall()

        return (result[0][0],
                result[0][1])

    def __isExists(self, keywordListStr, s):
        k1 = keywordListStr.split(',')
        match = True
        for k in k1:
            if not Tools.isExists(k, s):
                match = False
                break
        return match

    def __fetchArticle(self):
        '''
        获取数据库中某段时间发表的文章
        '''
        selectSql = '''select * 
                        from %s
                        where publish_datetime between '%s' and '%s' 
        '''
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_datetime, end_datetime = self.__fetchEventTime()
        if end_datetime is None:
            end_datetime = now
        tableName = Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        sql = selectSql % (tableName, start_datetime, end_datetime)

        self.dbProxy.execute(sql)
        results = self.dbProxy.fetchall()
        self.logger.debug(len(results))

        articleList = list()
        for item in results:
            article = Article(tid=item[0], url=item[1], publish_datetime=item[3], publish_method=item[4],
                              title=item[7], author_id=item[8], author_name=item[9], content=item[11],
                              heat=item[17], channel_id=item[18], classified_nature=item[20], entity=self.entity_id)
            article.statistics.read_count = item[12]
            article.statistics.like_count = item[13]
            article.statistics.reply_count = item[14]
            article.statistics.forward_count = item[15]
            article.statistics.collect_count = item[16]
            articleList.append(article)
        return articleList

    def __delEventArticle(self, hitArticleList, tableName):
        '''
            检查数据库中的文章跟回溯过后的文章是否相同，
            若数据库的文章不存在回溯过后的文章中，则删除数据库中的文章。
            :return:
        '''
        articleList = hitArticleList
        selectSql = '''
                    SELECT tid, channel_id, event_id FROM %s WHERE event_id=%d
                '''
        self.dbProxy.execute(selectSql % (tableName, self.event_id))
        results = self.dbProxy.fetchall()
        eventArticleDict = dict()
        for item in results:
            article = Article(item[0], item[1], eventId=item[2])
            index = str(item[0]) + '_' + str(item[1]) + '_' + str(item[2])
            eventArticleDict[index] = article

        insertArticleList = list()

        if len(eventArticleDict) > 0 or len(articleList) > 0:
            indexList = list()
            for article in articleList:
                # 当sa_event_article_<entity>中数据为0时
                if len(eventArticleDict) == 0:
                    insertArticleList.append(article)
                else:
                    for eventArticle in eventArticleDict.values():
                        if eventArticle.tid == article.tid and eventArticle.channel_id == article.channel_id:
                            index = str(eventArticle.tid) + '_' + str(eventArticle.channel_id) + '_' + str(eventArticle.eventId)
                            indexList.append(index)

            for index in indexList:
                del eventArticleDict[index]
            # 删除之后再进行判断
            if len(eventArticleDict) > 0:
                # 删除sa_event_article_<entity>修改关键词后没有回溯到的文章
                deleteSql = '''DELETE FROM %s WHERE ''' % tableName

                whereClauseList = list()
                for delArticle in eventArticleDict.values():
                    whereClauseList.append('(event_id=%s and tid="%s" and channel_id=%s)' % (
                        delArticle.eventId,
                        delArticle.tid,
                        delArticle.channel_id
                    ))
                self.logger.debug(u'删除在数据库中的%d篇文章--事件id:%d' % (len(eventArticleDict), self.event_id))
                self.dbProxy.execute(deleteSql + ' or '.join(whereClauseList))
                self.dbProxy.commit()

    def __updateToArticleTable(self, articleList, tableName, eventId=None, hasMetaInfo=False):
        '''
        更新到文章表
        @param tableName: 全局文章表、实体文章表或者实体事件文章表
        @param eventId: 如果更新到实体事件文章表，则需要提供事件id，否则为None
        '''
        n = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if eventId is None:
            eventIdFieldName = ''
            eventIdFieldValue = ''
        else:
            eventIdFieldName = 'EVENT_ID,'
            eventIdFieldValue = str(eventId)+','
        if hasMetaInfo:
            metaInfoFieldName = ',META_INFO'
            metaInfoOnUpdate = ',META_INFO=VALUES(META_INFO)'
        else:
            metaInfoFieldName = ''
            metaInfoOnUpdate = ''

        # deleteSql = 'DELETE FROM %s where event_id=%s' % (tableName, str(eventId))
        # self.dbProxy.execute(deleteSql)

        insertSql = '''
        INSERT INTO %s (TID, %s CHANNEL_ID, URL, ADD_DATETIME, PUBLISH_DATETIME, PUBLISH_METHOD,
            TITLE, AUTHOR_ID, AUTHOR_NAME, DIGEST, CONTENT, READ_COUNT,LIKE_COUNT, REPLY_COUNT,
            FORWARD_COUNT, COLLECT_COUNT, HEAT, CLASSIFIED_NATURE, UPDATE_DATETIME %s)
        VALUES %s 
        ON DUPLICATE KEY UPDATE READ_COUNT=VALUES(READ_COUNT), LIKE_COUNT=VALUES(LIKE_COUNT), 
        REPLY_COUNT = VALUES(REPLY_COUNT), FORWARD_COUNT=VALUES(FORWARD_COUNT), 
        COLLECT_COUNT = VALUES(COLLECT_COUNT), CLASSIFIED_NATURE=VALUES(CLASSIFIED_NATURE),
        HEAT = VALUES(HEAT), UPDATE_DATETIME=VALUES(UPDATE_DATETIME)
        %s
        '''
        valueList = list()
        self.__delEventArticle(articleList, tableName)
        for article in articleList:
            statistics = article.statistics
            if hasMetaInfo:
                metaInfoFieldValue = ',"'+encodeText(article.meta_info)+'"' if article.meta_info is not None else ','+Constants.DEFAULT_STR
            else:
                metaInfoFieldValue = ''
            valueList.append('("%s", %s %d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s","%s", %s, %s, %s, %s, %s, %s, %s, "%s" %s)' % (
                        article.tid,
                        eventIdFieldValue,
                        article.channel_id,
                        article.url,
                        n,
                        article.publish_datetime if article.publish_datetime is not None else Constants.DEFAULT_PUBLISH_DATETIME,
                        article.publish_method if article.publish_method is not None else Constants.DEFAULT_PUBLISH_METHOD,
                        encodeText(article.title),
                        article.author_id if article.author_id is not None else Constants.DEFAULT_AUTHOR_ID,
                        article.author_name if article.author_name is not None else Constants.DEFAULT_AUTHOR_NAME,
                        encodeText(article.digest) if article.digest is not None else Constants.DEFAULT_DIGEST,
                        encodeText(article.content),
                        statistics.read_count if statistics.read_count is not None else Constants.DEFAULT_NUM,
                        statistics.like_count if statistics.like_count is not None else Constants.DEFAULT_NUM,
                        statistics.reply_count if statistics.reply_count is not None else Constants.DEFAULT_NUM,
                        statistics.forward_count if statistics.forward_count is not None else Constants.DEFAULT_NUM,
                        statistics.collect_count if statistics.collect_count is not None else Constants.DEFAULT_NUM,
                        statistics.heat if statistics.heat is not None else Constants.DEFAULT_NUM,
                        article.classified_nature,
                        n,
                        metaInfoFieldValue
                        ))
        if len(valueList) > 0:
            self.dbProxy.execute(insertSql % (tableName, eventIdFieldName, metaInfoFieldName, ','.join(valueList), metaInfoOnUpdate))
            self.logger.debug(u'最终写入数据库有%d文章, 事件id--%d' % (len(valueList), self.event_id))
            self.dbProxy.commit()

    def __updateToArticleHistoryTable(self, articleList, tableName, eventId=None):
        '''
        更新到文章历史表
        @param tableName: 当前文章表：全局文章表、实体文章表或者实体事件文章表
        @param eventId: 如果更新到实体事件文章表，则需要提供事件id，否则为None
        '''
        # self.logger.error('history')
        n = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if eventId is None:
            eventIdFieldName = ''
            eventIdFieldValue = ''
        else:
            eventIdFieldName = 'EVENT_ID,'
            eventIdFieldValue = str(eventId)+','
        insertSql = '''
        INSERT INTO %s (TID, %s CHANNEL_ID,
            READ_COUNT,LIKE_COUNT, REPLY_COUNT,
            FORWARD_COUNT, COLLECT_COUNT, HEAT, ADD_DATETIME)
        VALUES %s 
        '''
        valueList = list()
        for article in articleList:
            statistics = article.statistics
            valueList.append('("%s", %s %d, %s, %s, %s, %s, %s, %s, "%s")' % (
                        article.tid,
                        eventIdFieldValue,
                        article.channel_id,
                        statistics.read_count if statistics.read_count is not None else Constants.DEFAULT_NUM,
                        statistics.like_count if statistics.like_count is not None else Constants.DEFAULT_NUM,
                        statistics.reply_count if statistics.reply_count is not None else Constants.DEFAULT_NUM,
                        statistics.forward_count if statistics.forward_count is not None else Constants.DEFAULT_NUM,
                        statistics.collect_count if statistics.collect_count is not None else Constants.DEFAULT_NUM,
                        statistics.heat if statistics.heat is not None else Constants.DEFAULT_NUM,
                        n
                        ))
        if len(valueList)>0:
            self.dbProxy.execute(insertSql % (tableName, eventIdFieldName, ','.join(valueList)))
            self.dbProxy.commit()


class SingleEventMediaStatics(AnalyticsBase):
    '''
    新建事件数据统计
    '''
    def __init__(self, dbProxy, entity_id, event_id, logger=None):
        '''
        Constructor
        '''
        super(SingleEventMediaStatics, self).__init__(dbProxy, entity_id, logger)
        self.event_id = event_id

    def analysis(self):
        self.logger.info('[Event]Begin to analyse for %s', self.event_id)

        # table_name: sa_event_article_<entity>
        selectTableName = Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        # table_name: sa_event_media_statistics_<entity>
        deleteTableName = Constants.TABLE_SA_EVENT_MEDIA_STATISTICS + Constants.TABLE_NAME_DELIMITER + self.entity_id

        # 0.清除当前统计表
        deleteSql = 'delete from '+deleteTableName + ' where event_id=' + str(self.event_id)
        self.dbProxy.execute(deleteSql)
        self.dbProxy.commit()
        # 计算时间段
        period = Tools.getPeriod()

        # 1.按照事件和媒体分组计算
        selectSql = '''select event_id, channel_id, sum(read_count), sum(like_count), 
                sum(reply_count), sum(forward_count),  sum(collect_count), count(*)
                from %s
                where event_id = %d and MANUAL_REMOVE = 'N'
                group by channel_id
        ''' % (selectTableName, self.event_id)
        self.logger.debug('[EventMediaStatics]Begin to analyse for %d', self.event_id)

        self.__analysisEventMedia(selectSql, period)

        self.logger.debug('[EventMediaStatistics]End analyse for %s' % self.event_id)

    def __analysisEventMedia(self, selectSql, period):
        '''
        @param selectSql: 计算所需的sql 
        '''
        # insert_table_name = sa_event_media_statistics_<entity>
        insertTableName = Constants.TABLE_SA_EVENT_MEDIA_STATISTICS + Constants.TABLE_NAME_DELIMITER + self.entity_id
        # insert_history_table_name = sa_event_media_history_statistics_<entity>
        insertHistoryTableName = Constants.TABLE_SA_EVENT_MEDIA_HISTORY_STATISTICS + Constants.TABLE_NAME_DELIMITER + self.entity_id

        insertSql = '''
        INSERT INTO %s (event_id, channel_id, add_datetime, period, original_count,
        read_count, like_count, reply_count, forward_count, collect_count, heat)
        VALUES %s
        '''
        insertHistorySql = '''
        INSERT INTO %s (EVENT_ID, CHANNEL_ID, ADD_DATETIME, PERIOD, ORIGINAL_COUNT, READ_COUNT, LIKE_COUNT,
            REPLY_COUNT, FORWARD_COUNT, COLLECT_COUNT, HEAT)
        VALUES %s
        ON DUPLICATE KEY UPDATE ADD_DATETIME=VALUES(ADD_DATETIME), ORIGINAL_COUNT=VALUES(ORIGINAL_COUNT), 
            READ_COUNT=VALUES(READ_COUNT), LIKE_COUNT=VALUES(LIKE_COUNT), REPLY_COUNT=VALUES(REPLY_COUNT),
            FORWARD_COUNT=VALUES(FORWARD_COUNT), COLLECT_COUNT=VALUES(COLLECT_COUNT), HEAT=VALUES(HEAT)
        '''

        insertValueList = list()
        insertValueTemplateStr = '(%s, %s, "%s", %s, %s, %s, %s, %s, %s, %s, %f)'
        n = datetime.now()
        currentDateTimeStr = n.strftime('%Y-%m-%d %H:%M:%S')
        self.dbProxy.execute(selectSql)
        results = self.dbProxy.fetchall()
        self.dbProxy.commit()
        # self.logger.debug(results)

        heatAna = HeatAnalytics(self.dbProxy, self.entity_id)
        eventValueDict = dict()
        mediaValueDict = dict()
        for item in results:
            objSta = ObjectStatistics(
                                   str(item[0])+'_'+str(item[1]),
                                   Constants.OBJECT_TYPE_EVENT,
                                   None,
                                   None,
                                   item[2], item[3], item[4], item[5], item[6], item[7]
                                   )
            obj = SAObjectBase()
            obj.channel_id = item[1]
            obj.statistics = objSta
            heat = heatAna.analysis(obj, Constants.OBJECT_TYPE_EVENT)
            insertValueList.append(insertValueTemplateStr % (
                                        item[0], item[1], currentDateTimeStr, period,
                                        item[7] if item[7] is not None else Constants.DEFAULT_NUM,
                                        item[2] if item[2] is not None else Constants.DEFAULT_NUM, 
                                        item[3] if item[3] is not None else Constants.DEFAULT_NUM, 
                                        item[4] if item[4] is not None else Constants.DEFAULT_NUM, 
                                        item[5] if item[5] is not None else Constants.DEFAULT_NUM, 
                                        item[6] if item[6] is not None else Constants.DEFAULT_NUM,
                                        heat
                                    ))
            record = TableRecord(item[2] if item[2] is not None else 0, 
                                 item[3] if item[3] is not None else 0, 
                                 item[4] if item[4] is not None else 0, 
                                 item[5] if item[5] is not None else 0, 
                                 item[6] if item[6] is not None else 0, 
                                 item[7] if item[7] is not None else 0, 
                                 heat)
            if item[0] not in eventValueDict:
                eventValueDict[item[0]] = record 
            else:
                eventValueDict[item[0]].read_count += record.read_count
                eventValueDict[item[0]].reply_count += record.reply_count
                eventValueDict[item[0]].forward_count += record.forward_count
                eventValueDict[item[0]].like_count += record.like_count
                eventValueDict[item[0]].collect_count += record.collect_count
                eventValueDict[item[0]].article_count += record.article_count
                eventValueDict[item[0]].heat += record.heat
            if item[1] not in mediaValueDict:
                mediaValueDict[item[1]] = record
            else:
                mediaValueDict[item[1]].read_count += record.read_count
                mediaValueDict[item[1]].reply_count += record.reply_count
                mediaValueDict[item[1]].forward_count += record.forward_count
                mediaValueDict[item[1]].like_count += record.like_count
                mediaValueDict[item[1]].collect_count += record.collect_count
                mediaValueDict[item[1]].article_count += record.article_count
                mediaValueDict[item[1]].heat += record.heat

        if len(insertValueList) > 0:
            # 插入当前表
            sql = insertSql % (insertTableName, ','.join(insertValueList))
            self.dbProxy.execute(sql)
            # 插入历史表
            sql = insertHistorySql % (insertHistoryTableName, ','.join(insertValueList))
            # self.logger.debug('INSERT SQL to history table: %s', sql)
            self.dbProxy.execute(sql)
            self.dbProxy.commit()
        
        eventValueList = list()
        for eventId in eventValueDict:
            record = eventValueDict[eventId]
            eventValueList.append(insertValueTemplateStr % (
                                        eventId, 0, currentDateTimeStr, period,
                                        record.article_count if record.article_count is not None else Constants.DEFAULT_NUM,
                                        record.read_count if record.read_count is not None else Constants.DEFAULT_NUM, 
                                        record.like_count if record.like_count is not None else Constants.DEFAULT_NUM, 
                                        record.reply_count if record.reply_count is not None else Constants.DEFAULT_NUM, 
                                        record.forward_count if record.forward_count is not None else Constants.DEFAULT_NUM, 
                                        record.collect_count if record.collect_count is not None else Constants.DEFAULT_NUM, 
                                        record.heat
                                    )
                                  )
        if len(eventValueList) > 0:
            # 插入当前表
            sql = insertSql % (insertTableName, ','.join(eventValueList))
            # self.logger.debug('INSERT SQL to current table: %s', sql)
            self.dbProxy.execute(sql)
            # 插入历史表
            sql = insertHistorySql % (insertHistoryTableName, ','.join(insertValueList))
            # self.logger.debug('INSERT SQL to history table: %s', sql)
            self.dbProxy.execute(sql)
            self.dbProxy.commit()
        
        mediaValueList = list()
        for mediaId in mediaValueDict:
            record = mediaValueDict[mediaId]
            mediaValueList.append(insertValueTemplateStr % (
                                        0, mediaId, currentDateTimeStr, period,
                                        record.article_count if record.article_count is not None else Constants.DEFAULT_NUM,
                                        record.read_count if record.read_count is not None else Constants.DEFAULT_NUM, 
                                        record.like_count if record.like_count is not None else Constants.DEFAULT_NUM, 
                                        record.reply_count if record.reply_count is not None else Constants.DEFAULT_NUM, 
                                        record.forward_count if record.forward_count is not None else Constants.DEFAULT_NUM, 
                                        record.collect_count if record.collect_count is not None else Constants.DEFAULT_NUM, 
                                        record.heat
                                    )
                                  )
        if len(mediaValueList) > 0:
            # 插入当前表
            sql = insertSql % (insertTableName, ','.join(mediaValueList))
            # self.logger.debug('INSERT SQL to current table: %s', sql)
            self.dbProxy.execute(sql)
            # 插入历史表
            sql = insertHistorySql % (insertHistoryTableName, ','.join(insertValueList))
            # self.logger.debug('INSERT SQL to history table: %s', sql)
            self.dbProxy.execute(sql)
            self.dbProxy.commit()


class FriendlyAnalytics(AnalyticsBase):
    '''
    友好度分析
    '''

    def __init__(self, dbProxy, entity_id, event_id, logger=None):
        '''
        Constructor
        '''
        super(FriendlyAnalytics, self).__init__(dbProxy, entity_id, logger)
        self.event_id = event_id
        self.event_article_tableName = Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        self.media_trend_tableName = Constants.TABLE_SA_MEDIA_TREND + Constants.TABLE_NAME_DELIMITER + self.entity_id

    def analysis(self):

        # 获取各媒体的所有文章性质
        select_bychannelid_Sql = '''
                              SELECT classified_nature, count(classified_nature), channel_id 
                              FROM %s WHERE MANUAL_REMOVE = 'N'
                              GROUP BY channel_id, classified_nature 
                              ORDER BY channel_id, classified_nature    
                     ''' % self.event_article_tableName

        self.logger.debug('[Friendly]Begin analyse for %d' % self.event_id)
        self.__analysisChannelMediaFriend(select_bychannelid_Sql)

        # 获取某事件的性质
        select_byeventid_Sql = '''SELECT event_id,classified_nature, count(classified_nature),channel_id
                            FROM %s WHERE EVENT_ID = %d and MANUAL_REMOVE = 'N'
                            GROUP BY event_id,classified_nature,channel_id
                            ORDER BY event_id,channel_id
                      ''' % (self.event_article_tableName, self.event_id)

        self.__analysisEventMediaFriend(select_byeventid_Sql)

        self.logger.debug('[Friendly]End analyse for %d' % self.event_id)
        self.dbProxy.commit()

    def __analysisChannelMediaFriend(self, selectSql):
        '''
        :param selectSql: 获取某事件的媒体友好度计算数据
        '''

        self.dbProxy.execute(selectSql)
        channelList = self.dbProxy.fetchall()

        cList = {}
        cList[0] = {}

        List = {}
        List['unset'] = 0
        List['negative'] = 0
        List['neutral'] = 0
        List['positive'] = 0

        for item in channelList:

            if item[2] not in cList[0]:
                cList[0][item[2]] = [List['unset'], List['negative'], List['neutral'], List['positive']]

                if item[0] == -2:
                    cList[0][item[2]][0] = item[1]
                elif item[0] == -1:
                    cList[0][item[2]][1] = item[1]
                elif item[0] == 0:
                    cList[0][item[2]][2] = item[1]
                elif item[0] == 1:
                    cList[0][item[2]][3] = item[1]
            else:
                if item[0] == -2:
                    cList[0][item[2]][0] = item[1]
                elif item[0] == -1:
                    cList[0][item[2]][1] = item[1]
                elif item[0] == 0:
                    cList[0][item[2]][2] = item[1]
                elif item[0] == 1:
                    cList[0][item[2]][3] = item[1]

        cList_for_db = list()
        # 获取所有关于该实体的文章数量
        select_totalSql = '''SELECT count(*) FROM %s WHERE MANUAL_REMOVE = "N" ''' % self.event_article_tableName

        self.dbProxy.execute(select_totalSql)
        article_total = self.dbProxy.fetchall()
        self.dbProxy.commit()
        insertValueTemplate = '(%d,%d,%d,%d)'

        for countList in cList[0]:
            friend = round((cList[0][countList][3]-cList[0][countList][2]-cList[0][countList][1])/float(sum(cList[0][countList]))*100, 2)
            # 该媒体发表的关于这个实体的文章数量对比所有媒体关于这个实体的文章数量的影响力
            influence = round(sum(cList[0][countList])/float(article_total[0][0])*100, 2)

            cList_for_db.append(insertValueTemplate % (0, countList, friend, influence))

        # 插入的sql语句
        insertSql = 'INSERT INTO %s (event_id,channel_id,friend,influence) VALUES ' % self.media_trend_tableName

        if len(cList_for_db) > 0:
            # 先删除sa_media_trend表原有数据
            deleteSql = 'delete from %s where event_id=0 ' % self.media_trend_tableName
            self.dbProxy.execute(deleteSql)

            # 插入sa_media_trend表
            sql = (insertSql) + ','.join(cList_for_db)
            self.dbProxy.execute(sql)
            self.dbProxy.commit()

    def __analysisEventMediaFriend(self, selectSql):
        '''
        @param selectSql: 获取单个事件的媒体友好度计算数据
        '''

        select_totalSql = '''SELECT event_id,count(annotated_nature)
                        FROM %s WHERE MANUAL_REMOVE = 'N'
                        GROUP BY event_id
                              ''' % self.event_article_tableName

        self.dbProxy.execute(select_totalSql)
        results_total = self.dbProxy.fetchall()
        resultlist_total = {}

        for item_total in results_total:
            resultlist_total[item_total[0]]=item_total[1]

        self.dbProxy.execute(selectSql)
        results = self.dbProxy.fetchall()
        self.dbProxy.commit()

        resultlist = {}
        resultlist_for_db = list()
        insertValueTemplate = '(%d,%d,%d,%d)'

        for item in results:

            resultlist2 = {}
            resultlist2['event_id'] = item[0]
            resultlist2['channel_id'] = item[3]

            if item[0] not in resultlist:   # 事件id一开始不存在resultlist则初始化四大数据
                resultlist2['unset'] = 0
                resultlist2['negative'] = 0
                resultlist2['neutral'] = 0
                resultlist2['positive'] = 0

                resultlist[resultlist2['event_id']] = {
                    resultlist2['channel_id']: [resultlist2['unset'], resultlist2['negative'],
                                                resultlist2['neutral'], resultlist2['positive']]}
                # 将记录筛选写入resultlist
                if item[1] == -2:
                    resultlist[resultlist2['event_id']][item[3]][0] = item[2]
                elif item[1] == -1:
                    resultlist[resultlist2['event_id']][item[3]][1] = item[2]
                elif item[1] == 0:
                    resultlist[resultlist2['event_id']][item[3]][2] = item[2]
                elif item[1] == 1:
                    resultlist[resultlist2['event_id']][item[3]][3] = item[2]

            else:  # 事件id存在resultlist中
                if item[3] not in resultlist[item[0]]:  # 媒体id不存在当前事件id中则初始化该媒体Id的四大数据
                    resultlist2['unset'] = 0
                    resultlist2['negative'] = 0
                    resultlist2['neutral'] = 0
                    resultlist2['positive'] = 0

                    resultlist[resultlist2['event_id']][item[3]] = [resultlist2['unset'], resultlist2['negative'],
                                                                    resultlist2['neutral'], resultlist2['positive']]
                    # 筛选属于四大数据的哪一个
                    if item[1] == -2:
                        resultlist[resultlist2['event_id']][item[3]][0] = item[2]
                    elif item[1] == -1:
                        resultlist[resultlist2['event_id']][item[3]][1] = item[2]
                    elif item[1] == 0:
                        resultlist[resultlist2['event_id']][item[3]][2] = item[2]
                    elif item[1] == 1:
                        resultlist[resultlist2['event_id']][item[3]][3] = item[2]

                else:  # 媒体id存在当前事件id中则直接筛选属于四大数据哪一个
                    if item[1] == -2:
                        resultlist[resultlist2['event_id']][item[3]][0] = item[2]
                    elif item[1] == -1:
                        resultlist[resultlist2['event_id']][item[3]][1] = item[2]
                    elif item[1] == 0:
                        resultlist[resultlist2['event_id']][item[3]][2] = item[2]
                    elif item[1] == 1:
                        resultlist[resultlist2['event_id']][item[3]][3] = item[2]

        # print resultlist => {1: {1: [0, 0, 5, 10], 4: [55, 0, 0, 0]},
        # 2:{11: [0, 0, 0, 1], 5: [0, 0, 44, 0]}, 5: {5: [111, 0, 0, 0]}}

        for eventid in resultlist:  # 计算friend 和 influence 并保存在resultlist_for_db数组
            for channelid in resultlist[eventid]:
                friend=round(((resultlist[eventid][channelid][3] - resultlist[eventid][channelid][2] - resultlist[eventid][channelid][1]) / float(resultlist[eventid][channelid][0]+
                            resultlist[eventid][channelid][1]+resultlist[eventid][channelid][2]+resultlist[eventid][channelid][3]) * 100), 2)

                influence=round(((resultlist[eventid][channelid][0]+ resultlist[eventid][channelid][1]+resultlist[eventid][channelid][2]+
                                  resultlist[eventid][channelid][3]) / float(resultlist_total[eventid])  * 100),2)
                resultlist_for_db.append(insertValueTemplate % (eventid,channelid,friend,influence))
        # friend=（一个事件在一个媒体里的positive的文章数量 - 在这个媒体里的negative的文章数量 - 在这个媒体里的中性文章数量）/ 在这个媒体下的所有文章（positive+negative+中性）
        # influence = (一个事件在一个媒体里的positive的文章数量 - 在这个媒体里的negative的文章数量 - 在这个媒体里的中性文章数量）/ 在所有的媒体下关于这个事件的所有文章（positive+negative+中性）

        # 插入的sql语句
        insertSql = '''INSERT INTO %s (event_id,channel_id,friend,influence) VALUES''' % self.media_trend_tableName

        if len(resultlist_for_db) > 0:
            # 先删除sa_media_trend表原有数据
            deleteSql = '''delete from %s where event_id = %d''' % (self.media_trend_tableName, self.event_id)
            self.dbProxy.execute(deleteSql)

            # 插入sa_media_trend表
            sql = insertSql + ','.join(resultlist_for_db)
            self.dbProxy.execute(sql)
            self.dbProxy.commit()


class HeatWordsAnalytics(AnalyticsBase):
    '''
    热词算法
    '''

    def __init__(self, dbProxy, entity_id, event_id):
        '''
        Constructor
        '''
        super(HeatWordsAnalytics, self).__init__(dbProxy, entity_id)
        self.event_id = event_id

    def __analysisEventArticle(self):
        heatWordsNum = int(self.saConf.getConf(self.saConf.CONF_HEAT_WORDS_NUM))
        # 获取尚未过期的事件
        eventDict = GlobalVariable.getEventMgmt().entityEventDict[self.entity_id]
        if len(eventDict.values()) == 0:
            self.logger.warn('[HeatWords]No valid event list for %s', self.entity_id)
            return
        # 获取文章标题
        tableName = Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id

        selectSql = '''
        SELECT title, tid, channel_id from %s where event_id=%d and MANUAL_REMOVE = 'N'
        ''' % (tableName, self.event_id)
        self.dbProxy.execute(selectSql)
        results = self.dbProxy.fetchall()
        self.dbProxy.commit()
        articleList = map(lambda x: Article(x[1], x[2], title=x[0]), results)
        # self.logger.debug("[Heatwords] Sql:%s", selectSql)
        self.logger.debug('[Heatwords] There are %d articles for %d for %s', len(list(articleList)), self.event_id,
                          self.entity_id)
        # 热词分析
        allContents = '\n'.join(map(lambda x: x[0], results))
        # heatWordList = self.__heatWordsAnalysis(allContents, stopWords, heatWordsNum)
        heatWordList = self.__heatWordsAnalysisV2(allContents, heatWordsNum)
        self.logger.debug('[Heatwords] There are %d words for %d for %s', len(heatWordList), self.event_id,
                          self.entity_id)
        self.__updateToDb(heatWordList, articleList, self.event_id)

    def __analysisEntityArticle(self):
        heatWordsDays = int(self.saConf.getConf(self.saConf.CONF_HEAT_WORDS_DAYS))
        heatWordsNum = int(self.saConf.getConf(self.saConf.CONF_HEAT_WORDS_NUM))
        # 获取文章标题
        tableName = Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        currentDatetime = datetime.now() - timedelta(days=heatWordsDays)
        targetDatetime = currentDatetime.strftime('%Y-%m-%d') + ' 00:00:00'
        selectSql = '''
        SELECT title, tid, channel_id from %s where publish_datetime>='%s'
        ''' % (tableName, targetDatetime)
        self.dbProxy.execute(selectSql)
        results = self.dbProxy.fetchall()
        articleList = map(lambda x: Article(x[1], x[2], title=x[0]), results)

        # 热词分析
        allContents = '\n'.join(map(lambda x: x[0], results))
        heatWordList = self.__heatWordsAnalysisV2(allContents, heatWordsNum)
        # 更新数据库
        self.__updateToDb(heatWordList, articleList, 0)

    def analysis(self, item=None):
        '''
        @param item: 此参数不起作用
        '''
        self.logger.info('[HeatWords]Begin to analyse for %s', self.entity_id)
        stopWordsFile = self.saConf.getConf(self.saConf.CONF_STOPS_WORDS_FILE)
        userDictFile = self.saConf.getConf(self.saConf.CONF_USER_DICT_FILE)

        if 'window' in platform.system().lower():
            # original_path = os_file.current_dir()
            words = os_file.current_dir
            stopWordsFile = os.path.join(words, stopWordsFile)
            userDictFile = os.path.join(words, userDictFile)
            # stopWordsFile = original_path + stopWordsFile.replace('/', '\\')
            # userDictFile = original_path + userDictFile.replace('/', '\\')
        # 获取stopwords
        # stopWords = Tools.fetchStopWords(stopWordsFile)
        # 加载用户词典
        jieba.load_userdict(userDictFile)
        # 加载停用词
        jieba.analyse.set_stop_words(stopWordsFile)
        # 加载收集词典
        stopWordSql = '''
        SELECT DISTINCT(WORD) FROM %s
        ''' % (Constants.TABLE_SA_HEAT_WORDS_REMOVE + Constants.TABLE_NAME_DELIMITER + self.entity_id)
        self.dbProxy.execute(stopWordSql)
        results = self.dbProxy.fetchall()
        for item in results:
            jieba.analyse.default_textrank.stop_words.add(item[0])
            jieba.analyse.default_tfidf.stop_words.add(item[0])
        # stopWordTmpFile = os.path.join('conf','stopwordTmpFile_'+self.entity_id+'.txt')
        self.__analysisEntityArticle()
        self.__analysisEventArticle()

    def __heatWordsAnalysisV2(self, content, heatWordsNum):
        allContents = re.sub(r'([\d]+)', '', content)
        allContents = re.sub(r'([\w]+)', '', allContents)
        allContents = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+", "", allContents) #.decode("utf8")
        return list(jieba.analyse.extract_tags(allContents, topK=heatWordsNum, withWeight=True))

    def __heatWordsAnalysis(self, allContents, stopWords, heatWordsNum):

        for w in stopWords:
            allContents = allContents.replace(w, '')
        allContents = re.sub(r'([\d]+)', '', allContents)
        allContents = re.sub(r'([\w]+)', '', allContents)
        return list(jieba.analyse.extract_tags(allContents, topK=heatWordsNum, withWeight=True))

    def __updateToDb(self, heatWordList, articleList, event_id=0):
        if len(heatWordList) > 0:
            # 1. 删除旧记录
            # 1.1 删除主表
            tableName = Constants.TABLE_SA_HEAT_WORDS + Constants.TABLE_NAME_DELIMITER + self.entity_id
            historyTableName = Constants.TABLE_SA_HEAT_WORDS_HISTORY + Constants.TABLE_NAME_DELIMITER + self.entity_id
            deleteSql = 'DELETE FROM %s where event_id=%d' % (tableName, self.event_id)
            self.dbProxy.execute(deleteSql)
            # 1.2 删除文章对应表
            tableName = Constants.TABLE_SA_HEAT_WORDS_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
            deleteSql = 'DELETE FROM %s where event_id="%d"' % (tableName, self.event_id)
            self.dbProxy.execute(deleteSql)
            self.dbProxy.commit()

            # 2. 插入记录
            n = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tableName = Constants.TABLE_SA_HEAT_WORDS + Constants.TABLE_NAME_DELIMITER + self.entity_id
            insertSql = 'INSERT INTO %s (event_id, add_datetime, word, weight) VALUES'
            insertValues = list()
            wordValues = list()
            for heatWord in heatWordList:
                insertValues.append('(%d, "%s", "%s", %f)' % (
                    self.event_id, n, heatWord[0], heatWord[1]))
                for article in articleList:
                    if article.title.find(heatWord[0]) > 0:
                        wordValues.append(
                            '(%d, "%s", "%s", %d)' % (self.event_id, heatWord[0], article.tid, article.channel_id))
            # 2.1 插入当前表
            self.dbProxy.execute(insertSql % tableName + ','.join(insertValues))
            # 2.2 插入历史表
            # self.dbProxy.execute(insertSql%historyTableName + ','.join(insertValues))

            # 3 遍历查找文章
            # tableName = Constants.TABLE_SA_HEAT_WORDS_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
            # insertWordArticleSql = 'INSERT INTO %s event_id, word, tid, channel_id) values ' % tableName
            # self.dbProxy.execute(insertWordArticleSql + ','.join(wordValues))

            # COMMIT
            self.dbProxy.commit()


class IndicatorWarningAnalytics(AnalyticsBase):
    '''
    指标预警
    '''

    def __init__(self, dbProxy, entity_id, event_id, logger=None):
        '''
        Constructor
        '''
        super(IndicatorWarningAnalytics, self).__init__(dbProxy, entity_id, logger)
        self.event_id = event_id

    def analysis(self, **kwargs):
        self._eventAnalysis()

    # 事件指标预警
    def _eventAnalysis(self):
        # 获取阀值，操作的表是sa_threshold_config, 条件为system_type=1
        thresholdList, sys_threshold, ene_threshold = self.__fetchThreshold(1)

        # 根据媒体时间表获取信息,
        eventIndDict = self.__fetchFromEventMediaStatistics()

        # check event media statistics
        eventWarningDict = dict()
        warningDatetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for eventInd in eventIndDict.values():
            warningTypeSet = set()
            is_set = {
                'sta_reply': False,
                'sta_read': False,
                'sta_forward': False,
                'heat': False,
                'media_range': False
            }

            for thresholdConf in thresholdList:
                # 评论数预警
                if str(eventInd.event_id) == thresholdConf.biz_id:  # 加集团的判断
                    threshold = thresholdConf.threshold
                    is_set[thresholdConf.type] = True
                    if thresholdConf.type == 'sta_reply' and Constants.WARNNING_TYPE_REPLY not in warningTypeSet:  #  预警值，避免重复
                        if eval(threshold.replace('v', str(eventInd.reply_count))):
                            obj = EventIndicatorWarningSignal(thresholdConf.id, eventInd.event_id,
                                                              None, None, thresholdConf.level,
                                                              Constants.WARNNING_TYPE_REPLY, warningDatetime,
                                                              threshold, eventInd.reply_count)
                            eventWarningDict[obj.idKey] = obj
                            warningTypeSet.add(Constants.WARNNING_TYPE_REPLY)

                    # 阅读数预警
                    if thresholdConf.type == 'sta_read' and Constants.WARNNING_TYPE_READ not in warningTypeSet:
                        if eval(threshold.replace('v', str(eventInd.read_count))):
                            obj = EventIndicatorWarningSignal(thresholdConf.id, eventInd.event_id,
                                                              None, None, thresholdConf.level,
                                                              Constants.WARNNING_TYPE_READ, warningDatetime,threshold,
                                                              eventInd.read_count)
                            eventWarningDict[obj.idKey] = obj
                            warningTypeSet.add(Constants.WARNNING_TYPE_READ)

                    # 转发数预警
                    if thresholdConf.type == 'sta_forward' and Constants.WARNNING_TYPE_FORWARD not in warningTypeSet:
                        if eval(threshold.replace('v', str(eventInd.forward_count))):
                            obj = EventIndicatorWarningSignal(thresholdConf.id, eventInd.event_id,
                                                              None, None, thresholdConf.level,
                                                              Constants.WARNNING_TYPE_FORWARD, warningDatetime,
                                                              threshold, eventInd.forward_count)
                            eventWarningDict[obj.idKey] = obj
                            warningTypeSet.add(Constants.WARNNING_TYPE_FORWARD)

                    # 热度预警
                    if thresholdConf.package_type == 'heat' and Constants.WARNNING_TYPE_HEAT not in warningTypeSet:
                        if eval(threshold.replace('v', str(eventInd.heat)).replace('i', '0')):
                            obj = EventIndicatorWarningSignal(thresholdConf.id, eventInd.event_id,
                                                              None, None, thresholdConf.level,
                                                              Constants.WARNNING_TYPE_HEAT, warningDatetime,
                                                              threshold, eventInd.heat)
                            eventWarningDict[obj.idKey] = obj
                            warningTypeSet.add(Constants.WARNNING_TYPE_HEAT)

                    # 原创数预警 2018.5.25预警配置表目前没有
                    if thresholdConf.type == 'sta_original' and Constants.WARNNING_TYPE_ORIGINAL not in warningTypeSet:
                        if eval(threshold.replace('v', str(eventInd.original_count))):
                            obj = EventIndicatorWarningSignal(thresholdConf.id, eventInd.event_id,
                                                              None, None, thresholdConf.level,
                                                              Constants.WARNNING_TYPE_ORIGINAL, warningDatetime,
                                                              threshold, eventInd.original_count)
                            eventWarningDict[obj.idKey] = obj
                            warningTypeSet.add(Constants.WARNNING_TYPE_ORIGINAL)

                    # 媒体列表预警
                    if thresholdConf.type == 'media_range' and Constants.WARNNING_TYPE_MEDIA not in warningTypeSet:
                        dataValue = ''
                        con = False
                        is_right = False
                        # 媒体数量跟设置的阈值相比
                        temp_threshold = re.search(r'(.*) or (.*)', threshold)
                        channel_count_threshold = temp_threshold.group(1)
                        channel_publish_threshold = temp_threshold.group(2)

                        try:
                            con = eval(channel_count_threshold.replace('v', str(eventInd.channel_count)))
                            # 如果第一个条件符合，则直接通过
                            if con:
                                dataValue = '媒体数：%s' % str(eventInd.channel_count)
                                is_right = True
                            else:
                                # 不通过，则判断第二个条件
                                # level=1时
                                if thresholdConf.level == '1':
                                    threshold_channels = re.search(r's in(.*)', channel_publish_threshold).group(1)
                                    threshold_channels_list = list(eval(threshold_channels))

                                    # 如果传播该事件的全部媒体包含条件中媒体
                                    is_f = [False for item in threshold_channels_list if
                                            item not in eventInd.channelIdList]

                                    if is_f:
                                        is_right = False
                                    else:
                                        dataValue = '传播媒体数包含%s' % threshold_channels
                                        is_right = True
                                else:
                                    # 开始传播媒体值
                                    if eventInd.first_media_type_id == Constants.CHANNEL_TYPE_NEWS:
                                        channel_value = 3
                                        dataValue = '%s开始传播' % eventInd.first_media_name
                                    elif eventInd.first_media_type_id == Constants.CHANNEL_TYPE_WEIBO:
                                        channel_value = 2
                                        dataValue = '%s开始传播' % eventInd.first_media_name
                                    elif eventInd.first_media_type_id == Constants.CHANNEL_TYPE_WECHAT:
                                        channel_value = 1
                                        dataValue = '%s开始传播' % eventInd.first_media_name
                                    else:
                                        channel_value = ''

                                    con = eval(channel_publish_threshold.replace('i', str(channel_value)))
                                    if con:
                                        is_right = True
                                    else:
                                        is_right = False
                        except Exception as e:
                            is_right = False

                        if is_right:
                            obj = EventIndicatorWarningSignal(thresholdConf.id, eventInd.event_id,
                                                              None, None, thresholdConf.level,
                                                              Constants.WARNNING_TYPE_MEDIA, warningDatetime,
                                                              threshold, dataValue)
                            eventWarningDict[obj.idKey] = obj
                            warningTypeSet.add(Constants.WARNNING_TYPE_MEDIA)

            # 未设置阀值的事件自动预警，集团或者系统阀值
            for i_item in is_set:
                # 判断哪种类型未预警
                if not is_set[i_item]:
                    threshold_type = i_item
                    # 获得集团或者系统的阀值
                    th_item = None
                    if threshold_type in ene_threshold:
                        th_item = ene_threshold[threshold_type]
                    else:
                        th_item = sys_threshold[threshold_type]
                    sort_key = sorted(th_item.keys())
                    for i in sort_key:
                        threshold = th_item[i].split('_')[0]
                        threshold_id = th_item[i].split('_')[1]
                        # 评论数预警
                        if threshold_type == 'sta_reply' and Constants.WARNNING_TYPE_REPLY not in warningTypeSet:
                            if eval(threshold.replace('v', str(eventInd.reply_count))):
                                obj = EventIndicatorWarningSignal(threshold_id, eventInd.event_id, None, None, str(i),
                                                         Constants.WARNNING_TYPE_REPLY, warningDatetime,
                                                         threshold, eventInd.reply_count
                                                         )
                                eventWarningDict[obj.idKey] = obj
                                warningTypeSet.add(Constants.WARNNING_TYPE_REPLY)

                        # 阅读数预警
                        if threshold_type == 'sta_read' and Constants.WARNNING_TYPE_READ not in warningTypeSet:
                            if eval(threshold.replace('v', str(eventInd.read_count))):
                                obj = EventIndicatorWarningSignal(threshold_id, eventInd.event_id, None, None, str(i),
                                                         Constants.WARNNING_TYPE_READ, warningDatetime,
                                                         threshold, eventInd.read_count
                                                         )
                                eventWarningDict[obj.idKey] = obj
                                warningTypeSet.add(Constants.WARNNING_TYPE_READ)

                        # 转发数预警
                        if threshold_type == 'sta_forward' and Constants.WARNNING_TYPE_FORWARD not in warningTypeSet:
                            if eval(threshold.replace('v', str(eventInd.forward_count))):
                                obj = EventIndicatorWarningSignal(threshold_id, eventInd.event_id, None, None, str(i),
                                                         Constants.WARNNING_TYPE_FORWARD, warningDatetime,
                                                         threshold, eventInd.forward_count
                                                         )
                                eventWarningDict[obj.idKey] = obj
                                warningTypeSet.add(Constants.WARNNING_TYPE_FORWARD)

                        # 热度预警
                        if threshold_type == 'heat' and Constants.WARNNING_TYPE_HEAT not in warningTypeSet:
                            if eval(threshold.replace('v', str(eventInd.heat)).replace('i', '0')):
                                obj = EventIndicatorWarningSignal(threshold_id, eventInd.event_id, None, None, str(i),
                                                         Constants.WARNNING_TYPE_HEAT, warningDatetime,
                                                         threshold, eventInd.heat
                                                         )
                                eventWarningDict[obj.idKey] = obj
                                warningTypeSet.add(Constants.WARNNING_TYPE_HEAT)

                        # 原创数预警
                        if threshold_type == 'sta_original' and Constants.WARNNING_TYPE_ORIGINAL not in warningTypeSet:
                            if eval(threshold.replace('v', str(eventInd.original_count))):
                                obj = EventIndicatorWarningSignal(threshold_id, eventInd.event_id, None, None, str(i),
                                                         Constants.WARNNING_TYPE_ORIGINAL, warningDatetime,
                                                         threshold, eventInd.original_count
                                                         )
                                eventWarningDict[obj.idKey] = obj
                                warningTypeSet.add(Constants.WARNNING_TYPE_ORIGINAL)

                        # 媒体列表预警
                        if threshold_type == 'media_range' and Constants.WARNNING_TYPE_MEDIA not in warningTypeSet:
                            dataValue = ''
                            con = False
                            is_right = False
                            # 媒体数量跟设置的阈值相比
                            temp_threshold = re.search(r'(.*) or (.*)', threshold)
                            channel_count_threshold = temp_threshold.group(1)
                            channel_publish_threshold = temp_threshold.group(2)

                            try:
                                con = eval(channel_count_threshold.replace('v', str(eventInd.channel_count)))
                                # 如果第一个条件符合，则直接通过
                                if con:
                                    dataValue = '媒体数：%s' % str(eventInd.channel_count)
                                    is_right = True
                                else:
                                    # 不通过，则判断第二个条件
                                    try:
                                        threshold_channels = re.search(r's in(.*)', channel_publish_threshold).group(1)
                                        threshold_channels_list = list(eval(threshold_channels))

                                        # 如果传播该事件的全部媒体包含条件中媒体
                                        is_f = [False for item in threshold_channels_list if
                                                item not in eventInd.channelIdList]

                                        if is_f:
                                            is_right = False
                                        else:
                                            dataValue = '传播媒体数包含%s' % threshold_channels
                                            is_right = True
                                    except Exception as e:
                                        # 开始传播媒体值
                                        if eventInd.first_media_type_id == Constants.CHANNEL_TYPE_NEWS:
                                            channel_value = 3
                                            dataValue = '%s开始传播' % eventInd.first_media_name
                                        elif eventInd.first_media_type_id == Constants.CHANNEL_TYPE_WEIBO:
                                            channel_value = 2
                                            dataValue = '%s开始传播' % eventInd.first_media_name
                                        elif eventInd.first_media_type_id == Constants.CHANNEL_TYPE_WECHAT:
                                            channel_value = 1
                                            dataValue = '%s开始传播' % eventInd.first_media_name
                                        else:
                                            channel_value = ''
                                        try:
                                            con = eval(channel_publish_threshold.replace('i', str(channel_value)))
                                        except Exception as e:
                                            con = False
                                        if con:
                                            is_right = True
                                        else:
                                            is_right = False
                            except KeyError as e:
                                is_right = False

                            if is_right:
                                obj = EventIndicatorWarningSignal(threshold_id, eventInd.event_id, None, None, str(i),
                                                                  Constants.WARNNING_TYPE_MEDIA, warningDatetime,
                                                                  threshold, dataValue)
                                eventWarningDict[obj.idKey] = obj
                                warningTypeSet.add(Constants.WARNNING_TYPE_MEDIA)

        self.logger.debug(len(eventWarningDict))
        # 老版本，新版本没用到该方法
        # self.__deleteDifferent(eventWarningDict)
        self.logger.debug(len(eventWarningDict))

        self.__checkExists(eventWarningDict)

        self.logger.debug(len(eventWarningDict))

        self.__updateToDb(eventWarningDict)

    def __updateToDb(self, eventWarningDict):

        historyWarningDict = self.__checkExists(eventWarningDict)

        insertSql = '''
                    INSERT INTO %s (config_id, event_id, level, warning_type, warning_datetime, threshold_value, data_value)
                    VALUES %s
                    ON DUPLICATE KEY UPDATE config_id=VALUES(config_id), level=VALUES(level), 
                    warning_type=VALUES(warning_type), warning_datetime=VALUES(warning_datetime), 
                    threshold_value=VALUES(threshold_value), data_value=VALUES(data_value)
                    '''

        insertHisSql = '''
                    INSERT INTO %s (config_id, event_id, level, warning_type, warning_datetime, threshold_value, data_value)
                    VALUES %s
        '''

        if len(eventWarningDict) > 0:
            # 事件预警
            valueList = list()
            for eventWarningObj in eventWarningDict.values():
                valueList.append('("%s", %s, %s, %s, "%s", "%s", "%s")' % (
                    eventWarningObj.config_id, eventWarningObj.event_id, eventWarningObj.level,
                    eventWarningObj.warning_type, eventWarningObj.warning_datetime,
                    eventWarningObj.threshold_value, eventWarningObj.data_value
                ))
            tableName = Constants.TABLE_SA_EVENT_INDICATOR_WARNING_SIGNAL + Constants.TABLE_NAME_DELIMITER + self.entity_id
            self.dbProxy.execute(insertSql % (tableName, ','.join(valueList)))

            tableName = Constants.TABLE_SA_EVENT_INDICATOR_WARNING_SIGNAL_HISTORY + Constants.TABLE_NAME_DELIMITER + self.entity_id
            self.dbProxy.execute(insertHisSql % (tableName, ','.join(valueList)))

            self.dbProxy.commit()

        if historyWarningDict:
            valueList = list()
            for historyWarningObj in historyWarningDict.values():
                valueList.append('("%s", %s, %s, %s, "%s", "%s", "%s")' % (
                    historyWarningObj.config_id,
                    historyWarningObj.event_id, historyWarningObj.level, historyWarningObj.warning_type,
                    historyWarningObj.warning_datetime, historyWarningObj.threshold_value, historyWarningObj.data_value
                ))
            # 如果数据有改动，则更新，完全不同的数据则插入
            tableName = Constants.TABLE_SA_EVENT_INDICATOR_WARNING_SIGNAL_HISTORY + Constants.TABLE_NAME_DELIMITER + self.entity_id
            self.dbProxy.execute(insertHisSql % (tableName, ','.join(valueList)))

            self.dbProxy.commit()

    def __checkExists(self, eventWarningDict):
        if len(eventWarningDict.values()) != 0:
            tableName = Constants.TABLE_SA_EVENT_INDICATOR_WARNING_SIGNAL + Constants.TABLE_NAME_DELIMITER + self.entity_id
            sql = '''select event_id, level, warning_type, data_value from %s where %s'''
            whereClauseList = list()
            historyListDict = dict()
            for item in eventWarningDict.values():
                self.logger.debug("id:" + str(item.event_id) + '_' + str(item.level) + '_' + str(item.warning_type))
                whereClauseList.append('(event_id=%s and level=%s and warning_type=%s)' % (
                    item.event_id,
                    item.level,
                    item.warning_type))

            self.dbProxy.execute(sql % (tableName, ' or '.join(whereClauseList)))
            # 获取到已经预警的全部事件
            results = self.dbProxy.fetchall()
            self.dbProxy.commit()
            self.logger.debug(len(results))

            for item in results:
                # 构造主键
                index = '_'.join([str(item[1]), str(item[2]), str(item[0])])

                # 非媒体预警
                # 如果预警data_value高的话就进行数据更新。
                if eventWarningDict[index].warning_type != Constants.WARNNING_TYPE_MEDIA:
                    if float(eventWarningDict[index].data_value) > float(item[3]):
                        historyListDict[index] = eventWarningDict[index]
                    else:
                        del eventWarningDict[index]
                else:
                    # 预警level越高数值越低，level升高就预警
                    if int(eventWarningDict[index].level) < int(item[1]):
                        historyListDict[index] = eventWarningDict[index]
                    else:
                        del eventWarningDict[index]

            self.logger.debug(len(historyListDict))
            return historyListDict

    def __fetchThreshold(self, system_type):
        tableName = Constants.TABLE_SA_THRESHOLD_CONFIG
        selectSql = '''select id, biz_id, level, type, threshold, data_scope, package_type
                        from %s
                        where system_type = %d and corp_code = "%s"
                        order by data_scope desc,  level asc
        ''' % (tableName, system_type, self.entity_id.lower())

        self.dbProxy.execute(selectSql)
        results = self.dbProxy.fetchall()

        # 先获取实体的阈值，若没有则获取平台的阈值
        if len(results) == 0:
            selectSql = '''select id, biz_id, level, type, threshold, data_scope, package_type
                                    from %s
                                    where system_type = %d
                                    order by data_scope desc,  level asc
            '''% (tableName, system_type)
            self.dbProxy.execute(selectSql)
            results = self.dbProxy.fetchall()
        self.dbProxy.commit()

        thresholdList = list()
        sys_threshold = dict() #2018.5.30 系统预警 姚
        ene_threshold = dict() #2018.5.30 集团预警 姚
        for item in results:
            thresholdConf = ThresholdConf()
            thresholdConf.id = item[0]
            thresholdConf.biz_id = item[1]
            thresholdConf.level = item[2]
            thresholdConf.type = item[3]
            thresholdConf.threshold = item[4].replace('||', ' or ')
            thresholdConf.data_scope = item[5]
            thresholdConf.package_type = item[6]
            thresholdList.append(thresholdConf)

            # 构造系统阀值字典，阀值类型类和阀值的等级为键，阀值的值为值　
            if thresholdConf.type is not '':  # 判断type类型是否为空
                if thresholdConf.type not in sys_threshold and thresholdConf.data_scope == '1':
                    sys_threshold[thresholdConf.type] = dict()
                    sys_threshold[thresholdConf.type][thresholdConf.level] = '_'.join([thresholdConf.threshold, thresholdConf.id])
                elif thresholdConf.data_scope == '1':
                    sys_threshold[thresholdConf.type][thresholdConf.level] = '_'.join([thresholdConf.threshold, thresholdConf.id])
                if thresholdConf.type not in ene_threshold and thresholdConf.data_scope == '2':
                    ene_threshold[thresholdConf.type] = dict()
                    ene_threshold[thresholdConf.type][thresholdConf.level] = '_'.join([thresholdConf.threshold, thresholdConf.id])
                elif thresholdConf.data_scope == '2':
                    ene_threshold[thresholdConf.type][thresholdConf.level] = '_'.join([thresholdConf.threshold, thresholdConf.id])
            elif thresholdConf.package_type != '':
                if thresholdConf.package_type not in sys_threshold and thresholdConf.data_scope == '1':
                    sys_threshold[thresholdConf.package_type] = dict()
                    sys_threshold[thresholdConf.package_type][thresholdConf.level] = '_'.join([thresholdConf.threshold, thresholdConf.id])
                elif thresholdConf.data_scope == '1':
                    sys_threshold[thresholdConf.package_type][thresholdConf.level] = '_'.join([thresholdConf.threshold, thresholdConf.id])
                if thresholdConf.package_type not in ene_threshold and thresholdConf.data_scope == '2':
                    ene_threshold[thresholdConf.package_type] = dict()
                    ene_threshold[thresholdConf.package_type][thresholdConf.level] = '_'.join([thresholdConf.threshold, thresholdConf.id])
                elif thresholdConf.data_scope == '2':
                    ene_threshold[thresholdConf.package_type][thresholdConf.level] = '_'.join([thresholdConf.threshold, thresholdConf.id])
        return thresholdList, sys_threshold, ene_threshold

    def __fetchFromEventMediaStatistics(self):
        '''
        根据事件媒体表的数据获取信息
        '''
        tableName = Constants.TABLE_SA_EVENT_MEDIA_STATISTICS + Constants.TABLE_NAME_DELIMITER + self.entity_id

        # 第一个开始传播媒体
        firstTableName = Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        channelTableName = Constants.TABLE_SA_CHANNEL
        channelTypeTableName = Constants.TABLE_SA_CHANNEL_TYPE

        first_media_sql = '''
              select sa_ea.channel_id, sc_t.CHANNEL_TYPE_ID, sc_t.CHANNEL_TYPE_NAME from %s as sa_ea 
              RIGHT JOIN %s as sc on sa_ea.channel_id = sc.channel_id
              RIGHT JOIN %s as sc_t on sc.CHANNEL_TYPE_ID = sc_t.CHANNEL_TYPE_ID
              where sa_ea.event_id = %d
              order by sa_ea.publish_datetime limit 1
        '''

        # 获取已删除的event_id
        self.dbProxy.execute('select event_id from %s where del_flag=1' % (Constants.TABLE_SA_EVENT +
                             Constants.TABLE_NAME_DELIMITER + self.entity_id))

        del_ids = self.dbProxy.fetchall()
        self.dbProxy.commit()

        ids_str = '()'
        if len(del_ids) > 0:
            ids_list = list()
            for item in del_ids:
                ids_list.append(str(item[0]))

            ids_str = '(' + ','.join(ids_list) + ')'

        # 主要指标->根据事件id
        selectSql = '''select event_id, read_count, like_count, reply_count, forward_count,
                            collect_count, heat, original_count
                            from %s where event_id = %d and channel_id=0
            ''' % (tableName, self.event_id)

        selectMediaSql = '''
                           select event_id, channel_id from %s 
                           where event_id = %d group by CHANNEL_ID order by CHANNEL_ID
                           ''' % (tableName, self.event_id)

        self.dbProxy.execute(selectSql)
        results = self.dbProxy.fetchall()
        self.dbProxy.commit()

        eventIndDict = dict()
        for item in results:
            eventInd = EventIndicator()
            eventInd.event_id = item[0]
            eventInd.read_count = item[1]
            eventInd.like_count = item[2]
            eventInd.reply_count = item[3]
            eventInd.forward_count = item[4]
            eventInd.collect_count = item[5]
            eventInd.heat = item[6]
            eventInd.original_count = item[7]
            eventInd.channelIdList = []

            # 第一个传播媒体
            sql = first_media_sql % (firstTableName, channelTableName, channelTypeTableName, item[0])
            self.dbProxy.execute(sql)
            channels = self.dbProxy.fetchall()
            eventInd.first_media = channels[0][0]
            eventInd.first_media_type_id = channels[0][1]
            eventInd.first_media_name = channels[0][2]

            # 媒体数量
            mediaCountSql = '''select count(distinct channel_id)
                                            from %s
                                            where event_id = %s and channel_id <> 0
                        ''' % (tableName, item[0])
            self.dbProxy.execute(mediaCountSql)
            count = self.dbProxy.fetchall()
            eventInd.channel_count = count[0][0]
            eventIndDict[item[0]] = eventInd

        self.dbProxy.execute(selectMediaSql)
        results = self.dbProxy.fetchall()
        self.dbProxy.commit()
        for item in results:
            if item[1] not in eventIndDict[item[0]].channelIdList:
                eventIndDict[item[0]].channelIdList.append(item[1])
        return eventIndDict

    def __isDefault(self, objectId, thresholdList):
        for thresholdConf in thresholdList:
            if (objectId == thresholdConf.event_id):
                return False
        return True

    def __deleteDifferent(self, eventWarningDict):
        '''
        删除数据库中不一致的数据
        '''
        if len(eventWarningDict.values()) != 0:
            # 删除非一致数据（即对象id、level和预警类型一致的数据进行保留，其他数据删除）
            tableName = Constants.TABLE_SA_WARNING_SIGNAL + Constants.TABLE_NAME_DELIMITER + self.entity_id
            deleteSql = '''delete from %s where object_type = %d and warning_type<>%d and''' % (
            tableName, Constants.OBJECT_TYPE_EVENT, Constants.WARNNING_TYPE_SENSITIVE)
            whereClauseList = list()
            for item in eventWarningDict.values():
                whereClauseList.append(' (object_id<>"%s" or level<>%s or warning_type<>%s or data_value<>"%s")' % (
                    item.event_id,
                    item.level,
                    item.warning_type,
                    str(item.data_value)))
            self.dbProxy.execute(deleteSql + ' and '.join(whereClauseList))
            self.dbProxy.commit()


class TableRecord(object):
    def __init__(self, read_count, like_count, reply_count, forward_count, collect_count, article_count, heat):
        self.read_count = read_count
        self.reply_count = reply_count
        self.forward_count = forward_count
        self.like_count = like_count
        self.collect_count = collect_count
        self.article_count = article_count
        self.heat = heat


class ThresholdConf(object):
    def __init__(self):
        self.id = 0  # 加了这个ID 20180624 Jondar
        self.biz_id = 0 #2018.5.25加 用来判断 姚
        self.level = 0
        self.type = 0
        self.threshold = ''
        self.data_scope = 1
        self.package_type = ''
        self.corp_code = ''  #2018.5.25加 用来判断 姚


class EventIndicator(object):
    def __init__(self):
        self.event_id = 0
        self.read_count = 0
        self.like_count = 0
        self.reply_count = 0
        self.forward_count = 0
        self.collect_count = 0
        self.heat = 0
        self.channelIdList = []
        self.foreign_count = 0
        self.original_count = 0
        self.province_count = 0
        self.netizen_count = 0
        self.channel_count = 0
        self.first_media = ''
        self.first_media_name = ''
        self.first_media_type_id = ''