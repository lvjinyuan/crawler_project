# -*- coding:utf-8 -*-
'''
Created on 1 Oct 2017

@author: eyaomai
'''
import sys, os
import jieba

from conf import os_file

sys.path.append(sys.argv[0][:sys.argv[0].rfind(os.path.join('com','naswork'))])

from crawler.lib.analysis.analysisbase import AnalyticsBase, Tools
from crawler.lib.common.constant import Constants
from crawler.lib.common.globalvar import GlobalVariable
from crawler.lib.objectmodel.article import Article
import json,platform

class EntityIdentificationAnalytics(AnalyticsBase):
    '''
    实体识别分析
    '''


    def __init__(self, dbProxy, entity_id):
        '''
        针对实体分析，entity不起作用
        '''
        super(EntityIdentificationAnalytics, self).__init__(dbProxy, '')
    
    def analysis(self, article):
        '''
        分析该文章属于哪个实体
        '''
        entityDict = GlobalVariable.getEntityMgmt().entityDict
        
        return self.__analysisEntity(article, entityDict)

    
    def __analysisEntity(self, article, entityDict):
        hitEntityList = list()
        for entity_id in entityDict:
            pattern = entityDict[entity_id].keyword_list
            if Tools.isExists(pattern, article.title) or\
                Tools.isExists(pattern, article.content) or Tools.isExists(pattern, article.entity):
                hitEntityList.append(entity_id)

        return hitEntityList
        

class EventIdentificationAnalytics(AnalyticsBase):
    '''
    事件识别分析
    '''

    def __init__(self, dbProxy, entity_id, logger=None):
        '''
        Constructor
        '''

        super(EventIdentificationAnalytics, self).__init__(dbProxy, entity_id, logger)
        stopWordsFile = self.saConf.getConf(self.saConf.CONF_STOPS_WORDS_FILE)
        userDictFile = self.saConf.getConf(self.saConf.CONF_USER_DICT_FILE)
        if 'window' in platform.system().lower():
            stopWordsFile = stopWordsFile.replace('/','\\')
            userDictFile = userDictFile.replace('/','\\')

        words = os_file.current_dir
        stopWordsFile = os.path.join(words,stopWordsFile)
        userDictFile =  os.path.join(words,userDictFile)

        self.stopWords = Tools.fetchStopWords(stopWordsFile)
        jieba.load_userdict(userDictFile)

    
    def analysis(self, article):
        '''
        分析该文章属于给定实体的哪个事件
        '''        
        entityEventDict = GlobalVariable.getEventMgmt().entityEventDict
        if entityEventDict is None or self.entity_id not in entityEventDict:
            self.logger.warn('Entity %d not found in system', self.entity_id)
            return []
        
        eventDict = entityEventDict[self.entity_id]
        return self.__analysisEvent(article, eventDict)

    def __analysisEvent(self, article, eventDict):
        hitEventList = list()
        for event in eventDict.values():
            algorithm = event.algorithm
            algorithm_conf = event.algorithm_conf
            if algorithm == Constants.EVENT_IDENTIFICATION_ALGORITHM_KW:
                jo = json.loads(algorithm_conf)
                keywordList = jo[Constants.EVENT_IDENTIFICATION_ALGORITHM_KW_FIELD_KW]
                nonKeyword = jo[Constants.EVENT_IDENTIFICATION_ALGORITHM_KW_FIELD_NON_KW]
                title = article.title
                content = article.content
                ##Tools.
                if self.__isExists(keywordList, title) or\
                    self.__isExists(keywordList, content):
                    if nonKeyword!="":
                        if not self.__isExists(nonKeyword, title) and\
                            not self.__isExists(nonKeyword, content):
                            hitEventList.append(event.event_id)
                    else:
                        hitEventList.append(event.event_id)
            elif algorithm == Constants.EVENT_IDENTIFICATION_ALGORITHM_SIMILARITY:
                jo = json.loads(algorithm_conf)
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
                        hitEventList.append(event.event_id)
                        break
        return hitEventList
    
    def __isExists(self, keywordListStr, s):
        kl = keywordListStr.split(',')
        match = True
        for k in kl:
            if not Tools.isExists(k, s):
                match = False
                break
        return match

    def analysisBefore(self, event_id):
        entityEventDict = dict({'event_id':event_id})
        if entityEventDict is None or self.entity_id not in entityEventDict:
            self.logger.warn('Entity %d not found in system', self.entity_id)
            return []

        eventDict = entityEventDict[self.entity_id]

        articleTableName = Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        eventTableName = Constants.TABLE_SA_EVENT + Constants.TABLE_NAME_DELIMITER + self.entity_id
        sqlArticlListBefore = '''
        SELECT a.TID,a.CHANNEL_ID, a.TITLE, a.CONTENT, a.PUBLISH_DATETIME, a.URL, a.AUTHOR_ID, a.AUTHOR_NAME,
        a.PUBLISH_METHOD, a.DIGEST, a.HEAT,
        FROM %s as a, %s as e
        WHERE a.PUBLISH_DATETIME > e.START_DATETIME
        '''%(articleTableName, eventTableName)
        self.dbProxy.execute(sqlArticlListBefore)
        resultList = self.dbProxy.fetchall()
        articleList = map(lambda item: Article(
            item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10]
        ), resultList)

        hitEventList = list()
        for article in articleList:
            hitEventList.append(self.__analysisEvent(article, eventDict))
        return hitEventList