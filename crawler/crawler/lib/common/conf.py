# -*- coding:utf-8 -*-
'''
Created on 1 Oct 2017

@author: eyaomai
'''
from crawler.lib.common.constant import Constants
class SAConfiguration(object):
    '''
            系统配置
    '''

    CONF_CLUSTER_DAYS = 'cluster_days'
    CONF_CLUSTER_CONF = 'cluster_conf'
    CONF_HEAT_WORDS_DAYS = 'heat_words_days'
    CONF_HEAT_WORDS_NUM = 'heat_words_num'
    CONF_WARNING_RETAIN_DAYS = 'warning_retain_days'
    CONF_STOPS_WORDS_FILE = 'stop_words_file'
    CONF_USER_DICT_FILE = 'user_dict_file'
    CONF_CRAWLER_SEARCH_DAYS = 'crawler_search_days'
    CONF_HEAT_ALGORITHM = 'heat_algorithm'
    CONF_ARTICLE_EXTINCT_CONF = 'article_extinct_conf'
    CONF_EVENT_EXTINCT_CONF = 'event_extinct_conf'
    CONF_SENSITIVE_WORD_CONF = 'sensitive_word_conf'
    
    def __init__(self, dbProxy, entity=''):
        '''
        Constructor
        '''
        self.dbProxy = dbProxy
        self.entity = entity
        self.sysConfDict = dict()
        self.entityConfDict = dict()
        self.__readEntityConf()

    def getConf(self, key):
        if key in self.entityConfDict:
            return self.entityConfDict[key]
        else:
            return None

    def __readSysConf(self):
        '''
                        读取系统配置，直接从sa_conf_sys表读取
        '''
        sql = 'SELECT conf_id, conf_value from %s' % Constants.TABLE_SA_SYS_CONF
        self.dbProxy.execute(sql)
        results = self.dbProxy.fetchall()
        self.sysConfDict.clear()
        for item in results:
            self.sysConfDict[item[0]] = item[1]

    def __readEntityConf(self):
        '''
        从sa_conf_sys_entity表读取配置，覆盖系统配置
        '''
        self.__readSysConf()
        self.entityConfDict = self.sysConfDict.copy()
        if self.entity == '':
            return
        tableName = Constants.TABLE_SA_ENTITY_CONF + Constants.TABLE_NAME_DELIMITER + self.entity
        sql = 'SELECT conf_id, conf_value from %s' % tableName
        self.dbProxy.execute(sql)
        results = self.dbProxy.fetchall()
        for item in results:
            self.entityConfDict[item[0]] = item[1]
