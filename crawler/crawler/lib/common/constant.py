# -*- coding:utf-8 -*-
'''
Created on 1 Oct 2017

@author: eyaomai
'''
class Constants(object):
    '''
            系统常量
    '''
    TABLE_NAME_DELIMITER = '_'
    TABLE_ARTICLE_WECHAT = 'sa_article_wechat'
    TABLE_PUBLIC_INFO_WECHAT = 'sa_public_info_wechat'
    TABLE_SA_SYS_CONF = 'sa_sys_conf'
    TABLE_SA_ENTITY_CONF = 'sa_entity_conf'
    TABLE_SA_EVENT = 'sa_event'
    TABLE_SA_ARTICLE = 'sa_article'
    TABLE_SA_ARTICLE_HISTORY = 'sa_article_history'
    TABLE_SA_ARTICLE_REMOVE = 'sa_article_remove'
    TABLE_SA_EVENT_ARTICLE = 'sa_event_article'
    TABLE_SA_EVENT_ARTICLE_HISTORY = 'sa_event_article_history'
    TABLE_SA_EVENT_MEDIA_STATISTICS = 'sa_event_media_statistics'
    TABLE_SA_EVENT_ARTICLE_REMOVE = 'sa_event_article_remove'
    TABLE_SA_EVENT_MEDIA_HISTORY_STATISTICS = 'sa_event_media_statistics_history'
    TABLE_SA_EVENT_WARNING_CONF = 'sa_event_warning_conf'
    TABLE_SA_COMMENT = 'sa_comment_wechat'
    TABLE_SA_CHANNEL = 'sa_channel'
    TABLE_SA_CHANNEL_TYPE = 'sa_channel_type'
    TABLE_SA_WARNING_SIGNAL = 'sa_warning_signal'
    TABLE_SA_WARNING_SIGNAL_HISTORY = 'sa_warning_signal_history'
    TABLE_SA_WARNING_NOTIFY = 'sa_warning_notify'
    TABLE_SA_HEAT_WORDS = 'sa_heat_words'
    TABLE_SA_HEAT_WORDS_ARTICLE = 'sa_heat_words_article'
    TABLE_SA_HEAT_WORDS_HISTORY = 'sa_heat_words_history'
    TABLE_SA_HEAT_WORDS_REMOVE = 'sa_heat_words_remove'
    TABLE_SA_CLUSTER = 'sa_cluster'
    TABLE_SA_CLUSTER_ARTICLE = 'sa_cluster_article'
    TABLE_SA_CLUSTER_HISTORY = 'sa_cluster_history'
    TABLE_SA_NOTIFY_CONF = 'sa_notify_conf'
    TABLE_SA_SENSITIVE_WORDS = 'sa_sensitive_words'
    TABLE_SA_CLASSIFY_ARTICLE = 'sa_classify_article'
    TABLE_SA_MEDIA_TREND = 'sa_media_trend'
    TABLE_SA_WETCHARTPUBLIC = 'sa_wetchartpublic'
    TABLE_SA_REPORT_STATUS = 'sa_report_status'
    TABLE_SA_ARTICLE_WARNING_CONF = 'sa_article_warning_conf'
    TABLE_SA_THRESHOLD_CONFIG = 'sa_threshold_config'
    EVENT_IDENTIFICATION_ALGORITHM_KW = 'KEYWORDS'
    EVENT_IDENTIFICATION_ALGORITHM_KW_FIELD_KW = 'keyword_list'
    EVENT_IDENTIFICATION_ALGORITHM_KW_FIELD_NON_KW = 'non_keyword_list'
    EVENT_IDENTIFICATION_ALGORITHM_SIMILARITY = 'SIMILARITY'

    #测试指标预警和敏感词预警代码 2018.5.17
    TABLE_SA_ARTICLE_INDICATOR_WARNING_SIGNAL = 'sa_article_indicator_warning_signal'
    TABLE_SA_ARTICLE_INDICATOR_WARNING_SIGNAL_HISTORY = 'sa_article_indicator_warning_signal_history'
    TABLE_SA_ARTICLE_SENSITIVE_WARNING_SIGNAL = 'sa_article_sensitive_warning_signal'
    TABLE_SA_EVENT_INDICATOR_WARNING_SIGNAL = 'sa_event_indicator_warning_signal'
    TABLE_SA_EVENT_INDICATOR_WARNING_SIGNAL_HISTORY = 'sa_event_indicator_warning_signal_history'
    
    CHANNEL_TYPE_NEWS = 1
    CHANNEL_TYPE_LUNTAN = 2
    CHANNEL_TYPE_TIEBA = 3
    CHANNEL_TYPE_ZHIHU = 4
    CHANNEL_TYPE_WEIBO = 5
    CHANNEL_TYPE_WECHAT = 6

    OBJECT_TYPE_ARTICLE = 1
    OBJECT_TYPE_EVENT = 2 
    OBJECT_TYPE_MEDIA = 3

    WARNING_DATETIME = 0
    WARNNING_TYPE_UNDEFINED = 0
    WARNNING_TYPE_READ = 1
    WARNNING_TYPE_REPLY = 2
    WARNNING_TYPE_FORWARD = 3
    WARNNING_TYPE_HEAT = 4
    WARNNING_TYPE_ORIGINAL = 5
    WARNNING_TYPE_MEDIA = 6
    WARNNING_TYPE_NETIZEN = 7
    WARNNING_TYPE_COUNTRY = 8
    WARNNING_TYPE_PROVINCE = 9
    WARNNING_TYPE_SENSITIVE = 10
    WARNNING_TYPE_DAILYREPORT = 11

    CLUSTER_SIZE_LARGE = 1
    CLUSTER_SIZE_MEDIUM = 2
    CLUSTER_SIZE_SMALL = 3

    ANATYPE_HEATWORDS = 'heatwords'
    ANATYPE_CLUSTER = 'cluster'
    ANATYPE_EVENT = 'event'
    ANATYPE_INDICATOR_WARNING = 'indicator'
    ANATYPE_SENSITIVE_WARNING = 'sensitive'
    ANATYPE_FRIENDLY = 'friendly'
    ANATYPE_CLASSIFIED = 'classify'
    ANATYPE_ARTICLE_EXTINCT = 'articleextinct'
    ANATYPE_EVENT_EXTINCT = 'eventextinct'
    ANATYPE_BACK_TRACK = 'back_track'
    
    AVAILABLE_ANATYPE_LIST = [ANATYPE_HEATWORDS, ANATYPE_CLUSTER, ANATYPE_EVENT, ANATYPE_INDICATOR_WARNING,ANATYPE_FRIENDLY,ANATYPE_ARTICLE_EXTINCT,ANATYPE_EVENT_EXTINCT ,ANATYPE_BACK_TRACK]
    
    DEFAULT_PUBLISH_DATETIME = '1970-01-01 00:00:00'
    DEFAULT_PUBLISH_METHOD = ''
    DEFAULT_AUTHOR_ID = ''
    DEFAULT_AUTHOR_NAME = ''
    DEFAULT_DIGEST = ''
    DEFAULT_NUM = 'null'
    DEFAULT_STR = 'null'
    
    NOTIFY_METHOD_EMAIL = 'email'
    NOTIFY_METHOD_SMS = 'sms'
    NOTIFY_METHOD_WECHAT = 'wechat'
    
    ARTICLE_NATURE_DEFAULT = -2
    ARTICLE_NATURE_NEGATIVE = -1
    ARTICLE_NATURE_NEUTRAL = 0
    ARTICLE_NATURE_POSITIVE = 1
    
    HEAT_ALGORITHM_VERSION1 = 'v1'
    HEAT_ALGORITHM_VERSION2 = 'v2'