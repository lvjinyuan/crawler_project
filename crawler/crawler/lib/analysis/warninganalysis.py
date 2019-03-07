# -*- coding:utf-8 -*-
'''
Created on 3 Oct 2017

@author: eyaomai
'''
import sys, os

from conf import os_file

sys.path.append(sys.argv[0][:sys.argv[0].rfind(os.path.join('com', 'naswork'))])

from crawler.lib.analysis.analysisbase import AnalyticsBase, Tools
from crawler.lib.common.constant import Constants
from crawler.lib.objectmodel.warningsignal import EventIndicatorWarningSignal, ArticleSensitiveWarningSignal, ArticleIndicatorWarningSignal
from crawler.lib.objectmodel.article import ArticleStatistics, Article
from crawler.lib.objectmodel.event import EventManager
from crawler.lib.objectmodel.entity import EntityManager

import json, time, math, datetime, re, platform
import jieba.analyse

# reload(sys)
# sys.setdefaultencoding('utf8')


class ArticleWarningAnalytics(AnalyticsBase):
    def __init__(self, dbProxy, entity_id, logger=None):
        '''
        Constructor
        '''
        super(ArticleWarningAnalytics, self).__init__(dbProxy, entity_id, logger)
        self.__conf = list()
        self.__fetchConfiguration()

    def analysis(self, **kwargs):
        self.logger.info('Begin to analyse for %s', self.entity_id)
        kwargs['articleList'] = self.__fetchFromArticle()
        kwargs['commit'] = True
        if 'articleList' in kwargs:
            articleList = kwargs['articleList']
            if len(articleList) == 0:
                return
            stopWordsFile = self.saConf.getConf(self.saConf.CONF_STOPS_WORDS_FILE)
            if 'window' in platform.system().lower():
                # 配置路径
                word = os_file.current_dir
                origin_path = word+'\\'
                stopWordsFile = origin_path + stopWordsFile.replace('/','\\')
            # stopWordsFile = 'conf/stopwords.cfg'
            # 获取stopwords

            stopWords = Tools.fetchStopWords(stopWordsFile)
            # 获取sentisyword配置
            sensitiveConf = json.loads(self.saConf.getConf(self.saConf.CONF_SENSITIVE_WORD_CONF))
            topK = sensitiveConf['topK']
            weight = sensitiveConf['weight']
            # topK = 20
            # weight = 0.03
            warningDatetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            signalList = list()
            for article in articleList:
                # 对文章内容过滤停用词
                allContents=str(article.content)
                for w in stopWords:
                    try:
                        allContents = allContents.replace(w, '')
                    except:
                        pass
                allContents = re.sub(r'([\d]+)','',allContents)
                allContents = re.sub(r'([\w]+)','',allContents)

                # 对文章内容进行tfidf权重判断
                wordList = list(jieba.analyse.extract_tags(allContents, topK=topK, withWeight=True))
                filterWordListStrList = map(lambda y: y[0], filter(lambda x:x[1]>weight, wordList))
                # 对文章标题进行分词
                titleWordListStrList = list(jieba.cut(article.title))
                for (nature, confList, sid) in self.__conf:
                    if len(confList)==0:
                        break
                    found = True
                    wordSet = set()
                    for keyword in confList:
                        ts = set(Tools.findAllFromList(keyword, titleWordListStrList))
                        cs = set(Tools.findAllFromList(keyword, filterWordListStrList))
                        m = ts.union(cs)
                        if len(m)>0:
                            wordSet.update(m)
                        else:
                            found = False
                            break

                    if found:
                        signalList.append(ArticleSensitiveWarningSignal(article.tid,article.channel_id,nature,sid, 0,Constants.WARNNING_TYPE_SENSITIVE,
                                                                        warningDatetime, ','.join(confList), '|'.join(wordSet) ))
                        # self.logger.debug('Warning signal generated hit (SID=%d) for %s (tid=%s,channel_id=%d)',sid, article.title, article.tid, article.channel_id)
                        break
            if 'commit' in kwargs:
                if kwargs['commit']==True and len(signalList)>0:
                    self.__checkExists(signalList)
                    self.logger.debug(len(signalList))
                    self.__updateToDb(signalList)

    def __updateToDb(self, signalList):
        if len(signalList)>0:
            insertSql = '''  
            INSERT INTO %s (tid, channel_id, nature, sid, warning_type, warning_datetime, threshold_value, data_value) VALUES 
            '''
            valueList = list()
            for articleSignal in signalList:
                self.logger.debug(articleSignal.tid)
                valueList.append('("%s", %d, %d, %d, %d, "%s", "%s","%s")' % (
                    articleSignal.tid, articleSignal.channel_id, articleSignal.nature,
                    articleSignal.sid, articleSignal.warning_type,articleSignal.warning_datetime,
                    articleSignal.threshold_value, articleSignal.data_value
                ))

            #插入当前表
            tableName = Constants.TABLE_SA_ARTICLE_SENSITIVE_WARNING_SIGNAL+Constants.TABLE_NAME_DELIMITER+self.entity_id
            self.dbProxy.execute(insertSql%tableName + ','.join(valueList))

            #插入历史表
            #tableName = Constants.TABLE_SA_WARNING_SIGNAL_HISTORY+Constants.TABLE_NAME_DELIMITER+self.entity_id
            #self.dbProxy.execute(insertSql % tableName + ','.join(valueList))

            self.dbProxy.commit()

    def __fetchConfiguration(self):
        self.__conf = list()
        sql = 'SELECT nature, keyword_conf,sid from %s where enable="Y" order by sequence desc' % (Constants.TABLE_SA_SENSITIVE_WORDS+Constants.TABLE_NAME_DELIMITER+self.entity_id)
        self.dbProxy.execute(sql)
        for item in self.dbProxy.fetchall():
            nature = item[0]
            keyword_conf = item[1]
            sid = item[2]
            commaSepList = keyword_conf.split(',')
            andCond = list()
            for cond in commaSepList:
                sepList = filter(lambda y: y!='', map(lambda x:x.strip(),cond.split('|')))
                if len(sepList)>0:
                    andCond.append('|'.join(sepList))
            self.__conf.append((nature,andCond,sid))

    def __fetchFromArticle(self):
        '''
        获取文章列表
        '''
        tableName = Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        date = datetime.datetime.now() - datetime.timedelta(days=7)
        date_str = date.strftime('%Y-%m-%d')
        # date_str = '2018-04-04'

        # 主要指标
        selectSql = '''select tid, title, channel_id,checked, content
                        from %s
                        where add_datetime >= %s
        ''' % (tableName, date_str)

        self.dbProxy.execute(selectSql)
        results = self.dbProxy.fetchall()

        articleList = list()
        for item in results:
            article = Article(item[0], item[2], item[1], content=item[3])
            articleList.append(article)
        return articleList

    def __checkExists(self, signalList):
        if len(signalList) != 0:
            tableName = Constants.TABLE_SA_WARNING_SIGNAL + Constants.TABLE_NAME_DELIMITER + self.entity_id
            sql = '''select object_id, level, nature, channel_id from %s where object_type=1 and warning_type=%d and (%s)
            '''
            whereClauseList = list()
            for item in signalList:
                whereClauseList.append('(object_id="%s" and level=%d and channel_id=%d)' %(
                    item.tid,
                    item.level,
                    item.channel_id))
            self.dbProxy.execute(sql %(tableName, Constants.WARNNING_TYPE_SENSITIVE, ' or '.join(whereClauseList)))
            results = self.dbProxy.fetchall()
            self.logger.debug(len(results))
            self.logger.debug(len(signalList))

            for item in results:
                for article in signalList:
                    if item[0] == article.tid and item[1] == article.level and  item[2] == article.nature and item[3] == article.channel_id:
                        signalList.remove(article)
            self.logger.debug(len(signalList))


class IndicatorWarningAnalytics(AnalyticsBase):
    '''
    指标预警
    '''

    def __init__(self, dbProxy, entity_id, logger=None):
        '''
        Constructor
        '''
        super(IndicatorWarningAnalytics, self).__init__(dbProxy, entity_id, logger)

    def analysis(self, **kwargs):
        self.logger.info('Begin to analyse for %s', self.entity_id)
        self._eventAnalysis()
        # self._articleAnalysis()

    # 事件指标预警
    def _eventAnalysis(self):
        # 获取阀值，操作的表是sa_threshold_config, 条件为system_type=1
        thresholdList, sys_threshold, ene_threshold = self.__fetchThreshold(1)

        # 根据媒体时间表获取信息,
        eventIndDict = self.__fetchFromEventMediaStatistics()
        # self.__fetchFromComment(eventIndDict) # 获取评论表由于数据太少取消使用

        # check event media statistics
        eventWarningDict = dict()
        warningDatetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for eventInd in eventIndDict.values():
            warningTypeSet = set()
            # self.logger.debug(eventInd.event_id)
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
                    if thresholdConf.type == 'sta_reply' and Constants.WARNNING_TYPE_REPLY not in warningTypeSet:  # 预警值，避免重复
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
                        except KeyError as e:
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

        # 老版本，新版本没用到该方法
        # self.__deleteDifferent(eventWarningDict)
        self.logger.debug('Event warning count(%d)' % len(eventWarningDict))

        self.__checkExists(eventWarningDict)

        self.logger.debug('Event warning count(%d) after check exists' % len(eventWarningDict))

        self.__updateToDb(eventWarningDict)

        self.__notify(eventWarningDict)

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
            # 插入当前表  改：20180608 将插入的表改为sa_event_indicator_warning_signal_<entity>
            tableName = Constants.TABLE_SA_EVENT_INDICATOR_WARNING_SIGNAL + Constants.TABLE_NAME_DELIMITER + self.entity_id
            self.dbProxy.execute(insertSql % (tableName, ','.join(valueList)))

            # # 插入历史表
            tableName = Constants.TABLE_SA_EVENT_INDICATOR_WARNING_SIGNAL_HISTORY + Constants.TABLE_NAME_DELIMITER + self.entity_id
            self.dbProxy.execute(insertHisSql % (tableName, ','.join(valueList)))

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
            # jj = sql % (tableName, ' or '.join(whereClauseList))
            # self.logger.debug(jj)
            self.dbProxy.execute(sql % (tableName, ' or '.join(whereClauseList)))
            # 获取到已经预警的全部事件
            results = self.dbProxy.fetchall()
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
                    if eventWarningDict[index].level < item[1]:
                        historyListDict[index] = eventWarningDict[index]
                    else:
                        del eventWarningDict[index]

            self.logger.debug(len(historyListDict))
            return historyListDict

    def __fetchFromComment(self, eventIndDict):
        """
        获取评论数，由于数据量太少，暂时不采用。
        :param eventIndDict:
        :return:
        """
        tableName = Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        # tableName->sa_event_article_(entity)

        # #国家数
        # selectSql = '''
        # select event.event_id, count(distinct(comment.location_country))
        # from %s as comment, %s as event
        # where comment.tid = event.tid and comment.channel_id = event.channel_id and event_id <> 0
        # and location_country<>'' and location_country<>'%s'
        # group by event.event_id
        # ''' %  (Constants.TABLE_SA_COMMENT, tableName, u'中国')
        # self.dbProxy.execute(selectSql)
        # results = self.dbProxy.fetchall()
        # for item in results:
        #     if item[0] not in eventIndDict:
        #         eventIndDict[item[0]] = EventIndicator()
        #     eventIndDict[item[0]].foreign_count = item[1]

        # #省份数
        # selectSql = '''
        # select event.event_id, count(distinct(location_region))
        # from %s as comment, %s as event
        # where comment.tid = event.tid and comment.channel_id = event.channel_id
        # and ip_address<>'' and location_country='CN' and event_id<>0
        # group by event.event_id
        # ''' %  (Constants.TABLE_SA_COMMENT, tableName)
        # self.dbProxy.execute(selectSql)
        # results = self.dbProxy.fetchall()
        # for item in results:
        #     if item[0] not in eventIndDict:
        #         eventIndDict[item[0]] = EventIndicator()
        #     eventIndDict[item[0]].province_count = item[1]

        # 网民数
        # selectSql = '''
        # select event.event_id, count(*)
        # from %s as comment, %s as event
        # where comment.tid = event.tid and comment.channel_id = event.channel_id
        # and ip_address<>'' and event_id<>0
        # group by event.event_id
        # ''' % (Constants.TABLE_SA_COMMENT, tableName)
        # self.dbProxy.execute(selectSql)
        # results = self.dbProxy.fetchall()
        # for item in results:
        #     if item[0] not in eventIndDict:
        #         eventIndDict[item[0]] = EventIndicator()
        #     eventIndDict[item[0]].netizen_count = item[1]

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
        sys_threshold = dict()  # 2018.5.30 系统预警 姚
        ene_threshold = dict()  # 2018.5.30 集团预警 姚
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

        # 主要指标->按事件分组的数据
        if ids_str is not '()':
            selectSql = '''select event_id, read_count, like_count, reply_count, forward_count,
                            collect_count, heat, original_count
                            from %s where event_id<>0 and channel_id=0 and event_id not in %s
            ''' % (tableName, ids_str)  # where event_id<>0 and channel_id=0 2018.5.30 注释测试

            selectMediaSql = '''
                           select event_id, channel_id from %s 
                           where event_id<>0 and channel_id<>0 and event_id not in %s order by CHANNEL_ID
                           ''' % (tableName, ids_str)
        else:
            selectSql = '''select event_id, read_count, like_count, reply_count, forward_count,
                                        collect_count, heat, original_count
                                        from %s where event_id<>0 and channel_id=0
                        ''' % tableName

            selectMediaSql = '''
                           select event_id, channel_id from %s 
                           where event_id<>0 and channel_id<>0 order by CHANNEL_ID
                           ''' % tableName

        self.dbProxy.execute(selectSql)
        results = self.dbProxy.fetchall()

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

    # 单贴指标预警->只对单篇文章进行预警
    def _articleAnalysis(self):
        # 获取阀值
        thresholdList, sys_threshold, ene_threshold = self.__fetchThreshold(2) #返回thresholdList, sys_threshold, ene_threshold
        articleIndDict = self.__fetchFromArticleMediaStatistics()               #返回tid, read_count, forward_count, reply_count, channel_id 字典

        articleWarningDict = dict()
        warningDatetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for articleInd in articleIndDict.values():
            warningTypeSet = set()
            is_set = {
                'sta_reply': False,
                'sta_read': False,
                'sta_forward': False
            }
            for thresholdConf in thresholdList:
                # 阅读数预警
                threshold = thresholdConf.threshold
                if str(articleInd.event_id) == thresholdConf.biz_id:
                    if thresholdConf.type == 'sta_read' and Constants.WARNNING_TYPE_READ not in warningTypeSet: #避免重复预警
                        if eval(threshold.replace('v', str(articleInd.read_count))):
                            obj = ArticleIndicatorWarningSignal(thresholdConf.id, articleInd.tid, articleInd.channel_id,
                                                                articleInd.event_id, None, None, thresholdConf.level,
                                                                Constants.WARNNING_TYPE_READ, warningDatetime, threshold,
                                                                articleInd.read_count)
                            index = articleInd.index + '_' + str(Constants.WARNNING_TYPE_READ)
                            articleWarningDict[index] = obj
                            warningTypeSet.add(Constants.WARNNING_TYPE_READ)

                    # 转发数预警
                    if thresholdConf.type == 'sta_forward' and Constants.WARNNING_TYPE_FORWARD not in warningTypeSet:
                        if eval(threshold.replace('v', str(articleInd.forward_count))):
                            obj = ArticleIndicatorWarningSignal(thresholdConf.id, articleInd.tid, articleInd.channel_id,
                                                                articleInd.event_id, None, None, thresholdConf.level,
                                                                Constants.WARNNING_TYPE_FORWARD, warningDatetime,
                                                                threshold, articleInd.forward_count)
                            index = articleInd.index + '_' + str(Constants.WARNNING_TYPE_FORWARD)
                            articleWarningDict[index] = obj
                            warningTypeSet.add(Constants.WARNNING_TYPE_FORWARD)

                    # 回复数预警
                    if thresholdConf.type == 'sta_reply' and Constants.WARNNING_TYPE_REPLY not in warningTypeSet:
                        if eval(threshold.replace('v', str(articleInd.reply_count))):
                            obj = ArticleIndicatorWarningSignal(thresholdConf.id, articleInd.tid, articleInd.channel_id,
                                                                articleInd.event_id, None, None, thresholdConf.level,
                                                                Constants.WARNNING_TYPE_REPLY, warningDatetime,
                                                                threshold, articleInd.reply_count)
                            index = articleInd.index + '_' + str(Constants.WARNNING_TYPE_REPLY)
                            articleWarningDict[index] = obj
                            warningTypeSet.add(Constants.WARNNING_TYPE_REPLY)
            # 未设置阀值的事件自动预警，集团或者系统的阀值
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
                        # 阅读数预警
                        if threshold_type == 'sta_read' and Constants.WARNNING_TYPE_READ not in warningTypeSet:
                            if eval(threshold.replace('v', str(articleInd.read_count))):
                                obj = ArticleIndicatorWarningSignal(threshold_id, articleInd.tid, articleInd.channel_id,
                                                                    articleInd.event_id, None, None, str(i),
                                                                    Constants.WARNNING_TYPE_READ, warningDatetime,
                                                                    threshold, articleInd.read_count)
                                index = articleInd.index + '_' + str(Constants.WARNNING_TYPE_READ)
                                articleWarningDict[index] = obj
                                warningTypeSet.add(Constants.WARNNING_TYPE_READ)

                        # 转发数预警
                        if threshold_type == 'sta_forward' and Constants.WARNNING_TYPE_FORWARD not in warningTypeSet:
                            if eval(threshold.replace('v', str(articleInd.forward_count))):
                                obj = ArticleIndicatorWarningSignal(threshold_id, articleInd.tid, articleInd.channel_id,
                                                                    articleInd.event_id, None, None, str(i),
                                                                    Constants.WARNNING_TYPE_FORWARD, warningDatetime,
                                                                    threshold, articleInd.forward_count)
                                index = articleInd.index + '_' + str(Constants.WARNNING_TYPE_FORWARD)
                                articleWarningDict[index] = obj
                                warningTypeSet.add(Constants.WARNNING_TYPE_FORWARD)

                        # 回复数预警
                        if threshold_type == 'sta_reply' and Constants.WARNNING_TYPE_REPLY not in warningTypeSet:
                            if eval(threshold.replace('v', str(articleInd.reply_count))):
                                obj = ArticleIndicatorWarningSignal(threshold_id, articleInd.tid, articleInd.channel_id,
                                                                    articleInd.event_id, None, None, str(i),
                                                                    Constants.WARNNING_TYPE_REPLY, warningDatetime,
                                                                    threshold, articleInd.reply_count)
                                index = articleInd.index + '_' + str(Constants.WARNNING_TYPE_REPLY)
                                articleWarningDict[index] = obj
                                warningTypeSet.add(Constants.WARNNING_TYPE_REPLY)

        self.logger.debug(len(articleWarningDict))

        self.__updateArticleWarningToDb(articleWarningDict)
        self.logger.debug(len(articleWarningDict))

        self.__notify(articleWarningDict)

    def __checkArticleWarningExists(self, articleWarningDict):
        # 查看预警表中是否已经预警过了 -> 2018.06.11 Jondar
        historyWarningDict = dict()
        if len(articleWarningDict.values()) != 0:
            tableName = Constants.TABLE_SA_ARTICLE_INDICATOR_WARNING_SIGNAL + Constants.TABLE_NAME_DELIMITER + self.entity_id
            # tableName -> sa_article_indicator_warning_signal_<entity>
            sql = '''select tid, event_id, channel_id, warning_type, level, data_value from %s where (%s)
            '''
            whereClauseList = list()
            for item in articleWarningDict.values():
                whereClauseList.append(
                    ' (tid="%s" and event_id=%s and warning_type=%s and channel_id=%s)' % (
                        item.tid,
                        item.event_id,
                        item.warning_type,
                        item.channel_id,
                    ))
            self.dbProxy.execute(sql % (tableName, ' or '.join(whereClauseList)))
            results = self.dbProxy.fetchall()

            for item in results:
                index = '_'.join([str(item[0]), str(item[1]), str(item[2]), str(item[3])])
                # 判断这篇文章的最新预警level是否比之前级别高,level越小等级越高
                if int(articleWarningDict[index].level) <= int(item[4]) and \
                        articleWarningDict[index].data_value > item[5]:
                    historyWarningDict[index] = articleWarningDict[index]
                else:
                    historyWarningDict[index] = articleWarningDict[index]
                    del articleWarningDict[index]
            return historyWarningDict

    def __fetchArticleThreshold(self):
        """
        获取文章的阈值配置
        :return:
        """
        tableName = Constants.TABLE_SA_ARTICLE_WARNING_CONF + Constants.TABLE_NAME_DELIMITER + self.entity_id
        selectSql = '''select article_id, level, threshold_read, threshold_forward, threshold_reply
                        from %s
                        order by level asc
                        limit 100
        ''' % (tableName)

        self.dbProxy.execute(selectSql)
        results = self.dbProxy.fetchall()

        thresholdList = list()
        for item in results:
            thresholdConf = ThresholdConf() #初始化，返回默认值
            thresholdConf.event_id = item[0]
            thresholdConf.level = item[1]
            thresholdConf.read_count = item[2]
            thresholdConf.forward_count = item[3]
            thresholdConf.reply_count = item[4]
            thresholdList.append(thresholdConf)
        return thresholdList

    def __fetchFromArticleMediaStatistics(self):
        '''
        根据文章表的数据获取所能获取的信息
        '''
        # 从sa_article_<entity>表中获取数据
        article_tableName = Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        # 从sa_event_<entity>表中获取数据
        event_tableName = Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id

        date = datetime.datetime.now() - datetime.timedelta(days=7)
        date_str = date.strftime('%Y-%m-%d')

        # 主要指标->sa_article_<entity> 文章指标预警
        selectSql1 = '''select tid, read_count, forward_count, reply_count, channel_id
                      from %s where add_datetime >= '%s' and extinct='N'
        ''' % (article_tableName, date_str)

        self.dbProxy.execute(selectSql1)
        article_results = self.dbProxy.fetchall()

        # 主要指标->sa_event_article_<entity>事件文章指标预警
        # 获取entityID列表
        entityDict = EntityManager(self.dbProxy).entityDict

        # 1、从sa_event_<entity>数据表中获取参与预警的数据ID列表
        entityEventDict = EventManager(self.dbProxy, entityDict).entityEventDict
        eventDict = entityEventDict[self.entity_id.upper()]

        # 2、取出事件类型为监测的event_id列表,即event_type=2
        eventIdList = [eventDict[i].event_id for i in eventDict if eventDict[i].event_type == '2']

        # 3、然后从sa_event_article_<entity>中获取事件列表
        selectSql2 = '''select tid, event_id, read_count, forward_count, reply_count, channel_id
                      from %s where add_datetime >= '%s' and manual_remove='N' and extinct='N'
        ''' % (event_tableName, date_str)

        self.dbProxy.execute(selectSql2)
        event_results = self.dbProxy.fetchall()
        articleIndDict = dict()
        for item in article_results:
            articleInd = ArticleStatistics(item[0], item[4])
            articleInd.event_id = 0
            articleInd.tid = item[0]
            articleInd.read_count = item[1]
            articleInd.forward_count = item[2]
            articleInd.reply_count = item[3]
            articleInd.channel_id = item[4]
            index = '_'.join([str(item[0]), '0', str(item[4])])
            articleInd.index = index
            articleIndDict[index] = articleInd

        for item in event_results:
            # 事件文章有效
            if item[1] in eventIdList:
                articleInd = ArticleStatistics(item[0], item[5])
                articleInd.tid = item[0]
                articleInd.event_id = item[1]
                articleInd.read_count = item[2]
                articleInd.forward_count = item[3]
                articleInd.reply_count = item[4]
                articleInd.channel_id = item[5]
                index = '_'.join([str(item[0]), str(item[1]), str(item[5])])
                articleInd.index = index
                articleIndDict[index] = articleInd

        return articleIndDict

    def __updateArticleWarningToDb(self, articleWarningDict):
        # 是否已经预警过
        hisWarningDict = self.__checkArticleWarningExists(articleWarningDict)
        # 插入数据库
        # 如果预警的级别变高了，则要在预警的基础上进行预警
        insertSql = '''
                    INSERT INTO %s (config_id, tid, event_id, level, warning_type, warning_datetime, threshold_value, 
                    data_value, channel_id)
                    VALUES %s
                    ON DUPLICATE KEY UPDATE config_id=VALUES(config_id), level = VALUES(level), 
                    threshold_value = VALUES(threshold_value), warning_datetime = VALUES(warning_datetime), 
                    data_value = VALUES(data_value), warning_type = VALUES(warning_type)
                    '''

        insertHisSql = '''
                    INSERT INTO %s (config_id, tid, event_id, level, warning_type, warning_datetime, threshold_value, 
                    data_value, channel_id)
                    VALUES %s
        '''

        valueList = list()
        historyList = list()
        if len(articleWarningDict) > 0:

            for articleWarningObj in articleWarningDict.values():
                valueList.append('("%s", "%s", %s, %s, %s, "%s", "%s", "%s", %s)' % (
                    articleWarningObj.config_id, articleWarningObj.tid, articleWarningObj.event_id,
                    articleWarningObj.level, articleWarningObj.warning_type, articleWarningObj.warning_datetime,
                    articleWarningObj.threshold_value, articleWarningObj.data_value, articleWarningObj.channel_id
                ))

            # 插入当前表
            tableName = Constants.TABLE_SA_ARTICLE_INDICATOR_WARNING_SIGNAL + Constants.TABLE_NAME_DELIMITER + self.entity_id
            self.dbProxy.execute(insertSql % (tableName, ','.join(valueList)))

            # 插入历史表
            tableName = Constants.TABLE_SA_ARTICLE_INDICATOR_WARNING_SIGNAL_HISTORY + Constants.TABLE_NAME_DELIMITER + self.entity_id
            # 第一次插入时
            self.dbProxy.execute(insertSql % (tableName, ','.join(valueList)))

        if hisWarningDict:
            for articleWarningObj in hisWarningDict.values():
                historyList.append('("%s", "%s", %s, %s, %s, "%s", "%s", "%s", %s)' % (
                    articleWarningObj.config_id, articleWarningObj.tid, articleWarningObj.event_id,
                    articleWarningObj.level, articleWarningObj.warning_type, articleWarningObj.warning_datetime,
                    articleWarningObj.threshold_value, articleWarningObj.data_value, articleWarningObj.channel_id
                ))
            tableName = Constants.TABLE_SA_ARTICLE_INDICATOR_WARNING_SIGNAL_HISTORY + Constants.TABLE_NAME_DELIMITER + self.entity_id
            self.dbProxy.execute(insertHisSql % (tableName, ','.join(historyList)))

        self.dbProxy.commit()

    def __notify(self, eventWarningDict):
        pass


class ThresholdConf(object):
    def __init__(self):
        self.id = 0  # 加了这个ID 20180624 Jondar
        self.biz_id = 0  # 2018.5.25加 用来判断 姚
        self.level = 0
        self.type = 0
        self.threshold = ''
        self.data_scope = 1
        self.package_type = ''
        self.corp_code = ''  # 2018.5.25加 用来判断 姚


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
