
import re,pymysql,random,math,copy
from datetime import datetime
from crawler.lib.analysis.eventidentificationanalysis import SingleEventBackTrack
from crawler.lib.analysis.identificationanalysis import EntityIdentificationAnalytics, EventIdentificationAnalytics
from crawler.lib.analysis.warninganalysis import ArticleWarningAnalytics
from crawler.lib.analysis.constant import Constants
from crawler.lib.common.globalvar import GlobalVariable
from crawler.lib.analysis.utils import Logging, MySqlProxy
from crawler.lib.objectmodel.article import Article, ArticleStatistics
from crawler.settings.user_config import MYSQL_HOST, MYSQL_DBNAME, MYSQL_USER, MYSQL_PASSWD, MYSQL_PORT


def encodeText(content): #?
    return content.replace('"','\\"').replace("%", "\%")
    #return content.replace("'", "''").replace("%", "\%").replace(":", "\:")


class SaveMysqlPipeline():
    def __init__(self):
        # 最后更新时间
        self.now = datetime.utcnow().replace(microsecond=0).isoformat(' ')
        self.connect= pymysql.connect(
            host=MYSQL_HOST,
            db=MYSQL_DBNAME,
            user=MYSQL_USER,
            passwd=MYSQL_PASSWD,
            port = MYSQL_PORT,
        )
        # 使用cursor()方法创建一个游标对象
        self.cursor = self.connect.cursor()


    def wb_store(self,r):
        for item in r:
            try:
                # 查重处理
                self.cursor.execute(
                    """select * from sa_article_weibo_sysu where TID = %s""",
                    item['TID'])
                # 是否有重复数据
                repetition = self.cursor.fetchone()

                # 重复处理
                if repetition:
                    # 更新评论，点赞，转发数
                    updata =(item['dianzan'],item['relay'],item['comment'],self.now,item['TID'])
                    sql = "update sa_article_weibo_sysu set LIKE_COUNT=%s,FORWARD_COUNT=%s,REPLY_COUNT=%s,UPDATE_DATETIME=%s where TID = %s"
                    effect_row = self.cursor.execute(sql,updata)
                    self.connect.commit()
                else:
                    self.cursor.execute("""
                                    insert into sa_article_weibo_sysu(PUBLISH_METHOD,AUTHOR_NAME,AUTHOR_ID,CONTENT,LIKE_COUNT,FORWARD_COUNT,REPLY_COUNT,PUBLISH_DATETIME,URL,ADD_DATETIME,TID,CHANNEL_ID)
                                    values(%s, %s, %s, %s, %s, %s,%s, %s, %s,%s,%s,%s)
                                """,
                            (item['category'],item['author'],item['author_id'],item['content'],item['dianzan'],item['relay'],item['comment'],item['send_time'],item['comment_url'],self.now,item['TID'],9))
                    # 提交sql语句
                    self.connect.commit()
            except Exception as error:
                print(error)
                print('失败微博'+item["comment_url"])
        # 关闭游标
        # self.cursor.close()
        # 关闭连接
        # self.connect.close()
        print('成功保存数据库')

    def xl_store(self, r):
        for item in r:
            try:
                # 查重处理
                self.cursor.execute(
                    """select * from sa_article_xinlang_sysu where TID = %s""",
                    item['TID'])
                # 是否有重复数据
                repetition = self.cursor.fetchone()

                # 重复处理
                if repetition:
                    pass
                else:
                    self.cursor.execute("""
                                    insert into sa_article_xinlang_sysu(TID, DIGEST, TITLE, URL, CONTENT, PUBLISH_DATETIME, AUTHOR_NAME, ADD_DATETIME, CHANNEL_ID)
                                    values(%s, %s, %s, %s, %s, %s,%s, %s, %s)
                                """,
                                        (item['TID'], item['intro'], item['title_main'], item['href'],item['article'], item['time'], item['source'],self.now,3))
                    # 提交sql语句
                    self.connect.commit()
            except Exception as error:
                print(error)
                print('失败' + item["href"])
        print('成功保存数据库')

    def xhw_store(self, r):
        for item in r:
            try:
                # 查重处理
                self.cursor.execute(
                    """select * from sa_article_xinhua_sysu where TID = %s""",
                    item['TID'])
                # 是否有重复数据
                repetition = self.cursor.fetchone()
                # 重复处理
                if repetition:
                    pass
                else:
                    self.cursor.execute("""
                                    insert into sa_article_xinhua_sysu(TID,TITLE,PUBLISH_DATETIME,URL,DIGEST,AUTHOR_NAME,CONTENT, ADD_DATETIME, CHANNEL_ID)
                                    values(%s, %s, %s, %s, %s, %s,%s, %s, %s)
                                """,
                                        (item['TID'],item['title'],item['time'],item['href'],item['intro'],item['source'],item['article'],self.now,11))
                    # 提交sql语句
                    self.connect.commit()
            except Exception as error:
                print(error)
                print('失败' + item["href"])
        print('成功保存数据库')

    def tb_store(self, r):
        pass
    def rmw_store(self, r):
        for item in r:
            try:
                # 查重处理
                self.cursor.execute(
                    """select * from sa_article_renmin_sysu where TID = %s""",
                    item['TID'])
                # 是否有重复数据
                repetition = self.cursor.fetchone()
                # 重复处理
                if repetition:
                    pass
                else:
                    self.cursor.execute("""
                                    insert into sa_article_renmin_sysu(TID,TITLE,PUBLISH_DATETIME,URL,DIGEST,AUTHOR_NAME,CONTENT, ADD_DATETIME, CHANNEL_ID)
                                    values(%s, %s, %s, %s, %s, %s,%s, %s, %s)
                                """,
                                        (item['TID'],item['title'],item['time'],item['href'],item['intro'],item['source'],item['article'],self.now,11))
                    # 提交sql语句
                    self.connect.commit()
            except Exception as error:
                print(error)
                print('失败' + item["href"])
        print('成功保存数据库')


    def channelDict(self,channel_id):
        # sql = """select * from sa_channel where CHANNEL_ID =%s; """,(channel_id)

        self.cursor.execute("""select * from sa_channel where CHANNEL_ID =%s; """,(channel_id))

        desc = self.cursor.description  # 获取字段的描述，默认获取数据库字段名称，重新定义时通过AS关键重新命名即可
        data_dict = [dict(zip([col[0] for col in desc], row)) for row in self.cursor.fetchall()]  # 列表表达式把数据组装起来
        self.cursor.close()
        self.connect.close()
        return data_dict



class SQLCrawler(object):

    def __init__(self,logger=None):
        self.keywordList2 = list()   # 用来过滤不包含关键词的文章
        self.nonekyewordList = list()  # 用来过滤含有反关键词的文章
        self.dbProxy = MySqlProxy(MYSQL_HOST,
                                 MYSQL_PORT,
                                 MYSQL_USER,
                                 MYSQL_PASSWD,
                                 MYSQL_DBNAME)
        if logger is None:
            self.logger = Logging.getLogger(Logging.LOGGER_NAME_DEFAULT)
        else:
            self.logger = logger


    def updateToArticleTable(self, articleList, tableName, eventId=None, hasMetaInfo=False):
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
            eventIdFieldValue = str(eventId) + ','
        if hasMetaInfo:
            metaInfoFieldName = ',META_INFO'
            metaInfoOnUpdate = ',META_INFO=VALUES(META_INFO)'
        else:
            metaInfoFieldName = ''
            metaInfoOnUpdate = ''

        insertSql = '''
                INSERT INTO %s (TID, %s CHANNEL_ID, URL, ADD_DATETIME, PUBLISH_DATETIME, PUBLISH_METHOD,
                    TITLE, AUTHOR_ID, AUTHOR_NAME, DIGEST, CONTENT, READ_COUNT,LIKE_COUNT, REPLY_COUNT,
                    FORWARD_COUNT, COLLECT_COUNT, CLASSIFIED_NATURE, HEAT, UPDATE_DATETIME %s,PUBLISH_DATE)
                VALUES %s 
                ON DUPLICATE KEY UPDATE READ_COUNT=VALUES(READ_COUNT), LIKE_COUNT=VALUES(LIKE_COUNT), 
                REPLY_COUNT = VALUES(REPLY_COUNT), FORWARD_COUNT=VALUES(FORWARD_COUNT), 
                COLLECT_COUNT = VALUES(COLLECT_COUNT), HEAT = VALUES(HEAT), UPDATE_DATETIME=VALUES(UPDATE_DATETIME)
                , CLASSIFIED_NATURE=VALUES(CLASSIFIED_NATURE)
                %s
                '''
        for article in articleList:
            data = article.publish_datetime
            data = data.split(' ')[0]
            # data = datetime.strptime(data, '%Y-%m-%d  %H:%M:%S')
            # data = data.strftime('%Y-%m-%d')
            if article.content is None:
                article.content = '无'
            valueList = list()
            statistics = article.statistics
            if hasMetaInfo:
                metaInfoFieldValue = ',"' + encodeText(
                    article.meta_info) + '"' if article.meta_info is not None else ',' + Constants.DEFAULT_STR
            else:

                metaInfoFieldValue = ''
            valueList.append(
                '("%s", %s %d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s","%s", %s, %s, %s, %s, %s, %d, %s, "%s" %s,"%s")' % (
                    article.tid,
                    eventIdFieldValue,
                    article.channel_id,
                    article.url,
                    n,
                    article.publish_datetime if article.publish_datetime is not None else Constants.DEFAULT_PUBLISH_DATETIME,
                    article.publish_method if article.publish_method is not None else Constants.DEFAULT_PUBLISH_METHOD,
                    encodeText(article.title) if article.title is not None else '',
                    article.author_id if article.author_id is not None else Constants.DEFAULT_AUTHOR_ID,
                    article.author_name if article.author_name is not None else Constants.DEFAULT_AUTHOR_NAME,
                    encodeText(article.digest) if article.digest is not None else Constants.DEFAULT_DIGEST,
                    encodeText(article.content),
                    statistics.read_count if statistics.read_count is not None else Constants.DEFAULT_NUM,
                    statistics.like_count if statistics.like_count is not None else Constants.DEFAULT_NUM,
                    statistics.reply_count if statistics.reply_count is not None else Constants.DEFAULT_NUM,
                    statistics.forward_count if statistics.forward_count is not None else Constants.DEFAULT_NUM,
                    statistics.collect_count if statistics.collect_count is not None else Constants.DEFAULT_NUM,
                    article.classified_nature,
                    statistics.heat if statistics.heat is not None else Constants.DEFAULT_NUM,
                    n,
                    metaInfoFieldValue,
                    data if data is not None else Constants.DATE,
                ))
            if len(valueList) > 0:
                tmp = insertSql % (
                    tableName, eventIdFieldName, metaInfoFieldName, ','.join(valueList), metaInfoOnUpdate)
                # self.logger.debug(tmp)
                self.dbProxy.execute(tmp)
                self.dbProxy.commit()


    def updateToArticleHistoryTable(self, articleList, tableName, eventId=None):
        '''
        更新到文章历史表
        @param tableName: 当前文章表：全局文章表、实体文章表或者实体事件文章表
        @param eventId: 如果更新到实体事件文章表，则需要提供事件id，否则为None
        '''
        n = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        period = int(datetime.now().strftime('%Y%m%d%H'))
        valueList = list()
        if eventId is None:
            eventIdFieldName = ''
            eventIdFieldValue = ''
            insertSql = '''
                INSERT INTO %s (TID, %s CHANNEL_ID,
                    READ_COUNT,LIKE_COUNT, REPLY_COUNT,
                    FORWARD_COUNT, COLLECT_COUNT, HEAT, ADD_DATETIME)
                VALUES %s 
                ON DUPLICATE KEY UPDATE READ_COUNT = VALUES(READ_COUNT),LIKE_COUNT = VALUES(LIKE_COUNT),
                REPLY_COUNT = VALUES(REPLY_COUNT),FORWARD_COUNT = VALUES(FORWARD_COUNT),COLLECT_COUNT = VALUES(COLLECT_COUNT),
                HEAT = VALUES(HEAT), ADD_DATETIME = VALUES(ADD_DATETIME)
            '''
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
                    n,
                ))
        else:
            eventIdFieldName = 'EVENT_ID,'
            eventIdFieldValue = str(eventId)+','

            insertSql = '''
                INSERT INTO %s (TID, %s CHANNEL_ID,
                    READ_COUNT,LIKE_COUNT, REPLY_COUNT,
                    FORWARD_COUNT, COLLECT_COUNT, HEAT, ADD_DATETIME, PERIOD)
                VALUES %s 
                ON DUPLICATE KEY UPDATE READ_COUNT = VALUES(READ_COUNT),LIKE_COUNT = VALUES(LIKE_COUNT),
                REPLY_COUNT = VALUES(REPLY_COUNT),FORWARD_COUNT = VALUES(FORWARD_COUNT),COLLECT_COUNT = VALUES(COLLECT_COUNT),
                HEAT = VALUES(HEAT), ADD_DATETIME = VALUES(ADD_DATETIME), PERIOD = VALUES(PERIOD)
            '''
            for article in articleList:
                statistics = article.statistics
                valueList.append('("%s", %s %d, %s, %s, %s, %s, %s, %s, "%s", %d)' % (
                            article.tid,
                            eventIdFieldValue,
                            article.channel_id,
                            statistics.read_count if statistics.read_count is not None else Constants.DEFAULT_NUM,
                            statistics.like_count if statistics.like_count is not None else Constants.DEFAULT_NUM,
                            statistics.reply_count if statistics.reply_count is not None else Constants.DEFAULT_NUM,
                            statistics.forward_count if statistics.forward_count is not None else Constants.DEFAULT_NUM,
                            statistics.collect_count if statistics.collect_count is not None else Constants.DEFAULT_NUM,
                            statistics.heat if statistics.heat is not None else Constants.DEFAULT_NUM,
                            n,
                            period
                            ))
        if len(valueList) > 0:
            tmp = insertSql % (tableName, eventIdFieldName, ','.join(valueList))

            self.dbProxy.execute(tmp)
            self.dbProxy.commit()

    def identifyArticle(self, articleList, predefinedEntityId=None):
        '''
        标识文章所属实体以及使实体内的事件。一篇文章可以属于多个实体，也可以属于同一个实体的多个事件
        '''
        entityIA = EntityIdentificationAnalytics(self.dbProxy, '')
        allEntityIdList = GlobalVariable.getEntityMgmt().entityDict.keys()
        eventIADict = dict()
        for entityId in allEntityIdList:
            eventIADict[entityId] = EventIdentificationAnalytics(self.dbProxy, entityId)
        entityEventArticleDict = dict()
        for article in articleList:
            # 定位实体
            # entityIdList = entityIA.analysis(article)
            if predefinedEntityId is not None:
                entityIdList = [predefinedEntityId]
            else:
                entityIdList = entityIA.analysis(article)
            if len(entityIdList) > 0:
                for entityId in entityIdList:
                    if entityId not in entityEventArticleDict:
                        entityEventArticleDict[entityId] = {'articleList': list(), 'eventArticleDict': dict()}
                    entityEventArticleDict[entityId]['articleList'].append(article)
                    # 根据实体，定位事件
                    eventIdList = eventIADict[entityId].analysis(article)
                    self.logger.debug('Article %s belongs to entity(%s) and eventlist:%s',
                                      article.tid, entityId, eventIdList)
                    if len(eventIdList) > 0:
                        eventArticleDict = entityEventArticleDict[entityId]['eventArticleDict']
                        for eventId in eventIdList:
                            if eventId not in eventArticleDict:
                                eventArticleDict[eventId] = list()
                            eventArticleDict[eventId].append(article)
        # 更新数据库
        for entityId in entityEventArticleDict:
            filteredArticle = self.filterRemovedArticle(entityEventArticleDict[entityId]['articleList'], entityId)
            # filteredArticle = entityEventArticleDict[entityId]['articleList']
            filteredArticle = list(filteredArticle)
            if len(filteredArticle) > 0:
                # 拆分新旧文章，进行敏感词预警分析
                (existingArticleList, newArticleList) = self.seperateNewOldArticles(filteredArticle, entityId)
                newArticleList = list(newArticleList)
                if len(newArticleList) > 0:
                    self.logger.info('Sensitive words analysis for %d article for %s',
                                     len(newArticleList), entityId)
                    ana = ArticleWarningAnalytics(self.dbProxy, entityId)
                    ana.analysis(articleList=newArticleList, commit=False)

                self.updateToArticleTable(filteredArticle,
                                            Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + entityId)
                self.updateToArticleHistoryTable(filteredArticle,
                                                   Constants.TABLE_SA_ARTICLE_HISTORY + Constants.TABLE_NAME_DELIMITER + entityId)
            for eventId in entityEventArticleDict[entityId]['eventArticleDict']:

                # filteredEventArticle = entityEventArticleDict[entityId]['eventArticleDict'][eventId]
                filteredEventArticle = self.filterRemovedArticle(
                    entityEventArticleDict[entityId]['eventArticleDict'][eventId],
                    entityId, eventId)

                # 将符合某事件的文章插入到事件文章表

                if len(list(filteredEventArticle)) > 0:
                    self.updateToArticleTable(filteredEventArticle,
                                                Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + entityId,
                                                eventId)
                    self.updateToArticleHistoryTable(filteredEventArticle,
                                                       Constants.TABLE_SA_EVENT_ARTICLE_HISTORY + Constants.TABLE_NAME_DELIMITER + entityId,
                                                       eventId)

                    # 插入文章表后进行分析
                    back_track_analyse = SingleEventBackTrack(self.dbProxy, entityId, eventId)
                    back_track_analyse.analysis(False)

    def filterRemovedArticle(self, articleList, entityId, eventId=None):
        '''
        与remove表格对比，进行文章过滤
        返回不存在remove表中的文章list
        '''
        if len(articleList)==0:
            return []
        if eventId is not None:
            tableName = Constants.TABLE_SA_EVENT_ARTICLE_REMOVE + Constants.TABLE_NAME_DELIMITER + entityId
            eventCondition = ' event_id=%d and ' % eventId

            start_datetime, end_datetime = self.fetchEventTime(entityId, eventId)

            # 过滤掉不在该事件开始时间和结束之间内的文章
            article_new_list = list()
            for article in articleList:
                if (str(article.publish_datetime) > str(start_datetime)) and (str(article.publish_datetime) < str(end_datetime)):
                    article_new_list.append(article)

            articleList = article_new_list

        else:
            tableName = Constants.TABLE_SA_ARTICLE_REMOVE + Constants.TABLE_NAME_DELIMITER + entityId
            eventCondition = ''
        # 在remove表里查找文章
        selectSql = '''
            SELECT TID, CHANNEL_ID FROM %s where %s (%s)
        '''
        whereClauseList = map(lambda article: '(TID="%s" and CHANNEL_ID=%d)' % (article.tid, article.channel_id),
                              articleList)

        self.dbProxy.execute(selectSql % (tableName, eventCondition, ' or '.join(whereClauseList)))
        resultList = self.dbProxy.fetchall()  # 查询返回结果集
        removedArticleList = map(lambda x: Article(x[0], x[1]), resultList)
        filteredArticle = filter(lambda x: x not in removedArticleList, articleList)
        return filteredArticle

    def seperateNewOldArticles(self, articleList, entityId=None):
        '''
        查询全局文章表，区分新文章和旧文章
        '''
        if len(articleList)==0:
            return ([],[])
        if entityId is None:
            selectSql = 'select tid, channel_id from %s where ' % Constants.TABLE_SA_ARTICLE
        else:
            selectSql = 'select tid, channel_id from %s where ' % (Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + entityId)
        whereClauseList = map(lambda article: '(tid="%s" and channel_id=%d)'%( article.tid, article.channel_id), articleList)
        self.dbProxy.execute(selectSql + ' or '.join(whereClauseList))
        resultList = map(lambda x: Article(x[0], x[1]), self.dbProxy.fetchall())

        existingArticleList = filter(lambda x: x in resultList, articleList)
        newArticleList = filter(lambda x: x not in resultList, articleList)
        return (existingArticleList, newArticleList)

    def fetchEventTime(self, entity_id, event_id):
        '''
        获取事件回溯的起始时间和结束时间
        :param event:
        :return:
        '''
        selectSql = '''
            SELECT start_datetime, end_datetime from %s where event_id=%d 
        '''
        tableName = Constants.TABLE_SA_EVENT + Constants.TABLE_NAME_DELIMITER + entity_id

        sql = selectSql % (tableName, event_id)
        self.dbProxy.execute(sql)
        result = self.dbProxy.fetchall()

        return (result[0][0],
                result[0][1])

        #  对应获取旧文章来进行分析

    def fetchOldArticleList(self,channel, articleList, articleCount=100):
        '''
        从全局文章表，获取尚未消亡的文章id，而且这些文章并不在本次爬虫爬回来的记录里
        '''
        channel = int(channel)
        # 用来查询总页数
        selectSql_count = 'SELECT COUNT(*) FROM %s where extinct="N" and channel_id=%d '
        sql2 = selectSql_count % (Constants.TABLE_SA_ARTICLE,
                                  channel)
        # 获取旧文章的sql
        selectSql = 'SELECT TID,title, publish_datetime,url, meta_info,like_count,reply_count,forward_count FROM %s where extinct="N" and channel_id=%d '
        sql = selectSql % (Constants.TABLE_SA_ARTICLE,
                           channel)

        if len(articleList) > 0:
            whereClauseList = map(lambda article: ' tid<>"%s" ' % (article.tid), articleList)
            whereClauseList = ' and '.join(whereClauseList)
            sql += ' and (%s)' % (whereClauseList)
            sql2 += ' and (%s)' % (whereClauseList)
        sql2 += ' order by add_datetime desc;'
        self.dbProxy.execute(sql2)
        resultList2 = self.dbProxy.fetchall()
        # print '12456789sssssssssssssssssss'
        # print resultList2 #((53,),)
        resultList2 = re.findall(r'\d+', str(resultList2))  # 返回一个list
        # print resultList2[0]
        if int(resultList2[0]) > int(articleCount):
            randpage = random.randint(0, int(math.ceil(float(resultList2[0]) / articleCount)))
        else:
            randpage = 0  # 用来随机取数据库页数

        sql += ' order by add_datetime desc limit %d,%d' % (randpage, articleCount)
        self.dbProxy.execute(sql)
        resultList = self.dbProxy.fetchall()

        L1 = []
        for item in resultList:
            result = Article(item[0], channel,
                                        title=item[1], publish_datetime=item[2], url=item[3], meta_info=item[4])
            result.statistics = ArticleStatistics(item[0], channel,
                                                  like_count=item[5], reply_count=item[6], forward_count=item[7])
            L1.append(result)

        return L1
        # return map(lambda item: Article(item[0], channel,
        #                                 title=item[1], publish_datetime=item[2], url=item[3], meta_info=item[4]
        #                                 ),
        #               resultList)

        # 将爬取的新文章加入文章表
    def addArticle(self, articleList, tableName, eventId=None, hasMetaInfo=False):
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
            eventIdFieldValue = str(eventId) + ','
        if hasMetaInfo:
            metaInfoFieldName = ',META_INFO'
            metaInfoOnUpdate = ',META_INFO=VALUES(META_INFO)'
        else:
            metaInfoFieldName = ''
            metaInfoOnUpdate = ''

        insertSql = '''
                INSERT INTO %s (TID, %s CHANNEL_ID, URL, ADD_DATETIME, PUBLISH_DATETIME, PUBLISH_METHOD,
                    TITLE, AUTHOR_ID, AUTHOR_NAME, DIGEST, CONTENT, READ_COUNT,LIKE_COUNT, REPLY_COUNT,
                    FORWARD_COUNT, COLLECT_COUNT, CLASSIFIED_NATURE, CHECKED, HEAT, UPDATE_DATETIME %s)
                VALUES %s 
                ON DUPLICATE KEY UPDATE READ_COUNT=VALUES(READ_COUNT), LIKE_COUNT=VALUES(LIKE_COUNT), 
                REPLY_COUNT = VALUES(REPLY_COUNT), FORWARD_COUNT=VALUES(FORWARD_COUNT), 
                COLLECT_COUNT = VALUES(COLLECT_COUNT), HEAT = VALUES(HEAT), UPDATE_DATETIME=VALUES(UPDATE_DATETIME)
                , CLASSIFIED_NATURE=VALUES(CLASSIFIED_NATURE)
                %s
                '''
        for article in articleList:
            if article.content is None:
                article.content = '无'
            valueList = list()
            statistics = article.statistics
            if hasMetaInfo:
                metaInfoFieldValue = ',"' + encodeText(
                    article.meta_info) + '"' if article.meta_info is not None else ',' + Constants.DEFAULT_STR
            else:
                metaInfoFieldValue = ''
            valueList.append(
                '("%s", %s %d, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s","%s", %s, %s, %s, %s, %s, %d, "%s", %s, "%s" %s)' % (
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
                    article.classified_nature,
                    article.checked,
                    statistics.heat if statistics.heat is not None else Constants.DEFAULT_NUM,
                    n,
                    metaInfoFieldValue
                ))
            if len(valueList) > 0:
                tmp = insertSql % (
                    tableName, eventIdFieldName, metaInfoFieldName, ','.join(valueList), metaInfoOnUpdate)
                # self.logger.debug(tmp)
                self.dbProxy.execute(tmp)
                self.dbProxy.commit()

    def updateOldArticleToArticleTable(self, articleList, tableName, isEventTable=False):
        '''
        更新旧文章到文章表
        @param tableName: 全局文章表、实体文章表或者实体事件文章表
        @param eventId: 如果更新到实体事件文章表，则需要提供事件id，否则为None
        '''
        articleList = list(articleList)
        if len(articleList) > 0:
            if isEventTable is False:
                eventIdFieldName = ''
            else:
                eventIdFieldName = ',EVENT_ID'
            #找寻老文章
            selectSql = '''
            SELECT TID, CHANNEL_ID %s FROM %s where %s
            '''
            whereClauseList = map(lambda article: '(TID="%s" and CHANNEL_ID=%d)'%(article.tid, article.channel_id),
                                  articleList)
            self.dbProxy.execute(selectSql % (eventIdFieldName, tableName, ' or '.join(whereClauseList)))
            resultList = self.dbProxy.fetchall()
            if isEventTable:
                existingArticleList = map(lambda item: Article(item[0], item[1], eventId=item[2]), resultList)
            else:
                existingArticleList = map(lambda item: Article(item[0], item[1]), resultList)
            toBeUpdateArticleList = list()
            for item in existingArticleList:
                index = articleList.index(item) # 返回查找对象的索引位置
                obj = copy.deepcopy(articleList[index])
                obj.eventId = item.eventId
                toBeUpdateArticleList.append(obj)
            if len(toBeUpdateArticleList) > 0:
                n = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if isEventTable is False:
                    eventIdFieldName = ''
                else:
                    eventIdFieldName = 'EVENT_ID,'
                insertSql = '''
                INSERT INTO %s (TID, %s CHANNEL_ID,
                    READ_COUNT,LIKE_COUNT, REPLY_COUNT,
                    FORWARD_COUNT, COLLECT_COUNT, HEAT, UPDATE_DATETIME,PUBLISH_DATE)
                VALUES %s 
                ON DUPLICATE KEY UPDATE READ_COUNT=VALUES(READ_COUNT), LIKE_COUNT=VALUES(LIKE_COUNT), 
                REPLY_COUNT = VALUES(REPLY_COUNT), FORWARD_COUNT=VALUES(FORWARD_COUNT), 
                COLLECT_COUNT = VALUES(COLLECT_COUNT), HEAT = VALUES(HEAT), UPDATE_DATETIME=VALUES(UPDATE_DATETIME)
                '''
                valueList = list()
                for article in toBeUpdateArticleList:
                    statistics = article.statistics
                    data = article.publish_datetime
                    data = data.strftime('%Y-%m-%d')
                    if isEventTable is False:
                        eventIdFieldValue = ''
                    else:
                        eventIdFieldValue = str(article.eventId)+','
                    valueList.append('("%s", %s %d, %s, %s, %s, %s, %s, %s, "%s","%s")' % (
                                article.tid,
                                eventIdFieldValue,
                                article.channel_id,
                                statistics.read_count if statistics.read_count is not None else Constants.DEFAULT_NUM,
                                statistics.like_count if statistics.like_count is not None else Constants.DEFAULT_NUM,
                                statistics.reply_count if statistics.reply_count is not None else Constants.DEFAULT_NUM,
                                statistics.forward_count if statistics.forward_count is not None else Constants.DEFAULT_NUM,
                                statistics.collect_count if statistics.collect_count is not None else Constants.DEFAULT_NUM,
                                statistics.heat if statistics.heat is not None else Constants.DEFAULT_NUM,
                                n,
                                data if data is not None else Constants.DATE,

                                ))
                if len(valueList)>0:
                    sql = insertSql % (tableName, eventIdFieldName, ','.join(valueList))
                    self.dbProxy.execute(sql)
                    self.dbProxy.commit()

    def updateOldArticleToArticleHistoryTable(self, articleList, currentTableName, historyTableName, isEventTable=False):
        '''
        更新到文章历史表
        @param currentTableName: 当前文章表：全局文章表、实体文章表或者实体事件文章表
        @param historyTableName: 历史文章表：全局文章表、实体文章表或者实体事件文章表
        @param eventId: 如果更新到实体事件文章表，则需要提供事件id，否则为None
        '''
        articleList = list(articleList)
        if len(articleList) > 0:
            if isEventTable is False:
                eventIdFieldName = ''
            else:
                eventIdFieldName = ',EVENT_ID'
            #找寻老文章
            selectSql = '''
            SELECT TID, CHANNEL_ID %s FROM %s where %s
            '''
            whereClauseList = map(lambda article: '(TID="%s" and CHANNEL_ID=%d)'%(article.tid, article.channel_id),
                                  articleList)
            self.dbProxy.execute(selectSql % (eventIdFieldName, currentTableName, ' or '.join(whereClauseList)))
            resultList = self.dbProxy.fetchall()
            if isEventTable:
                existingArticleList = map(lambda item: Article(item[0], item[1], eventId=item[2]), resultList)
            else:
                existingArticleList = map(lambda item: Article(item[0], item[1]), resultList)
            toBeUpdateArticleList = list()
            for item in existingArticleList:
                index = articleList.index(item)
                obj = copy.copy(articleList[index])
                obj.eventId = item.eventId
                toBeUpdateArticleList.append(obj)
            if len(toBeUpdateArticleList) > 0:
                n = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if isEventTable is False:
                    eventIdFieldName = ''
                else:
                    eventIdFieldName = 'EVENT_ID,'
                insertSql = '''
                INSERT INTO %s (TID, %s CHANNEL_ID,
                    READ_COUNT,LIKE_COUNT, REPLY_COUNT,
                    FORWARD_COUNT, COLLECT_COUNT, HEAT, ADD_DATETIME)
                VALUES %s 
                '''
                valueList = list()
                for article in toBeUpdateArticleList:
                    statistics = article.statistics
                    if isEventTable is False:
                        eventIdFieldValue = ''
                    else:
                        eventIdFieldValue = str(article.eventId)+','
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
                                n,
                                ))
                if len(valueList)>0:
                    self.dbProxy.execute(insertSql % (historyTableName, eventIdFieldName, ','.join(valueList)))
                    self.dbProxy.commit()

    def updateToCommentTable(self,channel ,commentList):
        insertSql = '''
        INSERT INTO %s (TID, CHANNEL_ID, CID,ADD_DATETIME, PUBLISH_DATETIME,
                        IP_ADDRESS, LOCATION_COUNTRY, LOCATION_REGION, LOCATION_CITY, 
                        AUTHOR_ID, AUTHOR_NAME, CONTENT, REPLY_AUTHOR_ID, 
                        READ_COUNT, LIKE_COUNT, REPLY_COUNT, DISLIKE_COUNT) VALUES %s
        ON DUPLICATE KEY UPDATE READ_COUNT=VALUES(READ_COUNT), LIKE_COUNT=VALUES(LIKE_COUNT),
                        REPLY_COUNT=VALUES(REPLY_COUNT), DISLIKE_COUNT=VALUES(DISLIKE_COUNT)
        '''
        valueList = list()
        for comment in commentList:
            valueList.append('("%s", %d, "%s","%s", "%s","%s", "%s", "%s", "%s","%s", "%s", "%s", "%s", %s, %s, %s, %s)\n' % (
                                    comment.tid,
                                    channel,
                                    comment.cid,
                                    comment.add_datetime,
                                    comment.publish_datetime,
                                    comment.ip_address,
                                    comment.location_country,
                                    comment.location_region,
                                    comment.location_city,
                                    comment.author_id,
                                    encodeText(comment.author_name),
                                    encodeText(comment.content),
                                    comment.reply_author_id,
                                    comment.read_count if comment.read_count is not None else Constants.DEFAULT_NUM,
                                    comment.like_count if comment.like_count is not None else Constants.DEFAULT_NUM,
                                    comment.reply_count if comment.reply_count is not None else Constants.DEFAULT_NUM,
                                    comment.dislike_count if comment.dislike_count is not None else Constants.DEFAULT_NUM
                            ))
        if len(valueList) > 0:
            self.dbProxy.execute(insertSql % (Constants.TABLE_SA_COMMENT, ','.join(valueList)))
            self.dbProxy.commit()


    def updateToUSERTable(self,userinfolist,):
        n = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tableName = 'sa_weibo_user_info'
        insertSql = '''
                        INSERT INTO %s (TID, CHANNEL_ID, URL, ADD_DATETIME, FANS_COUNT, LOGO_URL, CONCERN_COUNT,USER_NAME,CONTENT)
                        VALUES %s 
                        ON DUPLICATE KEY UPDATE ADD_DATETIME=VALUES(ADD_DATETIME), FANS_COUNT=VALUES(FANS_COUNT), 
                        LOGO_URL = VALUES(LOGO_URL),  CONCERN_COUNT=VALUES(CONCERN_COUNT),USER_NAME=VALUES(USER_NAME)
                        '''

        valueList = list()
        for userinfo in userinfolist:
            valueList.append(
                '("%s",%s,"%s","%s",%s,"%s",%s,"%s","%s")' % (
                    userinfo.tid,
                    userinfo.channel_id,
                    userinfo.url,
                    n,
                    userinfo.fans_count,
                    userinfo.logo_url if userinfo.logo_url is not None else Constants.DEFAULT_NUM,
                    userinfo.Concern_count if userinfo.Concern_count is not None else Constants.DEFAULT_NUM,
                    userinfo.user_name if userinfo.Concern_count is not None else Constants.DEFAULT_AUTHOR_NAME,
                    userinfo.content if userinfo.content is not None else Constants.DEFAULT_DIGEST,
                ))
        if len(valueList) > 0:
            tmp = insertSql % (
                tableName , ','.join(valueList))
            self.dbProxy.execute(tmp)
            self.dbProxy.commit()


