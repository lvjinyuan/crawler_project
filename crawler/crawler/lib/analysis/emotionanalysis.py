# -*- coding:utf-8 -*-
'''
Created on 20 March 2018

@author: KEYS
'''
import queue
# from gevent.queue import Queue

from crawler.lib.analysis.analysisbase import AnalyticsBase, Tools
from crawler.lib.common.constant import Constants
from crawler.lib.objectmodel.article import Article

from aip import AipNlp
from datetime import datetime, timedelta


class InitQueue(object):
    def __init__(self):
        self.app_list = [
            {'APP_ID': '14197851', 'API_KEY': '1ZDqwQCSoSuhWatQ5drh4qtQ', 'SECRET_KEY': 'hTKbk7pZkXE41yaN3ptaiYmqnD3v4NPp'},
            {'APP_ID': '10825478', 'API_KEY': 'hZx1dqbg7YzsKl9ENTIoXQYX', 'SECRET_KEY': 'GaWLHArH7TGaaMEGWSM7if7u9GnbF0IA'},
            {'APP_ID': '14200573', 'API_KEY': 'm4BUPNG9UIsvYdzUdOAGtGiI', 'SECRET_KEY': 'KNgrwvLfxehenjEYWkhMHWg26X8bewxb'},
            {'APP_ID': '14200672', 'API_KEY': 'hRGY7m3Za4nCDN4ROWmwoQnX', 'SECRET_KEY': 'clNMSWjAy6wwqnxeP9OrtHgIAdnbanbD'}
        ]
        self.que = queue.Queue()
        for app in self.app_list:
            self.que.put(app)

    def get_que(self):
        return self.que


class EmotionAnalysis(AnalyticsBase):
    '''
    文章情感分析
    '''
    def __init__(self, dbProxy, entity_id='', logger=None):
        '''
        Constructor
        '''
        super(EmotionAnalysis, self).__init__(dbProxy, entity_id)
        app = InitQueue()
        self.app_que = app.get_que()
        app_obj = self.app_que.get()
        self.client = AipNlp(app_obj['APP_ID'], app_obj['API_KEY'], app_obj['SECRET_KEY'])

    def analysisSingleArticle(self, content):
        if content is None:
            return -2
        else:
            content = content.encode('utf-8', 'ignore').decode('gbk', 'ignore')
            try:
                emotion_mes = self.client.sentimentClassify(content)
            except ValueError as e:
                if not self.app_que.empty():
                    app_obj = self.app_que.get()
                    self.client = AipNlp(app_obj['APP_ID'], app_obj['API_KEY'], app_obj['SECRET_KEY'])
                    emotion_mes = self.client.sentimentClassify(content)
                else:
                    self.logger.debug('Emotion analyse field because of %s' % e)
                    return None

            itemstr = 'items'
            emotion = -2
            if itemstr not in emotion_mes:
                if emotion_mes['error_msg'] == 'input text too long':
                    index = len(content) // 1011
                    emotion = 0
                    for i in range(0, index):
                        temp = content[i * 1011:(i + 1) * 1011 - 1]
                        try:
                            emotion_mes2 = self.client.sentimentClassify(temp)
                            emotion += emotion_mes2['items'][0]['sentiment']
                        except:
                            self.logger.debug('参数非法')
                            emotion += 0
                    emotion /= index
                    return emotion - 1
            else:
                emotion = emotion_mes['items'][0]['sentiment']
                return emotion - 1

            return emotion



    def analysisTimeArticle(self, entity_id, start_time, end_time):
        self.__getStartAndEndTime(start_time, end_time)
        artilceList = self.__fetchAllArticleList(entity_id, start_time, end_time)
        self.__analysisArticleList(artilceList)

    def __analysisArticleList(self, artilceList):
        valueList = list()
        for article in artilceList:
            article.classified_nature = self.client.sentimentClassify(article.content)['sentiment'] - 1
            valueList.append('("%s")')
    
    def analysisAllArticle(self):
        tableName = ''
        if self.entity_id is None:
            tableName += Constants.TABLE_SA_ARTICLE
        else:
            tableName += Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + self.entity_id
        articleList = self.__fetchArticleList(tableName)
        for i in range(len(articleList) / 100):
            tempList = articleList[i*100:(i+1)*100]
            self.updateInArticleTable(tempList, tableName)
            self.dbProxy.commit()

    def __fetchArticleList(self, tableName):
        '''
        获取数据库中某段时间添加的文章
        '''
        selectSql = '''select tid, channel_id, title, content 
                        from %s
                        where CLASSIFIED_NATURE=-2 or CLASSIFIED_NATURE=-3
        '''
        sql = selectSql % (tableName)
        self.dbProxy.execute(sql)
        results = self.dbProxy.fetchall()
        return results

    def updateInArticleTable(self, articleList, tableName):
        emotion_result = []
        for item in articleList:
            emotion = self.analysisSingleArticle(item[3])
            emotion_result.append(emotion)
        insertSql = '''
        INSERT INTO %s (TID, CHANNEL_ID, CLASSIFIED_NATURE)
        VALUES %s 
        ON DUPLICATE KEY UPDATE CLASSIFIED_NATURE=VALUES(CLASSIFIED_NATURE)
        '''
        valueList = list()
        print(articleList[0][0])
        for i in range(len(emotion_result)):
            valueList.append('("%s", "%s", %d)' %
                             (articleList[i][0], articleList[i][1], emotion_result[i]))
        print(valueList)
        if len(valueList) > 0:
            tmp = insertSql % (tableName, ','.join(valueList))
            # self.logger.debug(tmp)
            self.dbProxy.execute(tmp)

    def __fetchAllArticleList(self, entity_id, start_time, end_time):
        '''
        获取数据库中某段时间添加的文章
        '''
        selectSql = '''select * 
                        from %s
                        where add_datetime between '%s' and '%s'
        '''
        tableName = Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + entity_id
        sql = selectSql % (tableName, start_time, end_time)
        self.dbProxy.execute(sql)
        results = self.dbProxy.fetchall()
        self.logger.debug(len(results))
        articleList = list()
        for item in results:
            article = Article(tid=item[0], url=item[1], add_datetime=item[2], publish_datetime=item[3], 
            publish_method=item[4], title=item[7], author_id=item[8], author_name=item[9], content=item[11], 
            heat=item[17], channel_id=item[18], entity=entity_id)
            article.statistics.read_count = item[12]
            article.statistics.like_count = item[13]
            article.statistics.reply_count = item[14]
            article.statistics.forward_count = item[15]
            article.statistics.collect_count = item[16]
            articleList.append(article)
        return articleList

    def __getStartAndEndTime(self, start_time, end_time):
        now = datetime.now()
        if start_time is None:
            self.start_time = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        if end_time is None:
            self.end_time = now.strftime("%Y-%m-%d")


if __name__ == '__main__':
    app = InitQueue()
    app_que = app.get_que()
    print(app_que.get())
