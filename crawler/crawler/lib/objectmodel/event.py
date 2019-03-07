# -*- coding:utf-8 -*-
'''
Created on 3 Oct 2017

@author: eyaomai
'''
import datetime
from crawler.lib.common.constant import Constants


class EventManager(object):
    def __init__(self, dbProxy, entity_id_list):
        self.dbProxy = dbProxy
        self.entityEventDict = dict()  # 2018/5/31 实体事件字典 对应所有事件
        self.backTrackEventDict = dict()  # 2018/6/12 回溯事件字典
        for entity_id in entity_id_list:
            self.fetchValidEventList(entity_id)

    def fetchValidEventList(self, entity_id, refresh=False):
        if refresh is True or entity_id not in self.entityEventDict:
            tableName = Constants.TABLE_SA_EVENT+Constants.TABLE_NAME_DELIMITER+entity_id
            currentDateStr = datetime.datetime.now().strftime('%Y-%m-%d')

            # 当前时间加15分钟
            currentAdd5 = (datetime.datetime.now() + datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")

            # 当前时间减15分钟
            currentReduce5 = (datetime.datetime.now() - datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")

            sql = '''
                  select event_id, event_name, algorithm, algorithm_conf, enable, 
                    add_datetime, extinct_conf, extinct, update_date, external_crawlable, 
                    external_keyword, event_type, back_track, min_article_numbers,
                    start_datetime
            from %s
            where enable='Y' and extinct='N' and del_flag='0' and back_track='Y' and
            (end_datetime is null or (end_datetime is not null and end_datetime>="%s"))
            ''' % (tableName, currentDateStr)
            self.dbProxy.execute(sql)
            results = self.dbProxy.fetchall()
            eventDict = dict()
            backTrackEventList = []

            for item in results:
                if item[12] == 'Y':
                    if (str(item[8]) > currentReduce5) and (str(item[8]) < currentAdd5):
                        backTrackEventList.append(item[0])
                if item[9] == 'N':  # external_crawler
                    external_crawlable = False
                    external_keyword = None
                else:
                    external_crawlable = True
                    external_keyword = item[10]
                event = Event(event_id=item[0], event_name=item[1], algorithm=item[2], algorithm_conf=item[3],
                              enable=True, add_datetime=item[5], extinct_conf=item[6], extinct=False, update_date=item[8],
                              external_crawlable=external_crawlable, external_keyword=external_keyword,
                              min_article_numbers=item[13], event_type=item[11], back_track=item[12],
                              start_datetime=item[14]
                              )
                eventDict[event.event_id] = event

            self.backTrackEventDict[entity_id] = backTrackEventList
            # entityEventDict={hpc:{event_id:event类}，sysu：{event_id:event类}，...}
            self.entityEventDict[entity_id] = eventDict
            
    
class Event(object):
    '''
    classdocs
    '''
    def __init__(self, event_id, event_name, algorithm, algorithm_conf, enable, add_datetime, 
                 extinct_conf, extinct, update_date, external_crawlable=False, external_keyword=None,
                 min_article_numbers=None, start_datetime=None, event_type='1', back_track="Y"):
        '''
        Constructor
        '''
        self.event_id = event_id
        self.event_name = event_name
        self.algorithm = algorithm
        self.algorithm_conf = algorithm_conf
        self.enable = enable
        self.add_datetime = add_datetime
        self.extinct_conf = extinct_conf
        self.extinct = extinct
        self.update_date = update_date
        self.external_crawlable = external_crawlable
        self.external_keyword = external_keyword
        self.start_datetime = start_datetime
        self.event_type = event_type
        self.min_article_numbers = min_article_numbers
        self.back_track = back_track  # 6.11 yao 增加回溯判断