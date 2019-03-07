'''
Created on 2 Oct 2017

@author: eyaomai
'''

class EntityManager(object):
    '''
    classdocs
    '''


    def __init__(self, dbProxy):
        '''
        Constructor
        '''
        self.dbProxy = dbProxy
        self.entityDict = dict()
        self.__fetchEnableEntities()

    def __fetchEnableEntities(self):
        sql = 'select entity_id, entity_name, enable, keyword_list, internal_keyword_list,internal_none_keyword_list from sa_entity where enable="Y"'
        self.dbProxy.execute(sql)
        results = self.dbProxy.fetchall()
        for item in results:            
            entity = Entity(item[0], item[1], True, item[3], item[4],item[5])
            self.entityDict[entity.entity_id] = entity

class Entity(object):
    def __init__(self, entity_id, entity_name, enable, keyword_list, internal_keyword_list=None,internal_none_keyword_list=None):
        self.entity_id = entity_id
        self.entity_name = entity_name
        self.enable = enable
        self.keyword_list = keyword_list
        self.internal_keyword_list = internal_keyword_list
        self.internal_none_keyword_list = internal_none_keyword_list