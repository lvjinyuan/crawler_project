# -*- coding:utf-8 -*-
'''
Created on 1 Oct 2017

@author: eyaomai
'''
from crawler.lib.objectmodel.entity import EntityManager
from crawler.lib.objectmodel.event import EventManager
from crawler.lib.common.conf import SAConfiguration
from crawler.lib.objectmodel.channel import ChannelManager
class GlobalVariable(object):
    
    dbProxy = None
    entityMgmt = None
    saConfDict = None
    evtMgmt = None
    channelMgmt = None
    @staticmethod
    def init(dbProxy):
        GlobalVariable.dbProxy = dbProxy
        GlobalVariable.getEntityMgmt()
        GlobalVariable.getChannelMgmt()
        GlobalVariable.getEventMgmt()
        GlobalVariable.getSAConfDict()
    @staticmethod
    def getEntityMgmt():
        if GlobalVariable.entityMgmt is None:
            GlobalVariable.entityMgmt = EntityManager(GlobalVariable.dbProxy) #2018.6.1 实体字典表 2018.6.11 yao 备注 {'hpc':entity类, 'susy':entity类, 'scnu': entity类}
        
        return GlobalVariable.entityMgmt    #2018.5.31 从sa_entity获取配置表获取，实体字典列表
    @staticmethod
    def getEventMgmt():
        if GlobalVariable.evtMgmt is None:
            GlobalVariable.evtMgmt = EventManager(GlobalVariable.dbProxy,GlobalVariable.getEntityMgmt().entityDict.keys())
        return GlobalVariable.evtMgmt  #2018.6.1 返回事件字典 entityEventDict={hpc:{event_id:event类}，sysu：{event_id:event类}，...}
            
    @staticmethod
    def getSAConfDict():
        if GlobalVariable.saConfDict is None:
            GlobalVariable.saConfDict = dict()
            em = GlobalVariable.getEntityMgmt()
            entityDict = em.entityDict
            for entity_id in entityDict:
                GlobalVariable.saConfDict[entity_id] = SAConfiguration(GlobalVariable.dbProxy, entity_id) #2018.5.31 返回的是实体配置字典
            GlobalVariable.saConfDict[''] = SAConfiguration(GlobalVariable.dbProxy, '')
        return GlobalVariable.saConfDict #2018.5.31 返回系统配置字典
    
    @staticmethod
    def getChannelMgmt():
        if GlobalVariable.channelMgmt is None:
            GlobalVariable.channelMgmt = ChannelManager(GlobalVariable.dbProxy)
        return GlobalVariable.channelMgmt