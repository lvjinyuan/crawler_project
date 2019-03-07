'''
Created on Apr 7, stocklens01

@author: eyaomai
'''
import sys
import logging
import logging.config
import traceback



class Logging(object):

    LOGGER = logging.getLogger()
    LOGGER_NAME_TRADE = 'trade'
    LOGGER_NAME_MDS = 'mds'
    LOGGER_NAME_DEFAULT = 'default'
    LOGGER_NAME_UT = 'ut'
    LOGGER_NAME_DT = 'dt'
    LOGGER_NAME_RMT = 'rmt'
    LOGGER_NAME_CRAWL = 'crawl'
    LOGGER_NAME_ITRADE = 'itrade'
    LOGGER_NAME_ATRADE = 'atrade'

    @staticmethod
    def initLogger(filename):
        logging.config.fileConfig(filename)
        Logging.LOGGER = Logging.getLogger(Logging.LOGGER_NAME_DEFAULT)
    @staticmethod
    def getLogger(name):
        return logging.getLogger(name)

class Configuration(object):
    '''
    Configuration helper to read config in json format dict
    Pass the json_file when instantiate it and then invoke readConfig to get a dict with configuration
    '''
    def __init__(self, json_file):
        self.__json_file = json_file

    def readConfig(self):        
        with open(self.__json_file) as f:
            contentList = map(lambda x: x.replace("\r","").replace("\t","").replace("\n", "").strip(), list(f))
            contentList = filter(lambda x: not x.startswith('#'), contentList)
            json_string = ''.join(contentList)
            import json
            return json.loads(json_string)

import threading
import time    
class StoppableThread(threading.Thread):
    def __init__(self, defaultGran):
        super(StoppableThread, self).__init__()
        self.__shutDown = False
        self.__defaultGran = defaultGran
        self.setDaemon(True)
    
    def shutDown(self):
        self.__shutDown = True
    
    def looseSleep(self, interval):
        '''
        LooseSleep is to sleep with checking whether shutDown
        '''
        startTime = time.time()
        while (True):
            if time.time() - startTime >= interval:
                break
            if self.__shutDown is True:
                break
            time.sleep(self.__defaultGran)
            
    def isShutDown(self):
        return self.__shutDown

class OneTimeThread(threading.Thread):
    def __init__(self, func, paraDict, timeoutValue=None):
        self.__func = func
        self.__paraDict = paraDict
        self.__timeoutValue = timeoutValue
        super(OneTimeThread, self).__init__()
        self.setDaemon(True)

    def run(self):
        if self.__timeoutValue is not None:
            time.sleep(self.__timeoutValue)
        self.__func(**self.__paraDict)
    
import queue
import operator
class RequestHandler(threading.Thread):
    '''
    Base class for request handling. The class that subclass to this is supposed to overwrite following functions:
    -- _handlRequest(self,request): request handling function
    -- _shutDown(self): shutdown to ensure all resource are released
    
    When initiate the instance, you are supposed to pass in a time value to define how fast the request can be responded
    the value defaults to 0.1 second.
    '''
    def __init__(self, sleep_time=0.1, priorityNum=1, logger=None):
        super(RequestHandler, self).__init__()
        self.shutdownFlag = False
        self.__event = threading.Event()
        self.__requestQueueList = list()
        if priorityNum<=0:
            priorityNum = 1
        for i in range(0, priorityNum):
            self.__requestQueueList.append(queue.Queue())

        self.__sleep_time = sleep_time
        if logger is None:
            self.logger = Logging.LOGGER
        else:
            self.logger = logger
        #self.setDaemon(True)

    def __stopHandler(self):
        self.shutdownFlag = True
        self.__event.set()

    def addRequest(self, request, priority=0):
        if priority >= len(self.__requestQueueList):
            priority = len(self.__requestQueueList) - 1
        self.__requestQueueList[priority].put(request)
        self.__event.set()
    
    def getQueueSize(self):
        return sum([q.qsize() for q in self.__requestQueueList])
    def _isEmpty(self):
        for queue in self.__requestQueueList:
            if not queue.empty():
                return False
        return True

    def _handleRequest(self, request):
        '''
        _handleRequest is protected function to be inherited by subclass
        The default implementation only handles shutdown action
        The subclass can firstly invoke its super, determine whether to go or not by
        the return value: true means the request has been handled by super, false means
        super not yet handle.
        '''
        if not 'action' in request:            
            return False
        action = request['action']

        # if cmp(action, 'shutdown')==0:
        if operator.eq(action, 'shutdown') == 0:
            self._shutDown()
            return True
        return False
        
    def _shutDown(self):
        self.__stopHandler()
        
    def shutDown(self, force=False):
        if  force is True:
            self._shutDown()
            return
            
        request = dict()
        request['action']='shutdown'
        self.addRequest(request)

    def readConfig(self, json_config_file):
        c = Configuration(json_config_file)
        return c.readConfig()

    def run(self):
        while True:
            self.__event.wait()
            if self.shutdownFlag is True:
                return
            
            self.__event.clear()
            while self._isEmpty() is False:
                request = None
                currentQueue = None
                for queue in self.__requestQueueList:
                    try:
                        request = queue.get_nowait()
                        currentQueue = queue
                        break
                    except queue.Empty:
                        continue
                if request is None:
                    continue
                try:
                    self._handleRequest(request)
                except Exception:
                    traceInfo = traceback.format_exc()
                    self.logger.error('Fail to handle request: %s',traceInfo)
                currentQueue.task_done()
            if self.__sleep_time>0:
                time.sleep(self.__sleep_time)

class RPCServer(threading.Thread):
    def __init__(self, serverIp, serverPort, rcpFuncInstance, logger=None):
        super(RPCServer, self).__init__()
        self.__serverIp = serverIp
        self.__serverPort = serverPort
        self.__rcpFuncInstance = rcpFuncInstance
        self.__server = None
        self.setName('http://%s:%d' %(self.__serverIp, self.__serverPort))
        if logger is None:
            self.logger = Logging.getLogger(Logging.LOGGER_NAME_DEFAULT)
        else:
            self.logger = logger
        self.setDaemon(True)
        
    def shutDownServer(self):
        self.__server.shutdown()
        self.__server.server_close()
        
    def run(self):
        from xmlrpc.server import SimpleXMLRPCServer
        self.__server = SimpleXMLRPCServer((self.__serverIp, self.__serverPort), logRequests=False)
        self.__server.register_instance(self.__rcpFuncInstance)
        self.logger.info('RPC server started @ %s:%d', self.__serverIp, self.__serverPort)
        self.__server.serve_forever()

class RPCHandler(RequestHandler):
    def __init__(self, serverIp, serverPort, serviceHandler=None, rpcFuncInstance=None, sleep_time=0.1, logger=None):
        super(RPCHandler, self).__init__(sleep_time)
        self.__serverIp = serverIp
        self.__serverPort = serverPort
        
        #service handler is the handler instance to actually handle the request
        #it is the application level
        self._serviceHandler = serviceHandler
        
        #rpcFuncInstance is the rpc class instance to route income request
        if rpcFuncInstance is None: 
            self.__rpcFuncInstance = RPCFuncBase(self)
        else:
            self.__rpcFuncInstance = rpcFuncInstance
        
        #rpcClient is the rpc client to send request out
        self.rpcClient = None
        self.identifier = 'http://%s:%d' % (serverIp, serverPort)
        if logger is not None:
            self.logger = logger

    def setRPCClient(self, rpcClient):
        self.rpcClient = rpcClient

    def setRpcFuncInstance(self, rpcFuncInstance):
        self.__rpcFuncInstance = rpcFuncInstance
    
    def setServiceHandler(self, serviceHandler):
        self._serviceHandler = serviceHandler

    def sendServiceRequest(self, funcName, paraList, credential):
        if self.rpcClient is None:
            self.logger.error('rpc client is not set')
            return
        self.rpcClient.service(funcName, paraList, credential)
                        
    def start(self):
        super(RPCHandler, self).start()
        self.__startRPCServer()
        
    def __startRPCServer(self):    
        self.__rpcServer = RPCServer(self.__serverIp, self.__serverPort, self.__rpcFuncInstance, self.logger)
        self.__rpcServer.start()

    def _shutDown(self):
        '''
        Shutdown the agent, including all its threads and tasks
        '''
        #stop RPCServer
        if self.__rpcServer is not None:
            self.logger.info('Shutting down the RPC server')
            self.__rpcServer.shutDownServer()

        #stop jobs
        self._stopTasks()
        
        #stop the thread itself
        super(RPCHandler, self)._shutDown()
    
        
    def _handleRequest(self, request):
        if not 'action' in request:
            self.logger.error('Invalid request that does not contain action element')
            return
        action = request['action']
        
        if operator.eq(action, 'shutdown')==0:
            self._shutDown()            
        elif operator.eq(action, 'service')==0:
            self._handleServiceReq(request)
        else:
            self.logger.error('Unknown action request:%s', action)
        
    def _handleServiceReq(self, request):
        if isinstance(self._serviceHandler,RequestHandler):
            self._serviceHandler.addRequest(request)
        else:
            func = request['funcName']
            paraList = request['paraList']
            credential = request['credential']
            mtd = getattr(self._serviceHandler, func)
            mtd(paraList, credential)
            
    def _stopTasks(self):
        '''
        gracefully wait until the requests in request handler are handled
         
        '''
        pass
    
    def peerStop(self, credential):
        self.logger.error('peerStop shall be implemented by sub-class')
        pass
    
class RPCFuncBase(object):
    def __init__(self, server):
        self._server = server
                
    def _buildServiceDict(self):
        request = dict()
        request['action']='service'
        return request
    
    def peerStop(self, credential):
        self._server.peerStop(credential)
        return True
        
    def heartbeat(self):
        return True
    
    def service(self, funcName, paraList, credential):
        request = self._buildServiceDict()
        request['funcName'] = funcName
        request['paraList'] = paraList
        request['credential'] = credential
        self._server.addRequest(request)
        return True
        
class ServiceHandlerBase(object):
    def __init__(self, server):
        self._server = server
    
class Credential(object):
    def __init__(self, identifier, credentialStr):
        self.identifier = identifier
        self.credentialStr = credentialStr
        self.logger = Logging.LOGGER        
    
    def __str__(self):
        return 'ID=%s, CS=%s' % (self.identifier, self.credentialStr)
    
    def check(self, credential):
        if credential is None:
            return False
        if isinstance(credential, dict):
            if 'identifier' not in credential or 'credentialStr' not in credential:
                self.logger.error('Credential does not contain the keys')
                return False
            if self.identifier == credential['identifier'] and self.credentialStr == credential['credentialStr']:
                return True
        else:
            if self.identifier is None or self.credentialStr is None:
                self.logger.error('Credential member is none')
                return False
            if self.identifier == credential.identifier and self.credentialStr == credential.credentialStr:
                return True
            else:
                return False
import socket

class SocketClient(object):
    def __init__(self, ip, port):
        self.__ip = ip
        self.__port = port
        self.__socket = None
        self.logger = Logging.LOGGER
    
    def connect(self):
        if self.__socket is not None:
            self.logger.error('Socket is not None. Could not open again')
            return False
        try:
            self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.__socket.settimeout(1)
            self.__socket.connect((self.__ip, self.__port))
            return True
        except:
            self.__socket = None
            return False
            
    def close(self):
        if self.__socket is None:
            self.logger.error('Socket is None. Could not be closed.')
            return False
        self.__socket.close()
        return True
    
    def sendData(self, data):
        if self.__socket is None:
            self.logger.error('Socket is None. Could not be send data.')
            return False
        try:
            self.__socket.sendall(data)
        except Exception:
            type_, value_, traceback_ = sys.exc_info()
            ex = traceback.format_exception(type_, value_, traceback_)
            self.logger.error('Fail to send data:%s', ex)

def getNextAvailablePort(ip, startPort, endPort):
    while(startPort<=endPort):
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.settimeout(1)
        try:
            sk.connect((ip, startPort))
            sk.close()
            startPort+=1            
        except:
            return startPort
    return None        

class Audit(threading.Thread):
    '''
    Audit general for all purpose to periodically waken up to perform a specific action, which
    is wrapped in json format parameter: {'action':'audit', 'parameter':<parameter>} and sent to its controller to handle
    the audit request.
    '''
    def __init__(self, controller, interval, parameter=None, onetime=False, priority=0):
        super(Audit, self).__init__()
        self.__controller = controller
        self.interval = interval
        self.__shutDown = False
        self.__onetime = onetime
        self.__parameter = parameter
        self.__priority = priority
        self.finished = threading.Event()
        self.setDaemon(True)

    def shutDown(self):        
        self.__shutDown = True
        self.finished.set()
    
    def run(self):
        if self.__onetime:
            self.finished.wait(self.interval)
            if self.__shutDown is True:                
                return
            self.__doWork()
            return
            
        while(True):
            self.finished.wait(self.interval)
            if self.__shutDown is True:                
                return
            self.__doWork()
    
    def __doWork(self):
        request = dict()
        request['action']='audit'
        if self.__parameter is not None:
            request['parameter'] = self.__parameter
            '''
            #DEBUG only
            if self.__parameter == 'strategyCutoffAudit':
                try:
                    Logging.LOGGER.debug('strategyCutoffAudit dowork')
                    queuelist = self.__controller._RequestHandler__requestQueueList
                    queueInfo = [q.qsize() for q in queuelist]
                    Logging.LOGGER.debug('strategyCutoffAudit dowork, queueinfo:%s', queueInfo)
                except:
                    pass
            '''
        self.__controller.addRequest(request, self.__priority)                  

class MySqlProxy(StoppableThread):
    def __init__(self, host, port, user, passwd, db, interval=60*60, logger=None):
        #import pymysql
        import pymysql
        self.connPara = {'host':host,'user':user, 'passwd':passwd, 'db':db, 'port':port,'charset':'utf8'}
        #'charset':'utf8'
        self.conn = pymysql.Connect(**(self.connPara))
        self.conn.ping(True)
        self.cur=self.conn.cursor()
        self.cur.execute('SET NAMES utf8mb4')
        self.cur.execute("SET CHARACTER SET utf8mb4")
        self.cur.execute("SET character_set_connection=utf8mb4")        
        self.__interval = interval
        self.logger = logger
        if self.logger is None:
            self.logger = Logging.LOGGER
        super(MySqlProxy, self).__init__(5)
    def copy(self):
        return MySqlProxy(self.connPara['host'],
                          self.connPara['port'],
                          self.connPara['user'],
                          self.connPara['passwd'],
                          self.connPara['db'],
                          self.__interval,
                          self.logger)        
    def run(self):
        while(True):
            self.looseSleep(self.__interval)
            if self.isShutDown():
                try:
                    self.close()
                    self.logger.info('Close Mysql connection')
                except Exception as e:
                    pass
                return
                           
    def fetchall(self):
        return self.cur.fetchall()
        
    def execute(self, sql):
        try:
            return self.cur.execute(sql)
        except Exception as e:
            self.logger.error('ERROR sql:%s; Exception is %s', sql, str(e))
            raise e
    
    def commit(self):
        self.conn.commit()
    
    def close(self):
        try:
            self.conn.close()
            self.cur.close()
        except Exception:
            pass

import os
class PIDUtils(threading.Thread):
    def __init__(self, appname, stopFunc, sleep_time, logger=None):
        self.__appname = appname
        self.__stopFunc = stopFunc
        self.__sleep_time = sleep_time
        self.logger = logger
        if self.logger is None:
            self.logger = Logging.LOGGER
        super(PIDUtils, self).__init__()
    
    def run(self):
        while(True):
            #print 'checking pid:%s' % str(PIDUtils.isPidFileExist(self.__appname))
            if not PIDUtils.isPidFileExist(self.__appname):
                self.__stopFunc()
                self.logger.info('App:%s is shutdown', self.__appname)
                time.sleep(30)
                self.logger.info('Following thread is still active:%s', threading._active)
                return
            time.sleep(self.__sleep_time)
            
    @staticmethod
    def isPidFileExist(appname):
        return os.path.exists('./.'+appname+'.pid')
    @staticmethod
    def writePid(appname, pid):
        pidfile = open('./.'+appname+'.pid','w')
        pidfile.write(str(pid))
        pidfile.flush()
        pidfile.close()

import datetime
class CTPUtils(StoppableThread):
    Singleton = None
    DEFAULT_TIME_WINDOW = {
	"tradingTime":[["09:30","11:30"],["13:00","15:00"]],
	"breakTime":[["11:30","13:00"]],
	"clearExpiredOrderTime":[["15:10","15:15"]],
	"auditTime":[["08:50","11:35"],["13:00","15:05"]],
	"orderAuditTime":[["09:00","09:10"],["15:15","15:20"],["16:00","16:05"],["17:00","17:05"],["18:00","18:05"],["19:00","19:05"]],
	"ctpCriticalTime":[["08:45","11:31"],["12:45","15:01"]],
	"mdTime":[["08:58","11:31"],["12:58","15:01"]],
	"busyCalculatingTime":[["23:34","23:59"]],
    "strategyForbidTime":[["08:00","15:30"]]
} 
    KEY_TRADING_TIME = 'tradingTime'
    KEY_BREAK_TIME = 'breakTime'
    KEY_EXPIRED_ORDER_TIME = 'clearExpiredOrderTime'
    KEY_AUDIT_TIME = 'auditTime'
    KEY_ORDER_AUDIT_TIME = 'orderAuditTime'
    KEY_CTP_CRITICAL_TIME = 'ctpCriticalTime'
    KEY_MD_TIME = 'mdTime'
    KEY_BUSY_CALCULATING_TIME = 'busyCalculatingTime'
    KEY_STRATEGY_FORBID_TIME = 'strategyForbidTime'
    
    MARKET_SSE = 'SSE'
    MARKET_SZE = 'SZE'
    MARKET_UNKNOWN = ''
    def __init__(self, interval=60, timewindow_cfg='conf/timewindow.cfg', holiday_cfg='conf/holiday.cfg', sse_cfg='conf/sse.cfg', sze_cfg='conf/sze.cfg', ignore_holiday=False):
        self.__interval = interval
        self.__timewindow_cfg = timewindow_cfg
        self.__holiday_cfg = holiday_cfg
        self.__sse_cfg = sse_cfg
        self.__sze_cfg = sze_cfg
        self.__timeWindow = CTPUtils.DEFAULT_TIME_WINDOW
        self.__ignoreholiday = ignore_holiday
        super(CTPUtils, self).__init__(self.__interval)
        self.logger = Logging.LOGGER
        self.__readTimeWindow()
        self.__holidayList = self.__readCfgFile(self.__holiday_cfg)
        self.__sseList = self.__readCfgFile(self.__sse_cfg)
        self.__szeList = self.__readCfgFile(self.__sze_cfg)
        self.setDaemon(True)
    
    def run(self):
        
        while(True):
            self.__readTimeWindow()
            self.__holidayList = self.__readCfgFile(self.__holiday_cfg)
            self.__sseList = self.__readCfgFile(self.__sse_cfg)
            self.__szeList = self.__readCfgFile(self.__sze_cfg)
            self.looseSleep(self.__interval)
            if self.isShutDown():
                return
    
    def __readTimeWindow(self):
        try:
            c = Configuration(self.__timewindow_cfg)
            self.__timeWindow = c.readConfig()
        except:
            type_, value_, traceback_ = sys.exc_info()
            ex = traceback.format_exception(type_, value_, traceback_)
            self.logger.warn('Fail to read conf/timewindow.cfg, reset to default. %s', ex)
            self.__timeWindow = CTPUtils.DEFAULT_TIME_WINDOW
    
    def __readCfgFile(self, cfg_file):
        try:
            f = open(cfg_file)
            fl = list(f)
            f.close()
            return filter(lambda y: not y.startswith('#'), map(lambda x:x.replace('\n','').replace('\r','').split('\t')[0], fl))            
        except:
            type_, value_, traceback_ = sys.exc_info()
            ex = traceback.format_exception(type_, value_, traceback_)
            self.logger.warn('Fail to read %s, reset to null. %s', cfg_file, ex)
            return list()
    
    def getMarket(self, stockid):
        if stockid in self.__sseList:
            return CTPUtils.MARKET_SSE
        elif stockid in self.__szeList:
            return CTPUtils.MARKET_SZE
        else:
            return CTPUtils.MARKET_UNKNOWN
    def isHoliday(self, pDatetime=None):
        '''
        Judge whether it is holiday or not
        '''
        if self.__ignoreholiday:
            return False
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        
        if pDatetime.isoweekday()>5:
            return True
        
        ds = pDatetime.strftime("%Y-%m-%d")
        return ds in self.__holidayList
    
    def isTradingTime(self, pDatetime=None):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        if self.isHoliday(pDatetime):
            return False
        return self.__isInTimeWindow(pDatetime, CTPUtils.KEY_TRADING_TIME)

    def isClearExpiredOrderTime(self, pDatetime=None):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        if self.isHoliday(pDatetime):
            return False
        return self.__isInTimeWindow(pDatetime, CTPUtils.KEY_EXPIRED_ORDER_TIME)

    def isBreakTime(self, pDatetime=None):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        if self.isHoliday(pDatetime):
            return False
        return self.__isInTimeWindow(pDatetime, CTPUtils.KEY_BREAK_TIME)
    
    def isAuditTime(self, pDatetime=None):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        if self.isHoliday(pDatetime):
            return False
        return self.__isInTimeWindow(pDatetime, CTPUtils.KEY_AUDIT_TIME)

    def isOrderAuditTime(self, pDatetime=None):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        if self.isHoliday(pDatetime):
            return False
        return self.__isInTimeWindow(pDatetime, CTPUtils.KEY_ORDER_AUDIT_TIME)

    def isCTPCriticalTime(self, pDatetime=None):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        if self.isHoliday(pDatetime):
            return False
        return self.__isInTimeWindow(pDatetime, CTPUtils.KEY_CTP_CRITICAL_TIME)
    
    def isMDTime(self, pDatetime=None):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        if self.isHoliday(pDatetime):
            return False
        return self.__isInTimeWindow(pDatetime, CTPUtils.KEY_MD_TIME)
    
    def isBusyCalculatingTime(self, pDatetime=None):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        if self.isHoliday(pDatetime):
            return False
        return self.__isInTimeWindow(pDatetime, CTPUtils.KEY_BUSY_CALCULATING_TIME)
    
    def isStrategyForbidTime(self, pDatetime=None):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        if self.isHoliday(pDatetime):
            return False
        return self.__isInTimeWindow(pDatetime, CTPUtils.KEY_STRATEGY_FORBID_TIME)
        
    def getNextForTimeWindow(self, pDatetime, key):
        if pDatetime is None:
            pDatetime = datetime.datetime.now()
        
        timewindow = self.__timeWindow[key]
        if self.isHoliday(pDatetime):
            while self.isHoliday(pDatetime):
                pDatetime += datetime.timedelta(days=1)
            return datetime.datetime(pDatetime.year, pDatetime.month, pDatetime.day,int(timewindow[0][0][:2]),int(timewindow[0][0][3:5]))
        
        time_str = pDatetime.strftime("%H:%M")        
        if time_str < timewindow[0][0]:
            pDatetime = datetime.datetime(pDatetime.year, pDatetime.month, pDatetime.day, int(timewindow[0][0][:2]), int(timewindow[0][0][3:5]))
        elif time_str >= timewindow[len(timewindow)-1][1]:
            pDatetime = datetime.datetime(pDatetime.year, pDatetime.month, pDatetime.day, int(timewindow[0][0][:2]), int(timewindow[0][0][3:5]))
            pDatetime += datetime.timedelta(days=1)
        else:
            for item in timewindow:
                if time_str<item[0]:
                    pDatetime = datetime.datetime(pDatetime.year, pDatetime.month, pDatetime.day, int(item[0][:2]), int(item[0][3:5]))
                    break
                if time_str>=item[0] and time_str<item[1]:
                    break 
        while self.isHoliday(pDatetime):
            pDatetime += datetime.timedelta(days=1)
        return pDatetime
    
    def __isInTimeWindow(self, pDatetime, key):
        time_str = pDatetime.strftime("%H:%M")
        timewindow = self.__timeWindow[key]
        for item in timewindow:
            if time_str>=item[0] and time_str<item[1]:
                return True
        
        return False
         
    @staticmethod
    def getTradingDay(currentDatetime=None):
        if currentDatetime is None:
            currentDatetime = datetime.datetime.now()
        return currentDatetime.strftime("%Y%m%d")
    
    @staticmethod
    def fetchDateAndTimeStrFromPeriod(self, period):
        return (period.strftime("%Y%m%d"), period.strftime("%H:%M:%S"))
    
    @staticmethod
    def fetchTimePeriod(utime, gran=10):
        timeint = time.mktime(utime.timetuple())
        return datetime.datetime.fromtimestamp(int(timeint)/gran * gran + (gran-1))

    @staticmethod
    def fetchTimeFromMarketData(marketData, logger):
        try:
            tradingDay = str(marketData.TradingDay)
            updateTime = str(marketData.UpdateTime)
            updateMillisec = int(marketData.UpdateMillisec)
            year = int(tradingDay[:4])
            month = int(tradingDay[4:6])
            day = int(tradingDay[6:])
            timearray = updateTime.split(':')
            hour = int(timearray[0])
            minute = int(timearray[1])
            seconds = int(timearray[2]) 
            utime = datetime.datetime(year, month, day, hour, minute, seconds, updateMillisec*1000)
            return utime
        except Exception:
            type_, value_, traceback_ = sys.exc_info()
            ex = traceback.format_exception(type_, value_, traceback_)
            marketData.printSelf(logger)
            return None
    
    @staticmethod
    def isReasonableTime(utime, delta_seconds):
        currentTime = datetime.datetime.now()
        return utime>=currentTime-datetime.timedelta(seconds=delta_seconds) and\
                utime<=currentTime+datetime.timedelta(seconds=delta_seconds) 
    
    @staticmethod
    def fetchTimePeriodFromMarketData(marketData, gran=10):
        utime = CTPUtils.fetchTimeFromMarketData(marketData)
        return CTPUtils.fetchTimePeriod(utime, gran)        
    
    @staticmethod
    def toDict(ctp_struct_data):
        cd = ctp_struct_data.toDict()
        for key in cd.keys():
            if cd[key]=='\x00':
                cd[key]=''
        return cd    
    @staticmethod
    def fromDict(ctp_type, ctp_dict):
        for key in ctp_dict:
            obj = str(ctp_type.__dict__[key])
            if 'type=c_char' in obj and 'size=1' in obj and ctp_dict[key]=='':
                ctp_dict[key]='\x00'
        return ctp_type(**ctp_dict)
    
    @staticmethod
    def extractStockCode(stockcode):
        return (stockcode[:3], stockcode[3:])
    @staticmethod
    def composeStockCode(exchangeId, instrumentId):
        return exchangeId+instrumentId

    def getWaitTime(self, timeStr, ignoreHoliday=False):
        now = datetime.datetime.now()
        hours = int(timeStr[:2])
        minutes = 0
        seconds = 0
        if len(timeStr) == 5:
            minutes = int(timeStr[3:5])
        if len(timeStr) == 8:
            seconds = int(timeStr[6:8])
        nextPossible = datetime.datetime(now.year, now.month, now.day, hours, minutes, seconds)
        if nextPossible<now:
            nextPossible = nextPossible + datetime.timedelta(1)
        if ignoreHoliday is False:
            while(CTPUtils.Singleton.isHoliday(nextPossible)):
                nextPossible = nextPossible + datetime.timedelta(1)

        return (time.mktime(nextPossible.timetuple())-time.time(), nextPossible)
    
    @staticmethod
    def getRoundNum(stockid):
        if stockid.startswith('3') or stockid.startswith('6') or stockid.startswith('0'):
            return 2
        else:
            return 3
    
    @staticmethod
    def setPriceVolume(obj, marketdata):
        obj.openprice = marketdata.OpenPrice
        obj.currentprice = marketdata.LastPrice
        obj.lastclose = marketdata.PreClosePrice
        obj.bidpriceone = marketdata.BidPrice1
        obj.askpriceone = marketdata.AskPrice1
        obj.bidpricetwo = marketdata.BidPrice2
        obj.askpricetwo = marketdata.AskPrice2
        obj.bidpricethree = marketdata.BidPrice3
        obj.askpricethree = marketdata.AskPrice3
        obj.bidpricefour = marketdata.BidPrice4
        obj.askpricefour = marketdata.AskPrice4
        obj.bidpricefive = marketdata.BidPrice5
        obj.askpricefive = marketdata.AskPrice5
        obj.bidvolumeone = marketdata.BidVolume1
        obj.askvolumeone = marketdata.AskVolume1
        obj.bidvolumetwo = marketdata.BidVolume2
        obj.askvolumetwo = marketdata.AskVolume2
        obj.bidvolumethree = marketdata.BidVolume3
        obj.askvolumethree = marketdata.AskVolume3
        obj.bidvolumefour = marketdata.BidVolume4
        obj.askvolumefour = marketdata.AskVolume4
        obj.bidvolumefive = marketdata.BidVolume5
        obj.askvolumefive = marketdata.AskVolume5
        obj.updatetime = marketdata.UpdateTime
        
class UTValidateInfo(object):
    def __init__(self, funcName, para):
        self.funcName = funcName
        self.para = para

class TimeUtils():
    SecOfOneDay        = 86400 #3600*24
    SecOfTimeZoneDelta = 28800 #3600*8
    ISOTIMEFORMAT      = '%Y-%m-%d %H:%M:%S'
    ISOTIMEFORMAT_S    = '%Y-%m-%d'

    #determine whether it is within time window to insert parked order
    @staticmethod
    def isCrawlerTime():
        curTS         = time.time()
        crawlerTW = [[32400,54000]] #9:00-15:00
        return TimeUtils.isInTimeWindow(curTS, crawlerTW)

    #get current datetime in format '%Y-%m-%d %H:%M:%S'
    @staticmethod
    def getCurTime():
        return time.strftime(TimeUtils.ISOTIMEFORMAT, time.localtime(time.time()))

    #get current date in format '%Y-%m-%d'
    @staticmethod
    def getCurDate():
        return time.strftime(TimeUtils.ISOTIMEFORMAT_S, time.localtime(time.time()))

    #convert float to date and time in format '%Y-%m-%d' and '%H:%M:%S'
    @staticmethod
    def getDateAndTime(timeInFloat):
        return (time.strftime('%Y%m%d', time.localtime(timeInFloat)), time.strftime('%H:%M:%S', time.localtime(timeInFloat)))

    #convert float to datetime in format '%Y-%m-%d %H:%M:%S'
    @staticmethod
    def getTime(timeInFloat):
        return time.strftime(TimeUtils.ISOTIMEFORMAT, time.localtime(timeInFloat))

    #convert string datetime in format '%Y-%m-%d %H:%M:%S' to float datetime
    @staticmethod
    def getFloatTime(timeInString):
        ts = time.strptime(timeInString, TimeUtils.ISOTIMEFORMAT)
        return time.mktime(ts)

    #get today start time with format '%Y-%m-%d %H:%M:%S' of the daytime of input parameter
    @staticmethod
    def getDayStartTime(timeInString):
        return time.strftime(TimeUtils.ISOTIMEFORMAT_S, time.localtime(TimeUtils.getFloatTime(timeInString)))+'00:00:00'

    #get current date with format like 20140601
    @staticmethod
    def getCurDecimalDate():
        curTime = time.localtime(time.time())
        return curTime.tm_year*10000 + curTime.tm_mon*100 + curTime.tm_mday

    #determine whether it is within given time window
    @staticmethod
    def isInTimeWindow(ts, twList):
        result = False
        if datetime.datetime.fromtimestamp(ts).isoweekday() > 5:
            return result
        SecOfTime = (int(ts) + TimeUtils.SecOfTimeZoneDelta) % TimeUtils.SecOfOneDay
        for i in range(len(twList)):
            if SecOfTime >= twList[i][0] and SecOfTime <= twList[i][1]:
                result = True
                break
        return result

class DBInformation():
    def __init__(self,
                 dbHost   = 'localhost',
                 dbPort   = 3306,
                 dbUser   = 'root',
                 dbPasswd = 'mysql',
                 dbName   = 'sa3'):
        self.dbHost   = dbHost
        self.dbPort   = dbPort
        self.dbUser   = dbUser
        self.dbPasswd = dbPasswd
        self.dbName   = dbName

    def toString(self):
        displayStr = '		Configuration Information: DB Information\n'
        displayStr = displayStr + '	%s: %s\n' % ('dbHost',  self.dbHost)
        displayStr = displayStr + '	%s: %s\n' % ('dbPort',  self.dbPort)
        displayStr = displayStr + '	%s: %s\n' % ('dbUser',  self.dbUser)
        displayStr = displayStr + '	%s: %s\n' % ('dbPasswd',self.dbPasswd)
        displayStr = displayStr + '	%s: %s\n' % ('dbName',  self.dbName)
        return displayStr
