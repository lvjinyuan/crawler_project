# -*- coding:utf-8 -*-
'''
Created on 1 Oct 2017

@author: eyaomai
'''
from crawler.lib.common.globalvar import GlobalVariable
from crawler.lib.common.utils import Logging
import re, datetime, math
import networkx as nx
import jieba.posseg as pseg
import numpy as np
POS = {  
    "n": {  #1. 名词  (1个一类，7个二类，5个三类)  
        "n":"名词",  
        "nr":"人名",  
        "nr1":"汉语姓氏",  
        "nr2":"汉语名字",  
        "nrj":"日语人名",  
        "nrf":"音译人名",  
        "ns":"地名",  
        "nsf":"音译地名",  
        "nt":"机构团体名",  
        "nz":"其它专名",  
        "nl":"名词性惯用语",  
        "ng":"名词性语素"  
    },  
    "t": {  #2. 时间词(1个一类，1个二类)  
        "t":"时间词",  
        "tg":"时间词性语素"  
    },  
    "s": {  #3. 处所词(1个一类)  
        "s":"处所词"  
    },  
    "f": {  #4. 方位词(1个一类)  
        "f":"方位词"  
    },  
    "v": {  #5. 动词(1个一类，9个二类)  
        "v":"动词",  
        "vd":"副动词",  
        "vn":"名动词",  
        "vshi":"动词“是”",  
        "vyou":"动词“有”",  
        "vf":"趋向动词",  
        "vx":"形式动词",  
        "vi":"不及物动词（内动词）",  
        "vl":"动词性惯用语",  
        "vg":"动词性语素"  
    },  
    "a": {  #6. 形容词(1个一类，4个二类)  
        "a":"形容词",  
        "ad":"副形词",  
        "an":"名形词",  
        "ag":"形容词性语素",  
        "al":"形容词性惯用语"  
    },  
    "b": {  #7. 区别词(1个一类，2个二类)  
        "b":"区别词",  
        "bl":"区别词性惯用语"  
    },  
    "z": {  #8. 状态词(1个一类)  
        "z":"状态词"  
    },  
    "r": {  #9. 代词(1个一类，4个二类，6个三类)  
        "r":"代词",  
        "rr":"人称代词",  
        "rz":"指示代词",  
        "rzt":"时间指示代词",  
        "rzs":"处所指示代词",  
        "rzv":"谓词性指示代词",  
        "ry":"疑问代词",  
        "ryt":"时间疑问代词",  
        "rys":"处所疑问代词",  
        "ryv":"谓词性疑问代词",  
        "rg":"代词性语素"  
    },  
    "m": {  #10.    数词(1个一类，1个二类)  
        "m":"数词",  
        "mq":"数量词"  
    },  
    "q": {  #11.    量词(1个一类，2个二类)  
        "q":"量词",  
        "qv":"动量词",  
        "qt":"时量词"  
    },  
    "d": {  #12.    副词(1个一类)  
        "d":"副词"  
    },  
    "p": {  #13.    介词(1个一类，2个二类)  
        "p":"介词",  
        "pba":"介词“把”",  
        "pbei":"介词“被”"  
    },  
    "c": {  #14.    连词(1个一类，1个二类)  
        "c":"连词",  
        "cc":"并列连词"  
    },  
    "u": {  #15.    助词(1个一类，15个二类)  
        "u":"助词",  
        "uzhe":"着",  
        "ule":"了 喽",  
        "uguo":"过",  
        "ude1":"的 底",  
        "ude2":"地",  
        "ude3":"得",  
        "usuo":"所",  
        "udeng":"等 等等 云云",  
        "uyy":"一样 一般 似的 般",  
        "udh":"的话",  
        "uls":"来讲 来说 而言 说来",  
        "uzhi":"之",  
        "ulian":"连 " #（“连小学生都会”）  
    },  
    "e": {  #16.    叹词(1个一类)  
        "e":"叹词"  
    },  
    "y": {  #17.    语气词(1个一类)  
        "y":"语气词(delete yg)"  
    },  
    "o": {  #18.    拟声词(1个一类)  
        "o":"拟声词"  
    },  
    "h": {  #19.    前缀(1个一类)  
        "h":"前缀"  
    },  
    "k": {  #20.    后缀(1个一类)  
        "k":"后缀"  
    },  
    "x": {  #21.    字符串(1个一类，2个二类)  
        "x":"字符串",  
        "xx":"非语素字",  
        "xu":"网址URL"  
    },  
    "w":{   #22.    标点符号(1个一类，16个二类)  
        "w":"标点符号",  
        "wkz":"左括号",    #（ 〔  ［  ｛  《 【  〖 〈   半角：( [ { <  
        "wky":"右括号",    #） 〕  ］ ｝ 》  】 〗 〉 半角： ) ] { >  
        "wyz":"全角左引号",  #“ ‘ 『  
        "wyy":"全角右引号",  #” ’ 』  
        "wj":"全角句号",    #。  
        "ww":"问号",  #全角：？ 半角：?  
        "wt":"叹号",  #全角：！ 半角：!  
        "wd":"逗号",  #全角：， 半角：,  
        "wf":"分号",  #全角：； 半角： ;  
        "wn":"顿号",  #全角：、  
        "wm":"冒号",  #全角：： 半角： :  
        "ws":"省略号", #全角：……  …  
        "wp":"破折号", #全角：——   －－   ——－   半角：---  ----  
        "wb":"百分号千分号",  #全角：％ ‰   半角：%  
        "wh":"单位符号" #全角：￥ ＄ ￡  °  ℃  半角：$  
    }  
}  
  
ALLOWED_SPEECH_TAGS = ['an', 'i', 'j', 'l', 'n', 'nr', 'nrfg', 'ns', 'nt', 'nz', 't', 'v', 'vd', 'vn', 'eng']
SENTENCE_DELIMETER = [u'?', u'!', u';', u'？', u'！', u'。', u'；', u'……', u'…', u'\n']
class AnalyticsBase(object):
    '''
    分析类基类
    
    '''


    def __init__(self, dbProxy, entity_id='', logger=None):
        '''
        Constructor
        '''
        self.dbProxy = dbProxy
        self.entity_id = entity_id
        if logger is None:
            self.logger = Logging.getLogger(Logging.LOGGER_NAME_DEFAULT)
        else:
            self.logger = logger
        
        confDict = GlobalVariable.getSAConfDict()
        self.saConf = None
        if entity_id in confDict:
            self.saConf = confDict[entity_id]
            
    def analysis(self, **kwargs):
        pass

class Tools(object):
    
    @staticmethod
    def findAll(pattern, s):
        hitWords = list()
        if s is None or len(s)==0:
            return hitWords
        wordList = pattern.split('|')
        for word in wordList:
            if s.find(word)>=0:
                hitWords.append(word)
        return hitWords

    @staticmethod
    def findAllFromList(pattern, strList):
        hitWords = list()
        if strList is None or len(strList)==0:
            return hitWords
        wordList = pattern.split('|')
        for word in wordList:
            if word in strList:
                hitWords.append(word)
        return hitWords
    @staticmethod
    def isExistsFromList(pattern, strList):
        if len(strList) == 0:
            return False
        wordList = pattern.split('|')
        for word in wordList:
            if word in strList:
                return True
        return False
    
    @staticmethod
    def isExists(pattern, s):
        if s is None or len(s) ==0:
            return False
        wordList = pattern.split('|')
        for word in wordList:
            if s.find(word)>=0:
                return True
        return False
    
    @staticmethod
    def getPeriod():
        n = datetime.datetime.now() - datetime.timedelta(hours=1)
        return int(n.strftime("%Y%m%d%H"))
    
    @staticmethod
    def fetchStopWords(stopWordsFile):
        with open(stopWordsFile, encoding='utf-8') as f:
            contentList = map(lambda x: x.replace("\r","").replace("\t","").replace("\n", "").strip(), list(f))
            return filter(lambda x: not x.startswith('#'), contentList)

    @staticmethod
    def get_similarity(word_list1, word_list2):
        """默认的用于计算两个句子相似度的函数。
    
        Keyword arguments:
        word_list1, word_list2  --  分别代表两个句子，都是由单词组成的列表
        """
        words   = list(set(word_list1 + word_list2))        
        vector1 = [float(word_list1.count(word)) for word in words]
        vector2 = [float(word_list2.count(word)) for word in words]
        
        vector3 = [vector1[x]*vector2[x]  for x in xrange(len(vector1))]
        vector4 = [1 for num in vector3 if num > 0.]
        co_occur_num = sum(vector4)
    
        if abs(co_occur_num) <= 1e-12:
            return 0.
        
        denominator = math.log(float(len(word_list1))) + math.log(float(len(word_list2))) # 分母
        
        if abs(denominator) < 1e-12:
            return 0.
        
        return co_occur_num / denominator
    
    @staticmethod
    def sentenceSegment(content):
        '''
        将内容分解成句子
        '''
        res = [content]
        for sep in SENTENCE_DELIMETER:
            text, res = res, []
            for seq in text:
                res += seq.split(sep)
        res = [s.strip() for s in res if len(s.strip()) > 0]
        return res 

    @staticmethod
    def wordSegementFromSentences(sentenceList, stopWordList):
        '''
        针对每一个句子进行分词
        '''
        return map(lambda sentence: Tools.segment(sentence, stopWordList), sentenceList)

    @staticmethod
    def segment(content, stopWordList):
        '''
        分词
        '''
        if content is None or len(content)==0:
            return []
        jieba_result = pseg.cut(content)
        result = filter(lambda y: len(y)>0,map(lambda x: x.word.strip(), filter(lambda w: w.flag in ALLOWED_SPEECH_TAGS, jieba_result)))
        return filter(lambda x: x not in stopWordList, result)
    @staticmethod
    def combine(word_list, window = 2):
        """构造在window下的单词组合，用来构造单词之间的边。
        
        Keyword arguments:
        word_list  --  list of str, 由单词组成的列表。
        windows    --  int, 窗口大小。
        """
        if window < 2: window = 2
        for x in xrange(1, window):
            if x >= len(word_list):
                break
            word_list2 = word_list[x:]
            res = zip(word_list, word_list2)
            for r in res:
                yield r

    @staticmethod
    def sortWords(vertext, edge, window = 2, pagerank_config = {'alpha': 0.85,}):
        sorted_words   = []
        word_index     = {}
        index_word     = {}
        words_number   = 0
        for word_list in vertext:
            for word in word_list:
                if not word in word_index:
                    word_index[word] = words_number
                    index_word[words_number] = word
                    words_number += 1
    
        graph = np.zeros((words_number, words_number))
        
        for word_list in edge:
            for w1, w2 in Tools.combine(word_list, window):
                if w1 in word_index and w2 in word_index:
                    index1 = word_index[w1]
                    index2 = word_index[w2]
                    graph[index1][index2] = 1.0
                    graph[index2][index1] = 1.0

        nx_graph = nx.from_numpy_matrix(graph)
        
        scores = nx.pagerank(nx_graph, **pagerank_config)          # this is a dict
        sorted_scores = sorted(scores.items(), key = lambda item: item[1], reverse=True)
        return map(lambda x: (index_word[x[0]],x[1]), sorted_scores)

    @staticmethod
    def get_title_similarity(title_word_list, word_list):
        """默认的用于计算句子与标题相似度的函数
    
        Keyword arguments:
        title_word_list  --  代表标题，由单词组成的列表
        word_list  --  代表一个句子，由单词组成的列表
        """
        words   = list(set(title_word_list + word_list))        
        vector1 = [float(title_word_list.count(word)) for word in words]
        vector2 = [float(word_list.count(word)) for word in words]
        
        vector3 = [vector1[x]*vector2[x]  for x in xrange(len(vector1))]
        vector4 = [1 for num in vector3 if num > 0.]
        co_occur_num = sum(vector4)
    
        if abs(co_occur_num) <= 1e-12:
            return 0.
        
        denominator = math.log(float(len(title_word_list))) + math.log(float(len(word_list))) # 分母
        
        if abs(denominator) < 1e-12:
            return 0.
        
        return co_occur_num / denominator
    @staticmethod
    def get_keyterm_similarity(keyterm_word_list, word_list):
        """默认的用于计算句子与候选关键术语相似度的函数
    
        Keyword arguments:
        keyterm_word_list  --  代表关键术语，由单词组成的列表
        word_list  --  代表一个句子，由单词组成的列表
        """
        words   = list(set(keyterm_word_list + word_list))        
        vector1 = [float(keyterm_word_list.count(word)) for word in words]
        vector2 = [float(word_list.count(word)) for word in words]
        
        vector3 = [vector1[x]*vector2[x]  for x in xrange(len(vector1))]
        vector4 = [1 for num in vector3 if num > 0.]
        co_occur_num = sum(vector4)
    
        if abs(co_occur_num) <= 1e-12:
            return 0.
        
        denominator = math.log(float(len(keyterm_word_list))) + math.log(float(len(word_list))) # 分母
        
        if abs(denominator) < 1e-12:
            return 0.
        
        return co_occur_num / denominator
    
    @staticmethod
    def sortSentences(sentenceList, wordSentenceList, titleWordList, keyWords, 
                       alpha = 0.3, beta = 0.4, gamma = 0.3,
                       omega = 1, theta = 5, pagerank_config = {'alpha': 0.85,}):
        """将句子按照关键程度从大到小排序
    
        Keyword arguments:
        sentenceList         --  列表，元素是句子
        wordSentenceList             --  二维列表，子列表和sentences中的句子对应，子列表由单词组成
        titleWordList       --  一维列表，每个元素由单词和权重组成
        keyWords     --  一维列表，子列表由单词组成
        sim_func          --  计算两个句子的相似性，参数是两个由单词组成的列表
        title_sim_func    --  计算句子和标题的相似性，参数是两个由单词组成的列表
        keyterm_sim_func  --  计算句子和关键术语的相似性，参数是两个由单词组成的列表
        alpha,beta,gamma  --  pagerank参数
        pagerank_config   --  pagerank的设置
        """
        sorted_sentences = []
    
        _source = wordSentenceList
        
        # 构造无向图 
        sentences_num = len(_source)
        graph = np.zeros((sentences_num, sentences_num))
        vertex_title_sim = np.zeros(sentences_num)
        vertex_keyterm_sim = np.zeros(sentences_num)

        # 求边edge的权重
        for x in xrange(sentences_num):
            for y in xrange(sentences_num):
                similarity = Tools.get_similarity( _source[x], _source[y] )
                title_similarity = Tools.get_title_similarity(titleWordList, _source[x])
                keyterm_similarity = Tools.get_keyterm_similarity(keyWords, _source[x])
                graph[x, y] = alpha * similarity + beta * title_similarity + gamma * keyterm_similarity
                #graph[y, x] = alpha * similarity + beta * title_similarity + gamma * keyterm_similarity
        nx_graph = nx.from_numpy_matrix(graph)
        
        # override
        scores = nx.pagerank(nx_graph, **pagerank_config)   # this is a dict
        
        # 时间递减效应
        new_scores = []
        for index, value in scores.items():
            order_weight = 1 #omega * exp(-index/theta)
            value = value * order_weight
            new_scores.append((index, value))
    
    
        sorted_scores = sorted(new_scores, key = lambda item: item[1], reverse=True)
        return map(lambda x: (sentenceList[x[0]],x[1]), sorted_scores)
    
        return sorted_sentences
        