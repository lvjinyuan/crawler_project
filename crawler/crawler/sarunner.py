import os,argparse
from crawler.lib.analysis.emotionanalysis import EmotionAnalysis
from crawler.lib.analysis.heatanalysis import HeatAnalytics
from crawler.items.save_mysql import SaveMysqlPipeline, SQLCrawler
from crawler.lib.analysis.constant import Constants
from crawler.lib.analysis.tencentnews import TencentNewsCrawler
from crawler.lib.common.globalvar import GlobalVariable
from crawler.lib.analysis.utils import MySqlProxy
from crawler.lib.read_mysql import SAConfiguration
from crawler.settings.user_config import SITE, MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD, MYSQL_DBNAME, MYSQL_PORT



class SARunner(object):

    def __init__(self):
        self.dbProxy = MySqlProxy(MYSQL_HOST,
                             MYSQL_PORT,
                             MYSQL_USER,
                             MYSQL_PASSWD,
                             MYSQL_DBNAME)
        GlobalVariable.init(self.dbProxy)
        args = self.parser()
        self.channels = args.channels  # 实例化通道
        self.entity_id = args.anaentities  # 实例化实体id
        self.keyword_list = self.keyworld_list(self.entity_id)  # 实例化关键词



    # TODO 根据媒体标识符对应爬虫项目的名字
    def runCrawler(self,channels):
        # 实例化channelDict
        channelDict =  SaveMysqlPipeline().channelDict(channels)[0]
        # 实例化self.crawler
        GlobalVariable.getChannelMgmt().channelDict = channelDict
        # 进行爬虫
        print('开始进行爬虫')
        crawler_code = SAConfiguration().inquire_type_id(channels)
        self.getCrawler(crawler_code)

    # TODO 分析文章热度
    def article_List(self,r):
        # 分析文章热度
        articleList = r
        ha = HeatAnalytics(self.dbProxy)
        ea = EmotionAnalysis(self.dbProxy)
        for article in articleList:
            article.classified_nature = ea.analysisSingleArticle(article.content)  # article_nature 返回数值~
            article.statistics.heat = ha.analysis(article, Constants.OBJECT_TYPE_ARTICLE, )  # 返回热度算法值
        self.save_article_List(articleList)


    # TODO 数据库存储
    def save_article_List(self,articleList):
        # 实例化爬虫
        channelDict = GlobalVariable.getChannelMgmt().channelDict
        entityDict = GlobalVariable.getEntityMgmt().entityDict
        db = SQLCrawler()
        ha = HeatAnalytics(self.dbProxy)
        # 写入全局文章表
        db.updateToArticleTable(articleList, Constants.TABLE_SA_ARTICLE, hasMetaInfo=True)
        self.dbProxy.commit()
        print('成功写入全局文章表')
        # 写入全局文章历史表
        db.updateToArticleHistoryTable(articleList, Constants.TABLE_SA_ARTICLE_HISTORY)
        self.dbProxy.commit()
        print('成功写入全局文章历史表')
        # 分析实体和事件并写入数据库 #===========
        db.identifyArticle(articleList,self.entity_id)
        self.dbProxy.commit()
        print('成功分析实体和事件并写入数据库')
        # 获取最近一批文章
        oldArticleList = db.fetchOldArticleList(channelDict['CHANNEL_ID'],articleList)
        # 重新获取统计信息
        statisticsArticleList = list()
        for article in oldArticleList:
            if TencentNewsCrawler(self.channels).crawlStatistics(article) is not False:
                article.statistics.heat = ha.analysis(article, Constants.OBJECT_TYPE_ARTICLE)
                statisticsArticleList.append(article)
        print('成功获取统计信息')
        # 更新旧文章到相关文章表   # ===============
        db.updateOldArticleToArticleTable(statisticsArticleList, Constants.TABLE_SA_ARTICLE)
        db.updateOldArticleToArticleHistoryTable(statisticsArticleList,
                                                     Constants.TABLE_SA_ARTICLE,
                                                     Constants.TABLE_SA_ARTICLE_HISTORY)

        for entityId in entityDict.keys():
            db.updateOldArticleToArticleTable(oldArticleList,
                                                  Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + entityId)
            db.updateOldArticleToArticleTable(oldArticleList,
                                                  Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + entityId,
                                                  True)
            db.updateOldArticleToArticleHistoryTable(oldArticleList,
                                                         Constants.TABLE_SA_ARTICLE + Constants.TABLE_NAME_DELIMITER + entityId,
                                                         Constants.TABLE_SA_ARTICLE_HISTORY + Constants.TABLE_NAME_DELIMITER + entityId)
            db.updateOldArticleToArticleHistoryTable(oldArticleList,
                                                         Constants.TABLE_SA_EVENT_ARTICLE + Constants.TABLE_NAME_DELIMITER + entityId,
                                                         Constants.TABLE_SA_EVENT_ARTICLE_HISTORY + Constants.TABLE_NAME_DELIMITER + entityId,
                                                         True)
        print('成功更新旧文章到相关文章表')
        print('数据库存储结束')
        self.dbProxy.commit()

    # TODO 获取关键词
    def keyworld_list(self,entity_id):
        keyword_list = SAConfiguration().inquire_entity_name(entity_id)
        return keyword_list

    # TODO 启动爬虫
    def getCrawler(self, channel):
        os.chdir(SITE)
        from scrapy import cmdline
        if channel == Constants.CRAWLER_CODE_SINA_WEIBO:
            args = "scrapy crawl wb".split()
            cmdline.execute(args)
        elif channel == Constants.CRAWLER_CODE_SINA_NEWS:
            args = "scrapy crawl xl".split()
            cmdline.execute(args)
        elif channel == Constants.CRAWLER_CODE_PEOPLE_NEWS:
            args = "scrapy crawl rmw".split()
            cmdline.execute(args)
        elif channel == Constants.CRAWLER_CODE_XINHUA:
            args = "scrapy crawl xhw".split()
            cmdline.execute(args)
        elif channel == Constants.CRAWLER_CODE_BAIDU_TIEBA:
            args = "scrapy crawl tb".split()
            cmdline.execute(args)
        elif channel == Constants.xq:
            args = "scrapy crawl xueqiu".split()
            cmdline.execute(args)
        elif channel == Constants.wbht:
            from crawler.spiders.wb import WBevent
            WBevent().run()
        else:
            print('没找到项目')


    # TODO 读取临时配置
    def parser(self):
        parser = argparse.ArgumentParser(description='public opinion')
        parser.add_argument('--target', dest='target',
                            choices=['crawler', 'analytics', 'sendemail', 'rpcserver', 'eventcrawler'],
                            help=u'目标，crawler或者analytics或者sendemail', default=False)
        parser.add_argument('--channels', dest='channels',
                            help=u'媒体标识列表，用逗号隔开，不指定则针对所有媒体', default=False)
        parser.add_argument('--anatype', dest='anatype',
                            help=u'分析类型，不指定则针对所有类型进行分析，\n\t\t支持heatwords, cluster, event, indicator, sensitive, '
                                 u'classify, articleextinct, eventextinct, back_track',
                            default=False)
        parser.add_argument('--anaentities', dest='anaentities',
                            help=u'实体识别列表，用逗号隔开，不指定则针对所有实体来分析', default=False)
        parser.add_argument('--emailtype', dest='emailtype', choices=['warning', 'report'],
                            help=u'邮件类型，预警邮件或者每日简报', default=False)
        args = parser.parse_args()
        return args


if __name__ == '__main__':
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    saRunner = SARunner()
    args = saRunner.parser()
    if args.target == 'crawler':
        saRunner.runCrawler(args.channels)



