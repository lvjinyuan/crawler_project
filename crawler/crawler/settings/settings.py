

BOT_NAME = 'crawler'
SPIDER_MODULES = ['crawler.spiders']
NEWSPIDER_MODULE = 'crawler.spiders'

# 运行速度
DOWNLOAD_DELAY = 2


# redis
# scrapy 调度器
# SCHEDULER = "scrapy_redis.scheduler.Scheduler"
# 去重组件
# DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
# 共享的爬虫队列
# REDIS_URL = 'redis://root:@192.168.31.145:6379'
# 设置为为True则不会清空redis里的dupefilter和requests队列
# SCHEDULER_PERSIST = True
# 设置重启爬虫时是否清空爬取队列
# SCHEDULER_FLUSH_ON_START=True




import datetime
to_day = datetime.datetime.now()
log_file_path = "../log/mySpider.log_{}_{}_{}.log".format(to_day.year,to_day.month,to_day.day)
LOG_FILE = log_file_path


# CRITICAL - 严重错误# ERROR - 一般错误# WARNING - 警告信息# INFO - 一般信息 # DEBUG - 调试信息
LOG_LEVEL = "WARNING"


USER_AGENTS_LIST=[ "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
                   "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
                   "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
                   "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
                   "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
                   "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
                   "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
                   "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5" ]

DEFAULT_REQUEST_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Referer': 'https://passport.weibo.cn/signin/login?entry=mweibo&r=https%3A%2F%2Fweibo.cn%2F&backTitle=%CE%A2%B2%A9&vt=',
    'Origin': 'https://passport.weibo.cn',
    'Connection': 'keep-alive',
}

ROBOTSTXT_OBEY = False


DOWNLOADER_MIDDLEWARES = {
    'crawler.middlewares.RandomUserAgentMiddleware':543,
}


# ITEM_PIPELINES = {
#     'scrapy_redis.pipelines.RedisPipeline': 300
# }


# IMAGES_STORE = 'E:\pic2'    # 图片下载位置
# IMAGES_EXPIRES = 90     #90天内抓取的都不会被重抓
# IMAGES_MIN_HEIGHT = 110    # 过滤图片下载
# IMAGES_MIN_WIDTH = 110


