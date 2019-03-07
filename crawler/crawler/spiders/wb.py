# -*- coding: utf-8 -*-
import datetime, json, re, urllib, scrapy,urllib3,os,requests,time,random
from lxml import etree
from conf import os_file
from retrying import retry
from crawler.items.items import Weibospider1Item, Weibodetailspider1Item, WeiborelayspiderItem
from crawler.items.save_mysql import SQLCrawler
from crawler.lib.CrawlerMonitor import SendEmail
from crawler.lib.analysis.eventidentificationanalysis import encodeText
from crawler.lib.common.utils import MySqlProxy
from crawler.lib.objectmodel.article import Article, ArticleStatistics, User_info
from crawler.lib.url_jm import Urlchuli
from crawler.sarunner import SARunner
from crawler.settings.user_config import NUM_PAGE, START_TIME, END_TIME, USERNAME, PASSWORD, MYSQL_HOST, MYSQL_PORT, \
    MYSQL_USER, MYSQL_PASSWD, MYSQL_DBNAME


class WbSpider(scrapy.Spider):
    name = 'wb'
    allowed_domains = ['weibo.cn','weibo.com']
    custom_settings = {
        'ITEM_PIPELINES': {'crawler.pipelines.Weibo1Pipeline': 300,
                           },
    }
    R = []      # 用户信息字典保存列表
    r = []      # 搜索爬取数据字典保存列表
    name_url_list = []  # 用户信息也url保存列表
    relay_url_list =[]
    L1 = 0
    L2 = 0
    L3 = 0
    time = SendEmail.getYesterday()
    if START_TIME is None:
        starttime = str(time).replace(u'-','')
    else:
        starttime = START_TIME
    if END_TIME is None:
        endtime = datetime.datetime.now().strftime('%Y%m%d')
    else:
        endtime = END_TIME


    def start_requests(self):
        print('正在登陆.....')
        login_url = "https://passport.weibo.cn/sso/login"
        formdata = {
            'username': USERNAME,
            'password': PASSWORD,
            'savestate': '1',
            'r': 'https: // weibo.cn /',
            'ec': '0',
            'pagerefer': 'https: // weibo.cn / pub /',
            'entry': 'mweibo',
            'wentry': '',
            'loginfrom': '',
            'client_id': '',
            'code': '',
            'qq': '',
            'mainpageflag': '1',
            'hff': '',
            'hfp': '',
        }
        yield scrapy.FormRequest(url=login_url, formdata=formdata, callback=self.parse_login)

    def parse_login(self, response):
        # print("正在登陆....")
        # 对响应体判断是否登录成功
        json_res = json.loads(response.text)
        if json_res["retcode"] == 50060000:
            print('出现验证码，请在常用登陆地运行')
            print(json_res["data"]['errurl'])
        elif json_res["retcode"] == 20000000:
            print("登陆成功")
            args = SARunner().parser()
            keyword_list = SARunner().keyworld_list(args.anaentities)
            keyword = keyword_list.replace(u"|", "~", )
            seek_url = "https://weibo.cn/search/"
            fd = {
                'advancedfilter': '1',
                'keyword': keyword,
                'nick': '',
                'starttime': self.starttime,
                'endtime': self.endtime,
                'sort': 'time',
                'smblog': '搜索'
            }
            print('搜索关键词：', keyword)
            yield scrapy.FormRequest(url=seek_url,
                                     formdata=fd,
                                     callback=self.parse_info,)
        else:
            print('登陆失败！')

    def parse_info(self, response):
        weibo_list = response.xpath("//div[@class='c' and @id]")
        for weibo in weibo_list:
            item = Weibospider1Item()
            div = weibo.xpath("./div")
            if len(div) == 1:
                # 微博类型
                item["category"] = "无图原创"
                item["author"] = weibo.xpath("./div/a[@class='nk']/text()").extract_first()
                item['author_id'] = weibo.xpath("./div[1]/a[@class='nk']/@href").extract_first()
                item["content"] = weibo.xpath("./div/span[@class='ctt']").xpath('string(.)').extract()
                img = weibo.xpath("./div/span[@class='ctt']/img/@src")
                if len(img) == 1:
                    item["content"] = weibo.xpath(
                        "./div/text()|./div/span[@class='ctt']//text()").extract()
                item["dianzan"] = weibo.xpath("./div/a/text()").extract()[-4]
                item["relay"] = weibo.xpath("./div/a/text()").extract()[-3]
                item["comment"] = weibo.xpath("./div/a[@class='cc']/text()").extract_first()
                item["comment_url"] = weibo.xpath("./div/a[@class='cc']/@href").extract_first()
                item["send_time"] = weibo.xpath("./div/span[@class='ct']/text()").extract_first()
                item["reason"] = None
                item["img_url"] = None
                item['reason_name'] = None
                item['reason_id'] = None

            elif len(div) == 2:
                item["category"] = ""
                item["content"] = weibo.xpath("./div[1]/span[@class='ctt']").xpath('string(.)').extract()
                img = weibo.xpath("./div/span[@class='ctt']/img/@src")
                if len(img) == 1:
                    item["content"] = weibo.xpath(
                        "./div[1]/text()|./div[1]/span[@class='ctt']//text()").extract()
                item["relay"] = weibo.xpath("./div[2]/a/text()").extract()[-3]
                item["comment"] = weibo.xpath("./div[2]/a[@class='cc']/text()").extract_first()
                item["reason"] = None
                img = weibo.xpath("./div[2]//img[@class='ib']/@src")
                if len(img) == 0:
                    # 无图转发
                    item['category'] = "无图转发"
                    item["author"] = weibo.xpath("./div/span[@class = 'cmt']/a/text()").extract_first()
                    item['author_id'] = weibo.xpath("./div[1]/a[@class='nk']/@href").extract_first()
                    item['reason_name'] = weibo.xpath("./div[1]/span[@class = 'cmt']/a/text()").extract_first()
                    item['reason_id'] = weibo.xpath("./div[1]/span[@class = 'cmt']/a/@href").extract_first()
                    item["dianzan"] = weibo.xpath("./div[2]/a/text()").extract()[-4]
                    item["reason"] = weibo.xpath("./div[2]/text()|./div[2]//span[@class='kt']/text()").extract()
                    item["comment_url"] = weibo.xpath("./div[2]/a[@class='cc']/@href").extract_first()
                    item["img_url"] = None
                    item["send_time"] = weibo.xpath("./div[2]/span[@class='ct']/text()").extract_first()

                else:
                    # 有图原创
                    item['category'] = "有图原创"
                    item["author"] = weibo.xpath("./div/a[@class='nk']/text()").extract_first()
                    item['author_id'] = weibo.xpath("./div[1]/a[@class='nk']/@href").extract_first()
                    item['reason_name'] = None
                    item['reason_id'] = None
                    item["dianzan"] = weibo.xpath("./div[2]/a/text()").extract()[-4]
                    item["img_url"] = weibo.xpath("./div[2]//img[@class='ib']/@src").extract_first()
                    item["comment_url"] = weibo.xpath("./div[2]/a[@class='cc']/@href").extract_first()
                    item["send_time"] = weibo.xpath("./div[2]/span[@class='ct']/text()").extract_first()

            else:
                # len(div) == 3
                item["category"] = "带图片转发"
                item["author"] = weibo.xpath("./div[1]/a[@class='nk']/text()").extract_first()
                item['author_id'] = weibo.xpath("./div[1]/a[@class='nk']/@href").extract_first()
                item['reason_name'] = weibo.xpath("./div[1]/span[@class = 'cmt']/a/text()").extract_first()
                item['reason_id'] = weibo.xpath("./div[1]/span[@class = 'cmt']/a/@href").extract_first()
                item["content"] = weibo.xpath("./div[1]/span[@class = 'ctt']").xpath('string(.)').extract()
                img = weibo.xpath("./div[1]/span[@class='ctt']/img/@src")
                if len(img) == 1:
                    item["content"] = weibo.xpath(
                        "./div[1]/text()|./div[1]/span[@class='ctt']//text()").extract()
                item["send_time"] = weibo.xpath("./div[3]/span[@class='ct']/text()").extract_first()
                item["dianzan"] = weibo.xpath("./div[3]/a/text()").extract()[-4]
                item["relay"] = weibo.xpath("./div[3]/a/text()").extract()[-3]
                item["comment"] = weibo.xpath("./div[3]/a[@class='cc']/text()").extract_first()
                item["comment_url"] = weibo.xpath("./div[3]/a[@class='cc']/@href").extract_first()
                item["img_url"] = weibo.xpath("./div[2]//img[@class='ib']/@src").extract_first()
                item["reason"] = weibo.xpath("./div[3]/text()|./div[3]//span[@class='kt']/text()").extract()
            item['relay_url'] = ''

            item['TID'] = re.findall(r'uid=.{1,}&', item["comment_url"])[0][4:-1]
            a = weibo.xpath("//a[@class='nk']/@href").extract()
            yield item
            article = Article(tid=item['TID'],channel_id =9,content=item['content'],publish_datetime=item['send_time'],url=item['comment_url'],title=item['content'][0:100],
                              author_id=item['author_id'],author_name=item['author'])
            article.statistics = ArticleStatistics(tid=item['TID'],channel_id =9,reply_count=item['comment'],forward_count=item['relay'],like_count=item['dianzan'],)
            if int(item['relay'])> 0:
                self.relay_url_list.append(item['relay_url'])

            self.r.append(article)
            self.name_url_list.append(a)


        num_page = response.xpath("//div[@id='pagelist']/form/div/text()").extract()
        num_page = [i.replace(u"\xa0", "", ) for i in num_page]
        num_page = [i for i in num_page if len(i) > 0][0]
        num_page = re.findall(r'\d+', num_page)

        print('正在爬取第', num_page[0], '页',num_page[1])
        max_page = NUM_PAGE
        if max_page is None:
            max_page = int(num_page[1])
        if int(num_page[0]) == max_page:
            L = []
            for L1 in self.name_url_list:
                L += L1
            for url_1 in L:
                with open(os_file.a + '\\crawler_url.txt', 'a', encoding='utf-8') as f:
                    f.write(url_1 + "\n")

            print('页数上限,搜索页数据爬取完毕')
            print('爬虫结束，开始热度分析')
            SARunner().article_List(self.r)

            print("爬取微博数:", len(self.r))
            # print('开始爬取用户详情页数据,一共有', self.L2, '个非重复用户')
            # 爬取作者头像 id 关注 粉丝
            with open(os_file.a + '\\crawler_url.txt', 'r', encoding='utf-8') as f:
                urls = f.readlines()
                # 获取待爬个数
                # 去重
                L2 = {}.fromkeys(urls).keys()
                self.L2 = len(L2)
                print('开始爬取用户详情页数据,一共有', self.L2, '个非重复用户')
                for url in L2:
                    yield scrapy.FormRequest(url=url, callback=self.parse_info_detail,dont_filter=True)
        else:
            next_url = response.xpath("//a[text() = '下页']/@href").extract_first()
            next_url = urllib.parse.urljoin(response.url, next_url)
            yield scrapy.Request(
                next_url,
                callback=self.parse_info,
                dont_filter=True
            )

    # TODO 详情页数据
    def parse_info_detail(self, response):
        item = Weibodetailspider1Item()
        self.L3 +=1
        print(self.L3,self.L2)
        item['author_url'] = response.url
        item['head_url'] = response.xpath("//img[@class='por']/@src").extract_first()
        item['fans'] = response.xpath("//div[@class='tip2']/a[2]/text()").extract_first()
        item['Concern'] = response.xpath("//div[@class='tip2']/a[1]/text()").extract_first()
        item['author_url'] = item['author_url'].replace(u"weibo.cn", "weibo.com", )
        if '/u/' in item['author_url']:
            item['tid'] = re.findall(r'\/u\/.{1,}\%0A', item['author_url'])[0][3:-3]
        else:
            item['tid'] = re.findall(r'com\/.{1,}\%0A', item['author_url'])[0][4:-3]
        item["fans"] = re.findall(r"\d+\.?\d*", item["fans"])[0]
        item["Concern"] = re.findall(r"\d+\.?\d*", item["Concern"])[0]
        item['user_name'] = response.xpath("//div[@class='ut']/span[@class='ctt'][1]/text()[1]").extract_first()
        item['user_name'] = re.split('\xa0', item['user_name'])[0]
        yield item
        print(item)

        userinfo = User_info(item['tid'],9,item['author_url'],fans_count=item['fans'],Concern_count=item['Concern'],logo_url=item['head_url'],user_name = item['user_name'])
        self.R.append(userinfo)
        if self.L3 == self.L2:
            print('详情页爬取结束,开始保存数据库')
            # 删除临时url文件
            my_file = [os_file.a + '\\crawler_url.txt',]
            for file in my_file:
                if os.path.isfile(file):
                    os.remove(file)
                else:
                    print('no such file:%s' % my_file)
            SQLCrawler().updateToUSERTable(self.R)
            print('成功写入用户信息表')
            print('开始爬取转发页url')
            # self.run_parse_relay()

    def run_parse_relay(self):
        headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
            "Host": "weibo.com",
            "Referer": "https://passport.weibo.com/visitor/visitor?entry=miniblog&a=enter&url=https%3A%2F%2Fweibo.com%2Faj%2Fv6%2Fmblog%2Finfo%2Fbig%3Fajwvr%3D6%26id%3D4346465335813251&domain=.weibo.com&ua=php-sso_sdk_client-0.6.28&_rand=1551855605.3115",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36",
        }
        url = 'https://weibo.com/aj/v6/mblog/info/big?ajwvr=6&id=4346465335813251'
        # for relay_url in self.relay_url_list:
        yield scrapy.Request(url=url,callback=self.parse_relay,headers=headers,
        dont_filter=True)

    def parse_relay(self,response):
        print(response.text)


    """
    爬取微博转发思路：
        1.爬取所有关键词微博 将有转发数的微博用于传递
        2.将传递的微博进行爬取转发页（爬取转发人id，name，url） 此条微博为源微博
        4.构造电脑版的转发页url
        5.对前几位转发
        6.传递表单进行搜索源微博
        7.找到转发的微博进行爬取转发页
                重复2~6步
        结束：直至4步不符合要求   或者转发后微博转发数为0
            
        转发链：源微博微博tid 对应转发人id，name，转发内容，        
    """



class WBevent(object):
    def __init__(self):
        self.host = MYSQL_HOST
        self.port = MYSQL_PORT
        self.user = MYSQL_USER
        self.passward = MYSQL_PASSWD
        self.db = MYSQL_DBNAME
        self.Table = 'sa_weibo_topic'
        self.keywordlist = None
        self.headers = {
            "Accept": "*/*",
            # 'Accept': 'application/json;',
            'Connection': 'close',
            "Accept-Language": "zh-CN,zh;q=0.9",
            # "Connection": "keep-alive",
            "Host": "s.weibo.com",
            "Referer": "https://s.weibo.com/topic?q=%E4%B8%AD%E5%B1%B1%E5%A4%A7%E5%AD%A6&pagetype=topic&topic=1&Refer=weibo_topic&page=3",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.170 Safari/537.36",
        }

    def parse_item(self,html):
        a_list = html.xpath("//a[@class ='name']")
        list = []
        for a in a_list:
            item = {}
            # 话题
            item['title'] = a.xpath("./text()")[0]
            b = Urlchuli(item['title'], 'utf-8')
            item['title'] = b.url_jm()
            data = a.xpath("./../../p[2]/text()")[0]
            item['debate'] = re.findall(r'.{1,}讨论', data)[0][:-2]
            if '万' in item['debate']:
                x = re.findall(r'.{1,}万', item['debate'])[0][:-1]
                item['debate'] = float(x) * 10000
            elif '亿' in item['debate']:
                x = re.findall(r'.{1,}亿', item['debate'])[0][:-1]
                item['debate'] = float(x) * 100000000
            item['read'] = re.findall(r' .{1,}阅读', data)[0][1:-2]
            if '万' in item['read']:
                x = re.findall(r'.{1,}万', item['read'])[0][:-1]
                item['read'] = float(x) * 10000
            elif '亿' in item['read']:
                x = re.findall(r'.{1,}亿', item['read'])[0][:-1]
                item['read'] = float(x) * 100000000
            item['url'] = a.xpath('./@href')[0]
            print(item)
            list.append(item)
        return list

    # 保存数据库
    def dbmysql(self,item_list,keyword):
        dbProxy = MySqlProxy(self.host, self.port, self.user, self.passward, self.db)
        sql = """INSERT INTO %s(TITLE,%s ADD_DATETIME,DEBATE_COUNT, READ_COUNT, `DATE`, URL, SCHOOL)
                                   VALUES %s ON DUPLICATE KEY UPDATE ADD_DATETIME=VALUES(ADD_DATETIME),
                                   DEBATE_COUNT=VALUES(DEBATE_COUNT),READ_COUNT=VALUES(READ_COUNT),URL=VALUES(URL),
                                   SCHOOL=VALUES(SCHOOL),CREATE_DETETIME=VALUES(CREATE_DETETIME)"""
        for item in item_list:
            add_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            date =  datetime.datetime.now().strftime('%Y-%m-%d')
            item['title'] = item['title'].replace(u"\"", "\'", )
            # 判读是否为新话题
            create = self.look_create(dbProxy,item['title'],keyword)
            if create is None:
                create_ = ''
                create_detetime = ''
                d =''
            else:
                create_ = 'CREATE_DETETIME ,'
                create_detetime = str(create)
                d = ','
            # 判断是否需要预警
            valueList = list()
            self.warning(dbProxy,item['title'],item['read'],url = item['url'],keyword = keyword)
            valueList.append('("%s","%s" %s "%s",%d,%d,"%s","%s","%s")' % (
                    encodeText(item['title']),
                    create_detetime,
                    d,
                    add_time,
                    int(item['debate']),
                    int(item['read']),
                    date,
                    item['url'],
                    keyword,
                ))
            tmp = sql % (self.Table,create_,','.join(valueList))
            dbProxy.execute(tmp)
            dbProxy.commit()
        print('查询话题是否被删除')
        self.look_delete(dbProxy,keyword)
        dbProxy.close()


    # 查询话题产生时间
    def look_create(self,dbProxy,title,keyword):
        # 查询标题昨天是否存在 存在则NONE
        day = 1
        school = keyword
        pastdate = SendEmail.getYesterday(day=day)
        while True:
            if day>=7:
                # '超过7天没有更新数据'
                return
            sql1 = """SELECT `DATE` FROM %s where SCHOOL="%s" and `DATE`='%s'"""
            dbProxy.execute(sql1 % (self.Table,school, pastdate))
            n = dbProxy.fetchall()
            if len(n) > 0:
                break
            else:
                day += 1
        date = datetime.datetime.now().strftime('%Y-%m-%d')
        sql1 = """SELECT TITLE FROM %s where SCHOOL="%s" and `DATE`='%s' and TITLE="%s" """
        dbProxy.execute(sql1 % (self.Table,school, pastdate,title))
        n = dbProxy.fetchall()
        if len(n) > 0:
            create_detetime = None
        else:
            create_detetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return create_detetime

    # 查询话题删除时间
    def look_delete(self,dbProxy,keyword):
        # 查询数据库判读昨天是否有数据 如果没有数据则延长一天  直至有数据
        day = 1
        school = keyword
        pastdate = SendEmail.getYesterday(day=day)
        while True:
            if day>=7:
                # '超过7天没有更新数据'
                return
            sql1 = """SELECT `DATE` FROM %s where SCHOOL="%s" and `DATE`='%s'"""
            dbProxy.execute(sql1 % (self.Table,school, pastdate))
            n = dbProxy.fetchall()
            if len(n)>0:
                break
            else:
                day += 1
        # 查询数据库中昨天所有话题
        date = datetime.datetime.now().strftime('%Y-%m-%d')
        sql1 = """SELECT TITLE FROM %s where SCHOOL="%s" and `DATE`='%s'"""
        dbProxy.execute(sql1 % (self.Table,school,pastdate))
        titlelist = []
        alldata = dbProxy.fetchall()
        for s in alldata:
            titlelist.append(s[0])

        for t in titlelist:
            t = t.replace(u"\"", "\'", )
            # 把数据库中昨天所有标题 对应今天的日期  查询是否有  有的话说明话题还在，反之话题消失
            selectSql = """SELECT `DATE` FROM %s where title="%s" and `DATE`='%s'"""
            sql2 = selectSql % (self.Table,t,date)
            dbProxy.execute(sql2)
            n = dbProxy.fetchall()
            if len(n) > 0:
                delete_detetime = None
            else:
                delete_detetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sql = """UPDATE %s SET DELETE_DETETIME="%s" WHERE TITLE="%s" and `DATE`="%s" and SCHOOL="%s" """
                tmp = (self.Table,delete_detetime,t,pastdate,school)
                dbProxy.execute(sql % tmp)
                dbProxy.commit()
        dbProxy.close()

    # 警告
    def warning(self,dbProxy,title,read,url=None,keyword = None):
        school = keyword
        day = 1
        pastdate = SendEmail.getYesterday(day=day)
        while True:
            if day>=7:
                # '超过7天没有更新数据'
                return
            sql1 = """SELECT `DATE` FROM %s where SCHOOL="%s" and `DATE`='%s'"""
            dbProxy.execute(sql1 % (self.Table,school, pastdate))
            n = dbProxy.fetchall()
            if len(n) > 0:
                break
            else:
                day += 1
        # 查询数据库昨天的阅读数是否过1万
        selectSql = """SELECT READ_COUNT FROM %s where title="%s" and `DATE`='%s'"""
        sql2 = selectSql % (self.Table,title,pastdate,)
        dbProxy.execute(sql2)
        n = dbProxy.fetchall()
        if len(n)>0:
            if int(n[0][0]) >=10000:   # 昨天的阅读量
                # print('不警告')
                pass
            else:
                content = '警告！' + ' 昨天阅读量：' + str(n[0][0]) + ' 今天阅读量：' + str(read) + ' 微博话题为：'+ title +'  话题链接为：'+url
                title = '微博话题超过一万预警'
                if int(read) >=10000:    # 今天的阅读量
                    print(title)
                    SendEmail().send(title =title ,content=content)
                else:
                    # print('不警告')
                    pass

    @retry(stop_max_attempt_number=7,wait_random_min=3000, wait_random_max=5000) # 最大尝试7次 最小3秒最大5秒的等待
    def requests_run(self,keyword,page):
        url = 'http://s.weibo.com/topic?q={}&pagetype=topic&topic=1&Refer=weibo_topic&page={}'
        response = requests.get(url.format(keyword, page), verify=False, headers=self.headers)
        return response

    def run(self):
        if self.keywordlist is None:
            keywordlist = SendEmail().school_list()
        else:
            keywordlist = self.keywordlist
        for keyword in keywordlist:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            item_list=[]
            url = 'http://s.weibo.com/topic?q={}&pagetype=topic&topic=1&Refer=weibo_topic&page={}'
            response = self.requests_run(keyword,1)
            html = etree.HTML(response.content)
            max_page = html.xpath("//ul[@class='s-scroll']/li/a/text()")[-1]
            max_page = int(re.findall(r"\d+\.?\d*", max_page)[0])
            i = 1
            while i <= max_page:
                response = self.requests_run(keyword, i)
                html = etree.HTML(response.content)
                list = self.parse_item(html)
                item_list.extend(list)
                i += 1
            print('爬虫结束')
            print('正在写入数据库。。。')
            self.dbmysql(item_list,keyword)
            print("成功写入数据库")

