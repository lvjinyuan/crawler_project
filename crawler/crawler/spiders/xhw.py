# -*- coding: utf-8 -*-
import datetime,json,re,urllib,scrapy
from copy import deepcopy

from crawler.items.save_mysql import SaveMysqlPipeline
from crawler.items.items import XHWspider1Item
from crawler.lib.objectmodel.article import Article
from crawler.sarunner import SARunner
from crawler.settings.user_config import XHW_MAX_PAGE


class XHWSpider(scrapy.Spider):
    name = 'xhw'
    allowed_domains = ['so.news.cn']
    custom_settings = {
        'ITEM_PIPELINES': {'crawler.pipelines.RenMingPipeline': 300, },
    }
    args = SARunner().parser()
    keyword_list = SARunner().keyworld_list(args.anaentities)
    keyword_list = re.split(r'\|', keyword_list)
    p = len(keyword_list)
    page = 1
    R=[]
    r=[]

    def start_requests(self):
        print('正在搜索...')
        keyWordAll = self.keyword_list[0]
        if self.p >1:
            keyWordOne = self.keyword_list[1:]
            keyWordOne = '+'.join(keyWordOne)
            url = 'http://so.news.cn/getNews?keyWordAll={}&keyWordOne={}&keyWordIg=&searchFields=0&sortField=0&url=&senSearch=1&lang=cn&keyword={}&curPage=1'.format(keyWordAll,keyWordOne,keyWordAll)
            print(url)
        else:
            url ='http://so.news.cn/getNews?keyword={}&curPage=1&sortField=0&searchFields=1&lang=cn'.format(keyWordAll)
        yield scrapy.Request(url=url,callback=self.parse_seek,dont_filter=True)

    def parse_seek(self, response):
        html = json.loads(response.text)
        data_list = html['content']['results']
        max_page = html['content']['pageCount']
        for data in data_list:
            item = XHWspider1Item()
            item['title'] = data['title'].replace(u'<font color=red>', '').replace(u'</font>', '').replace(u'&nbsp', '').replace(u'&quot', '').replace(u'\u3000', '')
            # item['title'] = item['title'].replace(u'<font color=red>', '')
            item['time'] = data['pubtime']
            item['href'] = data['url']
            item['intro'] =data['des']
            if 'xhwkhdapp' in item['href']:
                continue
            if item['intro'] is not None:
                item['intro'] = ''.join(item['intro'])
                item['intro'] = item['intro'].replace(u'<font', '').replace(u'color=red>', '').replace(u'</font>', '')
            item['source'] = data['sitename']
            self.R.append(item['href'])
            yield scrapy.Request(url=item['href'], callback=self.parse_main, dont_filter=True,meta={'item':deepcopy(item)})


        if XHW_MAX_PAGE is not None:
            max_page = XHW_MAX_PAGE

        if self.page == max_page:
            print('页数上限')
        else:
            self.page += 1
            a = re.compile('&curPage=\d+')
            next_url = a.sub('&curPage={}'.format(self.page), response.url)
            yield scrapy.Request(url=next_url, callback=self.parse_seek, dont_filter=True)

    def parse_main(self,response):
        item = response.meta['item']
        item['article'] = response.xpath("//div[@class ='p-right left']//div[@id='p-detail']//p|"
                                        "//div[@id='content']//p|"
                                         "//div[@class='content']//p|"
                                         "//div[@class ='contant clearfix']/div[@class ='xl']//p|"
                                         "//div[@id ='Content']//p|"
                                         "//div[@class ='zj_left']/div[@class ='zj_nr']//p|"
                                         "//td[@class='text_con_16_33']//p|"
                                         "//div[@class ='content pack']//p|"
                                         "//div[@class = 'article']//p|"
                                         "//div[@class ='main-content-box']//p|"
                                         "//div[@id ='nr_wz']//p"
                                        ).xpath('string(.)').extract()
        item['TID'] = re.findall(r'c_.{1,}htm', item['href'])[0][2:-4]
        yield item
        article = Article(tid=item['TID'], channel_id=11, title=item['title'], content=item['article'],
                          publish_datetime=item['time'], url=item['href'], author_name=item['source']
                          , digest=item['intro'])
        self.r.append(article)
        if len(self.r) == len(self.R):
            print(len(self.r))
            print('爬虫结束，开始热度分析')
            SARunner().article_List(self.r)

