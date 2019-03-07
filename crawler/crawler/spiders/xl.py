# -*- coding: utf-8 -*-
import re
import urllib
import scrapy

from crawler.items.save_mysql import SaveMysqlPipeline
from crawler.items.xlitem import XinLangspider1Item, xw_type
from crawler.lib.objectmodel.article import Article
from crawler.lib.url_jm import Urlchuli
from crawler.sarunner import SARunner
from crawler.settings.user_config import XL_MAX_PAGE

# 可以多关键字搜索
class XlSpider(scrapy.Spider):
    name = 'xl'
    allowed_domains = ['sina.com']
    custom_settings = {
        'ITEM_PIPELINES': {'crawler.pipelines.XinLangPipeline': 300, },
    }
    r =[]
    R = []
    MAX_PAGE = XL_MAX_PAGE
    args = SARunner().parser()
    keyword_list = SARunner().keyworld_list(args.anaentities)
    if '|' in keyword_list:
        keyword_list = keyword_list.replace(u"|", "~", )
    a = Urlchuli(keyword_list, 'gbk')
    one = a.url_bm()
    start_urls = ['http://search.sina.com.cn/?c=news&q={}&range=all&time=w&stime=&etime=&num=10'.format(one)] #  time  w：一周,m：月 h:一小时 d：一天

    def parse(self, response):
        a = response.url
        if self.MAX_PAGE is None:
            MAX_PAGE = response.xpath("//span[@class ='pagebox_cur_page']/text()")
        else:
            MAX_PAGE = self.MAX_PAGE

        if  'https://s.weibo.com/weibo/' in str(a):
            print('搜索失败，请重新搜索')
            raise Exception(f'搜索失败，请重新搜索')

        else:
            div_list = response.xpath("//div[@class='box-result clearfix']")
            for div in div_list:
                data = div.xpath(".//p[@class = 'content']").xpath('string(.)').extract()
                title = div.xpath(".//h2/a/text()").extract()
                title = ''.join(title)
                href = div.xpath(".//h2/a/@href").extract_first()
                time = div.xpath(".//span[@class = 'fgray_time']/text()").extract_first()
                time =  re.split(r' ',time)
                time = time[-2]+' '+time[-1]
                self.r.append(href)
                yield scrapy.Request(
                    url = href,
                    meta={"intro": data,'href':href,'time':time,'title':title},
                    callback=self.parse_main,
                    dont_filter=True )
            next_url = response.xpath("//a[@title = '下一页']/@href").extract_first()
            next_url = urllib.parse.urljoin(response.url, next_url)
            page = response.xpath("//span[@class = 'pagebox_cur_page']/text()").extract_first()
            if int(page) is int(MAX_PAGE):
                print('页数上限')
            else:
                yield scrapy.Request(
                    next_url,
                    callback=self.parse,
                    dont_filter=True
                )



    def parse_main(self, response):
        item = XinLangspider1Item()
        item['intro'] = str(response.meta["intro"]).replace(u"...", "", ).replace(u"']", "", ).replace(u"['", "", )
        item['href'] = response.meta["href"]
        item['time'] = response.meta['time']
        item['title_main'] = response.meta['title']
        item['article'] = response.xpath("//div[@id = 'artibody']//p//text()|//div[@id = 'article']//p//text()").extract()
        item['source'] = response.xpath("//a[@class = 'source ent-source']/text()|//span[@class = 'source ent-source']/text()").extract()
        item['TID'] = None

        a = re.findall(r'http.{1,}sina',item['href'])[0][7:-5]
        a = a.replace(u"/", "", )

        if a in 'k':
            item['TID'] = re.findall(r'article_.{1,}_', item['href'])[0][8:-1]
        else:
            item['TID'] = re.findall(r'-ih.{1,}shtml', item['href'])[0][1:-6]

        if a  in xw_type.cs:
            item['source'] = response.xpath("//span[@id = 'art_source']/text()").extract()
            item['article'] = response.xpath("//div[@class = 'article-body main-body']//p//text()").extract()
        elif a in xw_type.ss:
            item['source'] = response.xpath("//a[@class = 'source content-color']/text()|//span[@class ='source content-color']/text()").extract()
        elif a in xw_type.xw:
            item['article'] = response.xpath("//div[@id = 'article']").xpath('string(.)').extract()
            item['source'] = response.xpath("//a[@class = 'source']/text()").extract()
        elif a in xw_type.bk:
            item['source'] = '新浪博客'
            item['article'] = response.xpath("//div[@id='sina_keyword_ad_area2']/div/font|//div[@id='sina_keyword_ad_area2']/p/font").xpath('string(.)').extract()

        # 手机版网站
        if len(item['article'])==0 and len(item['source']) == 0 :
            item['article'] = response.xpath(
                "//section[@class = 'art_pic_card art_content']/p//text()").extract()
            item['source'] = response.xpath(
                "//h2[@class ='weibo_user']/text()").extract()

        yield item
        article = Article(tid=item['TID'], channel_id=3, title=item['title_main'], content=item['article'],
                          publish_datetime=item['time'], url=item['href'], author_name=item['source']
                          , digest=item['intro'])

        self.R.append(article)
        if len(self.r) == len(self.R):
            print(len(self.R))
            print('开始保存数据库')
            print('爬虫结束，开始热度分析')
            SARunner().article_List(self.R)


