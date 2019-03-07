# -*- coding: utf-8 -*-
import re,urllib,scrapy
from crawler.items.items import RMWspider1Item
from crawler.sarunner import SARunner
from crawler.settings.user_config import RMW_MAX_PAGE
from crawler.lib.objectmodel.article import Article
# 无法多关键字搜索



class RMWSpider(scrapy.Spider):
    name = 'rmw'
    allowed_domains = ['people.com','people.com.cn.']
    custom_settings = {
        'ITEM_PIPELINES': {'crawler.pipelines.RenMingPipeline': 300, },
    }
    r = []
    args = SARunner().parser()
    keyword_list = SARunner().keyworld_list(args.anaentities)
    keyword_list = re.split(r'\|',keyword_list)
    R = []
    R2 = 0
    R1 = len(keyword_list)

    headers = {
        'Location':'news/getNewsResult.jsp',
        'Server':'Apache-Coyote/1.1',
    }


    def start_requests(self):
        print('爬取关键词',self.keyword_list)
        for keyword in self.keyword_list:
            keyword = keyword.encode('gbk')
            print('正在搜索...')
            url = 'http://search.people.com.cn/cnpeople/search.do'
            formdata={
                'siteName': 'news',
                'pageNum': '1',
                'facetFlag': 'true',
                'nodeType': 'belongsId',
                'nodeId': '0',
                'keyword': keyword,
            }
            yield scrapy.FormRequest(url=url, formdata=formdata,headers=self.headers, callback=self.parse_seek,dont_filter=True)

    def parse_seek(self, response):
        if response.url == 'http://search.people.com.cn/cnpeople/news/error.jsp':
            print('搜索失败')
        else:
            print(response.url)
            ul_list = response.xpath("//div[@class='fr w800']/ul")
            for ul in ul_list:
                item = {}
                item['title'] = ul.xpath("./li[1]//a").xpath('string(.)').extract()
                item['time'] = ul.xpath("./li[3]/text()").extract_first()
                item['intro']= ul.xpath("./li[2]").xpath('string(.)').extract()
                item['href'] = ul.xpath("./li[1]//a/@href").extract_first()
                self.R.append(item['href'])
                yield scrapy.Request(
                    item['href'],
                    callback=self.parse_main,
                    meta={'title':item['title'],'time':item['time'],'intro':item['intro'],'href':item['href']},
                    dont_filter=True
                )
            next_url = response.xpath("//a[text() = '下一页']/@href").extract_first()
            next_url = urllib.parse.urljoin(response.url, next_url)

            num_page = response.xpath("//div[@class = 'show_nav_bar']/text()").extract()
            try:
                num_page = ''.join(num_page)
                num_page =  re.findall(r"\d+", num_page)[0]
            except IndexError as e:
                pass
            self.R2 +=1

            if RMW_MAX_PAGE is not None:
                if int(num_page) == RMW_MAX_PAGE:
                    if self.R1 == self.R2:
                        print('页数上限')
                else:
                    yield scrapy.Request(
                        next_url,
                        callback=self.parse_seek,
                        dont_filter=True
                    )
            else:
                yield scrapy.Request(
                    next_url,
                    callback=self.parse_seek,
                    dont_filter=True
                )


    def parse_main(self, response):
        item = RMWspider1Item()
        item['title'] = response.meta['title'][0]
        item['time'] = response.meta['time']
        item['intro'] = response.meta['intro'][0].replace('[','',1).replace(']','',)
        item['href'] = response.meta['href']
        item['TID'] = re.findall(r'/c.{1,}html', item['href'])[0][1:-5]
        if 'people' in item['TID']:
            item['TID'] = re.findall(r'/c.{1,}', item['TID'])[0][1:]
        item['source'] = response.xpath("//div[@class = 'artOri']/a/text()|"
                                        "//div[@class='box01']//a/text()|"
                                        "//div[@class='text_c']/p//a/text()|"
                                        "//div[@class = 'msgBox']//a/text()|"
                                        "//div[@class = 'page_c']/div[@class = 'fr']/a/text()|"
                                        "//div[@class = 'w1000 p2']//a/text()|"
                                        "//div[@class = 'p2j_text fl']/h2/a/text()").extract_first()
        item['article'] = response.xpath("//div[@id='rwb_zw']//p|"
                                         "//div[@class='show_text']//p|"
                                         "//div[@class='artDet']//p|"
                                         "//div[@class='text_con clearfix']//p|"
                                         "//div[@class = 'content clear clearfix']//p|"
                                         "//div[@id = 'p_content']//p|"
                                         "//div[@class = 'box_con']//p|"
                                         "//div[@class = 'text_show']//p|"
                                         "//div[@class = 'gray box_text']//p|"
                                         "//div[@class = 'text_box clearfix']//p").xpath('string(.)').extract()
        yield item
        article = Article(tid=item['TID'],channel_id = 5 ,title = item['title'],content = item['article'],
                          publish_datetime =item['time'] ,url = item['href'],author_name = item['source']
                          ,digest = item['intro'])
        self.r.append(article)
        if len(self.R) == len(self.r):
            print(len(self.r))
            print('爬虫结束，开始热度分析')
            SARunner().article_List(self.r)
