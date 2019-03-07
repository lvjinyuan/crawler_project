# -*- coding: utf-8 -*-
import re
import urllib
import scrapy
from crawler.items.items import BaidutiebaItem
from crawler.sarunner import SARunner
from crawler.settings.user_config import TB_MAX_PAGE

# 无法多关键字搜索
class TbSpider(scrapy.Spider):
    name = 'tb'
    allowed_domains = ['tieba.baidu.com/mo/q']
    custom_settings = {
        'ITEM_PIPELINES': {'crawler.pipelines.BaiDuTBPipeline': 300, },
    }
    r = []
    R= []
    MAX_PAGE = TB_MAX_PAGE
    # start_urls = ['http://tieba.baidu.com/mo/q----,sz@320_240-1-3---2/m?kw=吧名&pn=0']
    """
    url = 'http://wap.baidu.com/sf/vsearch?pd=tieba&word=%E4%B8%AD%E5%B1%B1%E5%A4%A7%E5%AD%A6&tn=vsearch&sa=vs_tab&lid=8756617510026267405&ms=1'
    可以使用此url重新编写
    """


    args = SARunner().parser()
    keyword_list = SARunner().keyworld_list(args.anaentities)
    keyword_list = re.split(r'\|', keyword_list)
    p = 0
    P = len(keyword_list)

    def start_requests(self):
        for keyword in self.keyword_list:
            url = "http://tieba.baidu.com/f/search/res?ie=utf-8&qw={}".format(keyword)
            yield scrapy.FormRequest(url=url, callback=self.parse_detail,dont_filter=True)

    def parse_detail(self,response):
        print(response.url)
        div_list = response.xpath("//div[@class = 's_post_list']/div[@class = 's_post']")
        for div in div_list:
            item = BaidutiebaItem()
            item['title'] = div.xpath("./span[@class='p_title']/a[@class='bluelink' and @data-fid]").xpath('string(.)').extract()
            item['time'] = div.xpath(".//font[@class='p_green p_date']/text()").extract_first()
            item['intro'] = div.xpath(".//div[@class = 'p_content']").xpath('string(.)').extract()
            item['href'] = div.xpath("./span[@class='p_title']/a[@class='bluelink' and @data-fid]/@href").extract_first()
            item['href'] = urllib.parse.urljoin(response.url, item['href'])
            item['source'] = div.xpath("./text()|.//a//font//text()").extract()
            item['source'] = ''.join(item['source'])

            if item['time'] is None: # 过滤掉贴吧信息
                continue
            self.r.append(item['href'])
            yield scrapy.Request(
                item['href'],
                callback=self.parse_main,
                meta={'item': item},
                dont_filter=True
            )
        self.p +=1
        num_page = response.xpath("//span[@class='cur']/text()").extract_first()
        max_page_url = response.xpath("//a[text() = '尾页']/@href").extract_first()
        if self.MAX_PAGE is None and max_page_url is not None:
            self.MAX_PAGE = re.findall(r'&pn=.{1,}', max_page_url)[0][4:]

        if int(num_page) == self.MAX_PAGE:
            if self.p == self.P:
                print('页数上限')
        else:
            next_url = response.xpath("//a[text() = '下一页>']/@href").extract_first()
            next_url = urllib.parse.urljoin(response.url, next_url)
            yield scrapy.Request(
                next_url,
                callback=self.parse_detail,
                dont_filter=True
            )

    def parse_main(self,response):
        item = response.meta['item']
        item['reply'] = response.xpath("//div[@id='thread_theme_5']//span[@class='red'][1]/text()").extract()
        yield item
        self.R.append(item)
        if len(self.r) == len(self.R):
            print('开始保存数据库')
