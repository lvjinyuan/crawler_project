import scrapy

class XinLangspider1Item(scrapy.Item):
    # TID
    TID = scrapy.Field()
    # 新闻简介      DIGEST
    intro = scrapy.Field()
    # 新闻标题      TITLE
    title_main = scrapy.Field()
    # 新闻url     URL
    href = scrapy.Field()
    # 新闻文章  CONTENT
    article = scrapy.Field()
    # 来源和时期
    date_source = scrapy.Field()

    # PUBLISH_DATETIME
    time = scrapy.Field()
    # PUBLISH_METHOD
    source = scrapy.Field()


class xw_type:
    # 通过新闻类型的分类实现不同方式的爬取
    # 城市新闻           # 新浪广东 新浪大连
    cs = ['gd','dl','ln','hebei']
    # 新浪新闻          # 新浪新闻
    xw = ['news']
    # 新浪博客          # 新浪博客
    bk = ['blog']
    # 新浪时尚          # 新浪时尚
    ss = ['fashion','edu']

