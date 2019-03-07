from crawler.items.items import RMWspider1Item, XHWspider1Item


class RenMingPipeline(object):
    def process_item(self,item,spider):
        item["article"] = self.process_article(item["article"])
        item["intro"] = self.process_intro(item["intro"])
        if isinstance(item, RMWspider1Item):
            item["time"] = self.process_time_rmw(item["time"])
        if isinstance(item, XHWspider1Item):
            item["time"] = self.process_time_xhw(item["time"])
        print(item)
        return item

    def process_article(self,article):
        if isinstance(article,list) is True:
            article = [i.replace(u"\xa0", "", ) for i in article]
            article = [i.replace(u"\n", "", ) for i in article]
            article = [i.replace(u"\t", "", ) for i in article]
            article = [i.replace(u"\r", "", ) for i in article]
            article = [i.replace(u"\u3000", "", ) for i in article]
            article = [i for i in article if len(i) > 0] # 将空字符串去掉
            article = [i + '\n' for i in article]
            article = ''.join(article)
        else:
            article = article.replace(u"\xa0", "", )
            article = article.replace(u"\n", "", )
            article = article.replace(u"\t", "", )
            article = article.replace(u"\r", "", )
            article = article.replace(u"\u3000", "", )
        return article

    def process_intro(self,intro):
        if intro is not None:
            if isinstance(intro,list) is True:
                intro = [i.replace(u"\n", "", ) for i in intro]
                intro = [i.replace(u"\t", "", ) for i in intro]
                intro = [i.replace(u"\r", "", ) for i in intro]
                intro = [i.replace(u"\u3000", "", ) for i in intro]
                intro = [i for i in intro if len(i) > 0] # 将空字符串去掉
                intro = ''.join(intro)
            else:
                intro = intro.replace(u"\n", "", )
                intro = intro.replace(u"\t", "", )
                intro = intro.replace(u"\r", "", )
                intro = intro.replace(u"\u3000", "", )
        return intro

    def process_time_rmw(self,time):
            time = ''.join(time)
            time = time.replace(u"\xa0", "", )
            time = time.replace(u" ", "",1)
            return time

    def process_time_xhw(self,time):
            time = ''.join(time)
            time = time.replace(u"\xa0", "", )
            return time