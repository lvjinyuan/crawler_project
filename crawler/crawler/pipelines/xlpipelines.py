

class XinLangPipeline(object):
    def __init__(self):
        pass

    def process_item(self,item,spider):
        item['article'] =  self.process_article(item["article"])
        # item['source'] = self.process_date_source(item["date_source"])
        item['source'] = ''.join(item['source'])
        print(item)



    def process_article(self,article):
        if isinstance(article,list) is True:
            article = [i.replace(u"\n", "", ) for i in article]
            article = [i.replace(u"\t", "", ) for i in article]
            article = [i.replace(u"\u3000", "", ) for i in article]
            article = [i.replace(u"\xa0", "", ) for i in article]
            article = [i.replace(u"\u200b", "", ) for i in article]
            article = [i.replace(u"\u200d", "", ) for i in article]
            article = [i.replace(u" ", "", 2) for i in article]
            article = [i for i in article if len(i) > 0] # 将空字符串去掉
            article = [i+'\n' for i in article]
            article = ''.join(article)
            return article

    def process_date_source(self,date_source):
        date_source = [i.replace(u"\n", "", ) for i in date_source]
        date_source = [i.replace(u"\t", "", ) for i in date_source]
        date_source = [i.replace(u" ", "", ) for i in date_source]
        date_source = ''.join(date_source)
        return date_source

