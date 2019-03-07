

class BaiDuTBPipeline(object):
    def __init__(self):
        pass

    def process_item(self,item,spider):
        item['intro'] = ''.join(item['intro'])
        item['reply'] = int(item['reply'][0])
        item['title'] = ''.join(item['title'])
        item['source'] = item['source'].replace(u" ", "", )
        item['source'] = item['source'].replace(u"作者：", " 作者：",1 )
        print(item)
        return item


