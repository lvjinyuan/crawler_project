import datetime,re
from scrapy.pipelines.images import ImagesPipeline
from scrapy import Request
from crawler.items.items import Weibospider1Item, WeiborelayspiderItem
from crawler.settings.settings import to_day


class Weibo1Pipeline(object):
    def process_item(self,item,spider):
        if isinstance(item, Weibospider1Item):
            # item["content"] = item["content"] or '',  # 如果返回的data['pages']为空 则传递''中的内容
            item["content"] = self.process_content(item["content"])
            item['send_time'] = self.process_send_item(item['send_time'])
            if item["reason"] is not None:
                item["reason"] = self.process_reason(item["reason"])
            item['comment_url'] = self.process_comment_url(item['comment_url'])
            item['relay_url'] = item['comment_url'].replace(u"comment", "repost", 1)

            item["dianzan"] = re.findall(r"\d+\.?\d*", item["dianzan"])[0]
            item["relay"] = re.findall(r"\d+\.?\d*", item["relay"])[0]
            item["comment"] = re.findall(r"\d+\.?\d*", item["comment"])[0]
            if item['reason_id'] is not None:
                item['reason_id'] = re.findall(r"\w+\.?",item['reason_id'])[-1]
            item['author_id'] = re.findall(r"\w+\.?", item['author_id'])[-1]
            print(item)
            print("=" * 100)
            return item


    def process_content(self,content):
        # print(isinstance(content,list)) 判断content的类型
        if isinstance(content,list) is True:
            content = [i.replace(u"\u200b", "", ) for i in content]
            content = [i.replace(u"\xa0", "", ) for i in content]
            content = [i.replace(u" ", "", ) for i in content]
            content = [i.replace(u":", "",1) for i in content]
            content = [i.replace(u"\u3000", "", ) for i in content]
            content = [i.replace(u"<br/>", "", ) for i in content]
            content = [i for i in content if len(i) > 0] # 将空字符串去掉
            content = ''.join(content)
        else:
            content = content.replace(u"\u200b", "", ).replace(u"\xa0", "", ).replace(u"\u3000", "", ).replace(u"<br/>", "", ).replace(u" ", "", )
        a = re.compile('\[组图共\d+张\]')
        content = a.sub('', content)
        b = re.compile('原文评论\[d+\]')
        content = b.sub('', content)
        return content

    def process_reason(self,reason):
        if reason is not None:
            if isinstance(reason,list) is True:
                reason = [i.replace(u"\xa0", "", ) for i in reason]
                reason = [i.replace(u"<br/>", "", ) for i in reason]
                reason = [i for i in reason if len(i) > 0]
                reason = ''.join(reason)
            else:
                reason= reason.replace(u"\xa0","",).replace(u"<br/>", "", )
        return reason

    def process_send_item(self,send_time):
        send_time = send_time.replace(u"\xa0", "", )
        if '来自' in send_time:
            send_time = re.findall(r'.{1,}来自', send_time)[0][0:-2]
        if "今天" in send_time:
            a = to_day.strftime('%Y-%m-%d')
            send_time = send_time.replace(u"今天",a, )
        elif "分钟前" in send_time:
            a = re.findall(r'.{1,}分钟前', send_time)[0][:-3]
            send_time = (datetime.datetime.now() + datetime.timedelta(minutes=-int(a))).strftime('%Y-%m-%d %H:%M')
        elif '月'and '日' in send_time:
            send_time = send_time.replace(u"月", "-", ).replace(u"日", "", )
            a =to_day.strftime('%Y-')
            send_time = a + send_time
        return send_time

    def process_comment_url(self,comment_url):
        uid = re.findall(r'uid=.{1,}&', comment_url)[0][4:-1]
        comment = re.findall(r'ent/.{1,}\?', comment_url)[0][4:-1]
        comment_url = 'https://weibo.com/{}/{}?type=comment'.format(uid,comment)
        return comment_url




# 继承ImagesPipenine类，这是图片管道
class DownloadImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):  # 下载图片
        if item['img_url'] is not None:
            yield Request(item['img_url'],
                          meta={'item': item})  # 添加meta是为了下面重命名文件名使用

    def file_path(self, request, response=None, info=None):
        item = request.meta['item']
        image_guid = request.url.split('/')[-1]
        # print(image_guid)
        filename = u'wb/{0}/{1}'.format(item['name'], image_guid)
        return filename






