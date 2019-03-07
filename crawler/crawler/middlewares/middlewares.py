# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import random
from scrapy import signals


class RandomUserAgentMiddleware:
    def process_request(self, request, spider):
        ua = random.choice(spider.settings.get("USER_AGENTS_LIST"))
        # 在USER_AGENTS_LIST中随机选择一个USER_AGENTS
        request.headers["User-Agent"] = ua


# class XueqiuCookes:
#     def process_request(self, request, spider):


