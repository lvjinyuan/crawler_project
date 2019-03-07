# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class Weibospider1Item(scrapy.Item):
    # 微博div的种类元素为：四种
    # 1、不带图片的原创
    # 2、带图片的原创
    # 3、不带图片的转载
    # 4、带图片的转载

    # 公共部分
    TID = scrapy.Field()
    # 类型    PUBLISH_METHOD
    category = scrapy.Field()
    # 博主名字  AUTHOR_NAME
    author = scrapy.Field(serializer=str)
    # 博主id      AUTHOR_ID
    author_id = scrapy.Field(serializer=str)
    # 博主页url
    author_url = scrapy.Field()
    # 内容    CONTENT
    content = scrapy.Field(serializer=str)
    # 点赞    LIKE_COUNT
    dianzan = scrapy.Field(serializer=int)
    # 转发     FORWARD_COUNT
    relay = scrapy.Field(serializer=int)
    # 评论    REPLY_COUNT
    comment = scrapy.Field(serializer=int)
    # 发送时间  PUBLISH_DATETIME
    send_time = scrapy.Field()

    # 其他部分
    # 图片
    img_url = scrapy.Field()
    # 转发理由
    reason = scrapy.Field(serializer=str)
    # 评论页url（微博详情页） URL
    comment_url = scrapy.Field()
    # 被转发人名称
    reason_name = scrapy.Field(serializer=str)
    # 被转发人id
    reason_id = scrapy.Field(serializer=str)
    # 转发页url
    relay_url = scrapy.Field()

    # 用户详情页
    # 头像url
class Weibodetailspider1Item(scrapy.Item):
    tid =scrapy.Field()
    # 用户名字
    user_name = scrapy.Field()
    # 头像url
    head_url = scrapy.Field()
    # 粉丝数
    fans = scrapy.Field()
    # 关注数
    Concern = scrapy.Field()
    # 作者详情页url
    author_url =scrapy.Field()


class WeiborelayspiderItem(scrapy.Item):
    # 转发这条微博的人名字
    relay_people_name  = scrapy.Field()
    # 转发人的id
    relay_people_id = scrapy.Field()
    # 转发内容
    content = scrapy.Field()
    # 转发时间
    relay_time = scrapy.Field()
    # 转发内容被赞数
    relay_like_count = scrapy.Field()
    # 转发人头像
    # logo_url = scrapy.Field()
    # 被转发文章tid
    tid = scrapy.Field()
    # 媒体id
    channel_id = scrapy.Field()
    # 转发页url
    relay_url = scrapy.Field()



# item["reason"] = None
# item["img_url"] = None
# item['reason_name'] = None
# item['reason_id'] = None

class RMWspider1Item(scrapy.Item):

    TID =scrapy.Field()
    title = scrapy.Field()
    time = scrapy.Field()
    intro = scrapy.Field()
    href = scrapy.Field()
    source = scrapy.Field()
    article = scrapy.Field()

class BaidutiebaItem(scrapy.Item):
    title = scrapy.Field()
    intro = scrapy.Field()
    href = scrapy.Field()
    source = scrapy.Field()
    time = scrapy.Field()
    reply = scrapy.Field()


class XHWspider1Item(scrapy.Item):
    #TID
    TID = scrapy.Field()
    # TITLE
    title = scrapy.Field()
    # PUBLISH_DATETIME
    time = scrapy.Field()
    # URL
    href = scrapy.Field()
    # DIGEST
    intro = scrapy.Field()
    # AUTHOR_NAME
    source = scrapy.Field()
    # CONTENT
    article = scrapy.Field()

class XueQiuspider1Item(scrapy.Item):
    """
        搜索页数据
    """
    # 股票id
    id = scrapy.Field()
    # 搜索出的名称
    name = scrapy.Field()
    # 日收益
    daily_gain = scrapy.Field()
    # 月收益
    monthly_gain = scrapy.Field()
    # 总收益
    total_gain = scrapy.Field()
    # 净值
    net_value = scrapy.Field()
    # 比率
    bb_rate = scrapy.Field()
    # 详情页url
    href = scrapy.Field()
    # 组合创建人name
    screen_name = scrapy.Field()


    #     用户调仓历史
    # 股票名
    stock_name = scrapy.Field()
    # 股票符号logo
    stock_symbol = scrapy.Field()
    # 参考成交价
    price = scrapy.Field()
    # 目标涨幅
    target_weight = scrapy.Field()

