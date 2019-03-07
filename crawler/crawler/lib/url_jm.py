# coding:utf-8
import datetime
import urllib.parse

class Urlchuli():
    """Url处理类，需要传入两个实参：Urlchuli('实参','编码类型')，默认utf-8
    url编码方法：url_bm() url解码方法：url_jm()"""

    def __init__(self ,can=None ,mazhi='utf-8'):
        self.can = can
        self.mazhi = mazhi

    def url_bm(self):
        """url_bm() 将传入的中文实参转为Urlencode编码"""
        quma = str(self.can).encode(self.mazhi)
        bianma = urllib.parse.quote(quma)
        return bianma

    def url_jm(self):
        """url_jm() 将传入的url进行解码成中文"""
        quma = str(self.can)
        jiema = urllib.parse.unquote(quma ,self.mazhi)
        return jiema





if __name__ == '__main__':
    # 第一个是传入的实参，第二个是选填url编码的类型（默认utf-8），可以是utf-8、gbk或其他
    # a = Urlchuli('中山大学，中大，康乐园', 'gbk')
    # print(a.url_bm())
    """
    {'title': '#广州美术学院澄籍教师6人邀请作品展#', 'debate': '4', 'read': '0', 'url': 'http://s.weibo.com/weibo/%23%E5%B9%BF%E5%B7%9E%E7%BE%8E%E6%9C%AF%E5%AD%A6%E9%99%A2%E6%BE%84%E7%B1%8D%E6%95%99%E5%B8%886%E4%BA%BA%E9%82%80%E8%AF%B7%E4%BD%9C%E5%93%81%E5%B1%95%23'}
    {'title': '#广州美术学院客座教授#', 'debate': '1', 'read': '160', 'url': 'http://s.weibo.com/weibo/%23%E5%B9%BF%E5%B7%9E%E7%BE%8E%E6%9C%AF%E5%AD%A6%E9%99%A2%E5%AE%A2%E5%BA%A7%E6%95%99%E6%8E%88%23'}
    {'title': '#%E5%B9%BF%E5%B7%9E%E7%BE%8E%E6%9C%AF%E5%AD%A6%E9%99%A22012%E5%B9%B4%E7%A0%94%E7%A9%B6%E7%94%9F%E6%AF%95%E4%B8%9A%E4%BD%9C%E5%93%81%E5%B1%95#', 'debate': '2', 'read': '0', 'url': 'http://s.weibo.com/weibo/%23%25E5%25B9%25BF%25E5%25B7%259E%25E7%25BE%258E%25E6%259C%25AF%25E5%25AD%25A6%25E9%2599%25A22012%25E5%25B9%25B4%25E7%25A0%2594%25E7%25A9%25B6%25E7%2594%259F%25E6%25AF%2595%25E4%25B8%259A%25E4%25BD%259C%25E5%2593%2581%25E5%25B1%2595%23'}
    """
    title = '#%E5%B9%BF%E5%B7%9E%E7%BE%8E%E6%9C%AF%E5%AD%A6%E9%99%A22012%E5%B9%B4%E7%A0%94%E7%A9%B6%E7%94%9F%E6%AF%95%E4%B8%9A%E4%BD%9C%E5%93%81%E5%B1%95#'

    b = Urlchuli(title, 'utf-8')
    print(b.url_jm())