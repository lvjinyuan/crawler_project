# _*_ coding:utf-8 _*_

import smtplib
from email.mime.text import MIMEText
from crawler.lib.common.utils import MySqlProxy
import datetime
# import sys

# reload(sys)
# sys.setdefaultencoding('utf8')
# 发送邮件


class SendEmail(object):

    def __init__(self):

        self.username = '26380282@qq.com'
        self.passwd = 'vntbuvefdatjbihi'
        self.recv = '56066881@qq.com'      # lyjasdf@yeah.net  # 56066881@qq.com
        self.mail_host = 'smtp.qq.com'
        self.port = 25

    def send(self,title,content):
        msg = MIMEText(content,'plain', 'utf-8')
        msg['Subject'] = title
        msg['From'] = self.username
        msg['To'] = self.recv
        smtp = smtplib.SMTP(self.mail_host, port=self.port)
        smtp.login(self.username, self.passwd)
        smtp.sendmail(self.username, self.recv, msg.as_string())
        smtp.quit()


    @staticmethod
    def school_list():
        school_list = [
            '广州大学','华南理工大学','广东工业大学','广东药学院','中山大学','广东外语外贸大学','广州中医药大学',
            '广东药科大学','广州美术学院','星海音乐学院','华南师范大学'
        ]
        return school_list

    # 获取前day天的日期
    @staticmethod
    def getYesterday(day=None):
        if day is not None:
            day = day
        else:
            day = 1
        today = datetime.date.today()
        oneday = datetime.timedelta(days=day)
        yesterday = today - oneday
        return yesterday

#写入数据库
# class InsertDB():
#
#     def __init__(self):
#
#         self.host = 'a002.nscc-gz.cn'
#         self.port = 10416
#         self.user = 'test'
#         self.passward = 'test'
#         self.db='sa2'
#
#     def Insert(self,channelId,entityId,errorcontent):
#         dbProxy = MySqlProxy(self.host, self.port, self.user, self.passward, self.db)
#         add_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         valueList = []
#         valueList.append('(%d,"%s","%s","%s")' % (
#                 channelId,
#                 entityId,
#                 add_time,
#                 errorcontent
#             ))
#         sql = """INSERT INTO sa_monitor(CHANNEL_ID,
#                        ENTITY_ID, ADD_DATETIME, ERROR_CONTENT)
#                        VALUES %s"""
#         tmp = sql % (','.join(valueList))
#         dbProxy.execute(tmp)
#         dbProxy.commit()
#         dbProxy.close()







