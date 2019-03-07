from crawler.settings.user_config import MYSQL_HOST, MYSQL_DBNAME, MYSQL_USER, MYSQL_PASSWD, MYSQL_PORT
import pymysql


class SAConfiguration(object):
    def __init__(self):
        self.connect= pymysql.connect(
            host=MYSQL_HOST,
            db=MYSQL_DBNAME,
            user=MYSQL_USER,
            passwd=MYSQL_PASSWD,
            port = MYSQL_PORT,
            # charset='utf8',
            # use_unicode=True,
        )
        # 通过cursor执行增删查改
        self.cursor = self.connect.cursor()

    def inquire_type_id(self,TYPE_ID):
        sql = self.cursor.execute(
            """select CRAWLER_CODE from sa_channel where CHANNEL_ID=%s """, TYPE_ID)
        results = self.cursor.fetchone()
        channel_name = results[0]
        return channel_name

    def inquire_entity_name(self, ENTITY_ID):
        sql = self.cursor.execute(
            """select INTERNAL_KEYWORD_LIST from sa_entity where ENTITY_ID=%s """, ENTITY_ID)
        results = self.cursor.fetchone()
        keyword_list = results[0]
        return keyword_list



