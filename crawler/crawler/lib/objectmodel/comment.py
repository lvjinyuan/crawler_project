'''
Created on 7 Oct 2017

@author: eyaomai
'''

class Comment(object):
    '''
    classdocs
    '''


    def __init__(self, tid, channel_id, cid,
                 add_datetime, publish_datetime,
                 ip_address, location_country, location_region, location_city, 
                 author_id, author_name, content, reply_author_id, 
                 read_count, like_count, reply_count, dislike_count):
        '''
        Constructor
        '''
        self.tid = tid
        self.channel_id = channel_id
        self.cid = cid
        self.add_datetime = add_datetime
        self.publish_datetime = publish_datetime
        self.ip_address = ip_address
        self.location_country = location_country
        self.location_region = location_region
        self.location_city = location_city
        self.author_id = author_id
        self.author_name = author_name
        self.content = content
        self.reply_author_id = reply_author_id
        self.read_count = read_count
        self.like_count = like_count
        self.reply_count = reply_count
        self.dislike_count = dislike_count