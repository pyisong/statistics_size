# -*- coding:utf-8 -*-
import pymongo
import pymysql
import logging
from logging.handlers import TimedRotatingFileHandler
from threading import Thread

MONGO_PARA = "mongodb://access_mongo:donewsusemongodbby20170222@slave07:27017/admin?readPreference=secondaryPreferred"
MONGO_PARA_N = "mongodb://spider_slave06:28017"


def init_log(log_name):
    logging.basicConfig(level=logging.WARNING)
    # m 代表分钟，h 代表小时，d 代表天；backupCount 代表备份的天数
    console = TimedRotatingFileHandler("%s" % log_name, when='d', backupCount=10)
    console.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    console.setFormatter(formatter)
    logging.root.addHandler(console)
    # 如果不想在命令行窗口输出，去掉下一行的注释
    # logging.root.handlers.pop(0)


def get_mysql_db():
    mysql_db = None
    if mysql_db is None:
        mysql_db = pymysql.Connection(host="slave09",
                                      user="admin001",
                                      password="donews1234",
                                      database="supervision",
                                      charset="utf8")
    return mysql_db


def get_mysql_del_db():
    mysql_del_db = None
    if mysql_del_db is None:
        mysql_del_db = pymysql.Connection(host='10.31.144.174',  # 外网'47.94.36.239',
                                          user="crawler",
                                          password="donews1234",
                                          database="devdonewscms",
                                          charset="utf8")

    return mysql_del_db


def get_mongo(paras):
    try:
        client = pymongo.MongoClient(paras)
        return client
    except Exception as e:
        print "Connect mongo Error:", e
        return None


class MyThread(Thread):

    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None


def format_time(timestamp):
    month_dict = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
                  "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
                  "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}

    timestamp_list = timestamp.split("/")
    format_timestamp = timestamp_list[2].split()[0].split(":")[0] + '-' + month_dict[timestamp_list[1]] + '-' + \
                       timestamp_list[0] + ' ' + timestamp_list[2].split()[0].split(":")[1] + ':' +\
                       timestamp_list[2].split()[0].split(":")[2] + ':' + timestamp_list[2].split()[0].split(":")[3]

    return format_timestamp


def save_to_mysql(params_list):
    db = get_mysql_db()
    cursor = db.cursor()
    sql = "insert into {}(url, file_size) values (%s, %s)".format("statistics_osslog_url_size")
    cursor.executemany(sql, params_list)
    db.commit()

    cursor.close()
    db.close()

if __name__ == "__main__":
    pass
