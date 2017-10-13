# -*- coding:utf-8 -*-

import oss2
import os
import logging

# settings.py
OSS_INFO = {
    "access_key": "LTAIEdu2OfvhWD7f",
    "access_password": "dd2V8omztBSxqHHYvolmwS6HJqnpRn",
    "access_domain": "oss-cn-beijing.aliyuncs.com"
}

OSS_INFO_TEST = {'intranet': {'ENDPOINT': 'oss-cn-beijing-internal.aliyuncs.com'},
                 'external': {'ENDPOINT': 'oss-cn-beijing.aliyuncs.com'},
                 'APPKEY_ID': 'LTAIEdu2OfvhWD7f',  # 用于标识用户
                 'APPKEY_SECRET': 'dd2V8omztBSxqHHYvolmwS6HJqnpRn',  # 用户用于加密签名字符串和 OSS 用来验证签名字符串的密钥
                 'BUCKET_NAME': 'donews-data',  # 存储空间bucket_name
                 'RETRY_TIME': 10
                 }


def get_bucket():
    """
    获取链接实例
    :return: 连接到某个bucket的实例
    """
    APPKEY_ID = OSS_INFO_TEST['APPKEY_ID']
    APPKEY_SECRET = OSS_INFO_TEST['APPKEY_SECRET']
    ENDPOINT = OSS_INFO_TEST['intranet']['ENDPOINT']
    BUCKET_NAME = OSS_INFO_TEST['BUCKET_NAME']

    auth = oss2.Auth(APPKEY_ID, APPKEY_SECRET)
    bucket = oss2.Bucket(auth, ENDPOINT, BUCKET_NAME)

    return bucket


def get_file(file_path, bucket):
    try:
        obj = bucket.get_object(file_path)
    except Exception, e:
        print e
        obj = None
    return obj


def get_file_size(file_path, bucket):
    try:
        head_obj = bucket.head_object(file_path)
        return head_obj.headers.get("Content-Length")
    except Exception, e:
        logging.error("Error message: %s", e)
        obj = None
    return obj


def download_oss_log(year, month, day):
    bucket = get_bucket()
    os.mkdir('oss_logs/oss_log' + year + month + day)
    file_count = 0
    # 注意去除路径最前面的"/"
    for i in range(24):
        if i < 10:
            i = "0" + str(i)
        file_path = "log/danews-data/niuer-donews-data" + year + "-" + month + "-" + day + "-" + str(i) + "-00-00-0001"
        obj = get_file(file_path, bucket)
        file_name = file_path.split("/")[-1] + ".log"
        if obj:
            with open('oss_logs/oss_log' + year + month + day + '/' + file_name, "w") as f:
                content = obj.read()
                f.write(content)
                file_count += 1
    return file_count


def test_():
    bucket = get_bucket()
    path = 'data/shareimg_oss/big_media_article_video/2017/09/21/7c33306e-9eb1-11e7-a4d1-00163e03c2de.mp4'
    print get_file_size(path, bucket)

if __name__ == "__main__":
    test_()
