# -*- coding:utf-8 -*-
from settings import get_mysql_db, format_time


def only_osslog(year, month, day):
    compare_dict = {}
    only_osslog_count = 0
    only_osslog_size = 0
    f_compare = open("target_file/mongo_compare_osslog" + year + month + day + ".txt", "r")
    f_osslog = open("target_file/osslog_to_file" + year + month + day + ".txt", "r")
    content_compare = f_compare.readlines()
    content_osslog = f_osslog.readlines()
    for line_compare in content_compare:
        line_compare_list = line_compare.split()
        url_compare = line_compare_list[3].strip()
        file_size_compare = int(line_compare_list[5].strip())
        compare_dict[url_compare] = file_size_compare
    with open("target_file/only_osslog" + year + month + day + ".txt", "w") as f:
        for line_osslog in content_osslog:
            mongo_list = line_osslog.split()
            url = mongo_list[0]
            file_size = mongo_list[1]
            timestamp = mongo_list[2]
            if file_size.isdigit():
                file_size = int(file_size)
            else:
                file_size = 0
            if url not in compare_dict:
                only_osslog_count += 1
                only_osslog_size += file_size
                f.write(url + '\t' + str(file_size) + '\t' + timestamp + '\n')
    return only_osslog_count, only_osslog_size


def insert_mysql_only_osslog(year, month, day):
    db = get_mysql_db()
    cursor = db.cursor()

    sql = "insert into {}(url, file_size, timestamps, datetime)" \
                     " values (%s, %s, %s, %s)".format("statistics_only_osslog")

    f = open("target_file/only_osslog" + year + month + day + ".txt", "r")
    content = f.readlines()

    lis = list()
    for line in content:
        line_list = line.split()
        url = line_list[0].strip()
        file_size = int(line_list[1].strip())
        timestamp = format_time(line_list[2])
        param = (url, file_size, timestamp, year + '-' + month + '-' + day)
        lis.append(param)
        if len(lis) >= 2000:
            cursor.executemany(sql, lis)
            db.commit()
            lis = list()
    cursor.executemany(sql, lis)
    db.commit()


if __name__ == "__main__":
    pass
