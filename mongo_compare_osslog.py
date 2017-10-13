import logging
from settings import get_mysql_db
from settings import MyThread
from download_oss_log import get_bucket, get_file_size


def mongo_to_log(content_mongo, log_dict, bucket, f):
    error_count = 0
    for line_mongo in content_mongo:
        mongo_list = line_mongo.split()
        _id = mongo_list[0]
        media = mongo_list[1]
        posit = mongo_list[2]
        url = mongo_list[3]
        flag = 0
        file_size = 0
        if url in log_dict:
            flag = 1
            file_size = log_dict[url]
        if flag == 0:
            file_size = get_file_size(file_path=url.lstrip('/'), bucket=bucket)
            if not file_size:
                logging.error("Error mongo url: %s, _id: %s, media: %s", url, _id, media)
                error_count += 1
                file_size = 0
        f.write(_id + '\t' + media + '\t' + posit + '\t' + url + "\t" +
                str(flag) + "\t" + str(file_size) + '\n')
    return error_count


def compare_file(year, month, day, read_size):
    log_dict = {}
    count_dict = {
        "common_count": 0,
        "common_size": 0,
        "only_mongo_count": 0,
        "only_mongo_size": 0,
        "error_count": 0,
    }
    bucket = get_bucket()
    f_mongo = open("target_file/distinct_mongo_to_file" + year + month + day + ".txt", "r")
    f_osslog = open("target_file/osslog_to_file" + year + month + day + ".txt", "r")
    content_osslog = f_osslog.readlines()
    for line_osslog in content_osslog:
        log_list = line_osslog.split()
        log_url = log_list[0]
        file_size = log_list[1]
        if file_size.isdigit():
            file_size = int(file_size)
            log_dict[log_url] = file_size
        else:
            logging.error("error line: %s, %s", log_url, file_size)

    with open("target_file/mongo_compare_osslog" + year + month + day + ".txt", "w+") as f:
        content_mongo = f_mongo.readlines(read_size)
        thread_list = []
        while len(content_mongo) > 0:
            t = MyThread(func=mongo_to_log, args=(content_mongo, log_dict, bucket, f))
            thread_list.append(t)
            content_mongo = f_mongo.readlines(read_size)

        for item in thread_list:
            item.start()
        for item in thread_list:
            error_count = item.get_result()
            if error_count:
                count_dict["error_count"] += error_count
            item.join()

    f_compare = open("target_file/mongo_compare_osslog" + year + month + day + ".txt", "r")
    contents = f_compare.readlines()
    with open("target_file/only_mongo" + year + month + day + ".txt", "w") as f:
        for line in contents:
            line_list = line.split('\t')
            _id = line_list[0].strip()
            media = line_list[1].strip()
            flag = line_list[4]
            file_size = int(line_list[5])
            url = line_list[3].strip()
            if flag == '0':
                count_dict['only_mongo_count'] += 1
                count_dict['only_mongo_size'] += file_size
                f.write( _id + '\t' + url + '\t' + str(file_size) + '\t' + media + '\n')
            else:
                count_dict["common_count"] += 1
                count_dict["common_size"] += file_size

    return count_dict


def insert_mysql(year, month, day):
    db = get_mysql_db()
    cursor = db.cursor()

    sql = "insert into {}(_id, media, posit, url, flag, file_size, datetime)" \
                     " values (%s, %s, %s, %s, %s, %s, %s)".format("statistics_mongo_compare_osslog")
    f = open("target_file/mongo_compare_osslog" + year + month + day + ".txt", "r")
    content = f.readlines()

    lis = list()
    for line in content:
        line_list = line.split()
        _id = line_list[0].strip()
        media = line_list[1].strip()
        posit = line_list[2].strip()
        url = line_list[3].strip()
        flag = int(line_list[4].strip())
        file_size = int(line_list[5].strip())
        param = (_id, media, posit, url, flag, file_size, year + '-' + month + '-' + day)
        lis.append(param)
        if len(lis) >= 2000:
            cursor.executemany(sql, lis)
            db.commit()
            lis = list()
    cursor.executemany(sql, lis)
    db.commit()


def mongo_compare_osslog_main(year, month, day, read_size):
    count_dict = compare_file(year=year, month=month, day=day, read_size=read_size)
    insert_mysql(year=year, month=month, day=day)
    return count_dict

if __name__ == "__main__":
    compare_file(year='2017', month='09', day='20', read_size=50*1024)
