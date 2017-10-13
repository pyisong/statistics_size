# -*- coding:utf-8 -*-
import os
import logging


def get_file(year, month, day):
    file_list = os.listdir("oss_logs/oss_log" + year + month + day + "/")
    content_list = []
    osslog_lines = 0
    osslog_size = 0
    for file_name in file_list:
        if file_name.find(year + "-" + month + "-" + day) != -1:
            content = open("oss_logs/oss_log" + year + month + day + "/" + file_name, "r")
            result = content.readlines()
            for i in result:
                if i.find('"PUT') != -1:
                    timestamp = i.split("[")[1].split("]")[0]
                    url = i.split('"PUT')[1].split()[0].replace("%2F", '/')
                    file_size = i.split('"-"')[-3].strip().split(" ")[-1]
                    if file_size.isdigit():
                        file_size = file_size
                        osslog_lines += 1
                        osslog_size += int(file_size)
                        tup = (url, file_size, timestamp)
                        content_list.append(tup)
                    else:
                        logging.info("error line: %s, %s, %s", url, file_size, timestamp)
            content.close()
    result = {"osslog_lines": osslog_lines, "osslog_size": osslog_size}
    return content_list, result


def write_osslog_content(year, month, day):
    content_list, result = get_file(year=year, month=month, day=day)
    with open("target_file/osslog_to_file" + year + month + day + ".txt", 'w') as f:
        for item in content_list:
            f.write(str(item[0]) + '\t' + str(item[1]) + '\t' + str(item[2]) + '\n')
    return result


if __name__ == "__main__":
    pass
