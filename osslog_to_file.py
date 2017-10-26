# -*- coding:utf-8 -*-
import os
# import logging
import gc


def get_file(year, month, day):
    file_path_list = [
        'oss_logs/oss_log_danews-data' + year + month + day + "/",
        'oss_logs/oss_log_donews-test1' + year + month + day + "/",
        'oss_logs/oss_log_wangleilog' + year + month + day + "/",
    ]
    content_dict = {
        "oss_log_danews-data_content_list": [],
        "oss_log_danews-data_osslog_lines": 0,
        "oss_log_danews-data_osslog_size": 0,
        "oss_log_donews-test1_content_list": [],
        "oss_log_donews-test1_osslog_lines": 0,
        "oss_log_donews-test1_osslog_size": 0,
        "oss_log_wangleilog_content_list": [],
        "oss_log_wangleilog_osslog_lines": 0,
        "oss_log_wangleilog_osslog_size": 0,
        "error_line": 0,
        }
    for file_path in file_path_list:
        file_list = os.listdir(file_path)
        for file_name in file_list:
            if file_name.find(year + "-" + month + "-" + day) != -1:
                content = open(file_path + file_name, "r")
                result = content.readlines()
                for i in result:
                    if i.find('"PUT') != -1:
                        timestamp = i.split("[")[1].split("]")[0]
                        url = i.split('"PUT')[1].split()[0].replace("%2F", '/')
                        file_size = i.split('"-"')[-3].strip().split(" ")[-1]
                        if file_size.isdigit():
                            file_size = file_size
                            content_dict[file_path.split('/')[1][:-8] + "_osslog_lines"] += 1
                            content_dict[file_path.split('/')[1][:-8] + "_osslog_size"] += int(file_size)
                            if file_path == 'oss_logs/oss_log_danews-data' + year + month + day + "/":
                                tup = (url, file_size, timestamp)
                                content_dict[file_path.split('/')[1][:-8] + "_content_list"].append(tup)
                        else:
                            with open("target_file/error_line"+ year + month + day + ".txt", 'a') as f:
                                f.write(url + '\t' + file_size + '\t' + timestamp + '\n')
                                content_dict["error_line"] += 0

                content.close()
    return content_dict


def write_osslog_content(year, month, day):
    content_dict = get_file(year=year, month=month, day=day)
    with open("target_file/osslog_to_file" + year + month + day + ".txt", 'w') as f:
        for item in content_dict["oss_log_danews-data_content_list"]:
            f.write(str(item[0]) + '\t' + str(item[1]) + '\t' + str(item[2]) + '\n')

    dic = {
        "oss_log_danews-data_osslog_lines": content_dict["oss_log_danews-data_osslog_lines"],
        "oss_log_danews-data_osslog_size": content_dict["oss_log_danews-data_osslog_size"],
        "oss_log_donews-test1_osslog_lines": content_dict["oss_log_donews-test1_osslog_lines"],
        "oss_log_donews-test1_osslog_size": content_dict["oss_log_donews-test1_osslog_size"],
        "oss_log_wangleilog_osslog_lines": content_dict["oss_log_wangleilog_osslog_lines"],
        "oss_log_wangleilog_osslog_size": content_dict["oss_log_wangleilog_osslog_size"],
        "error_line": content_dict["error_line"],
    }

    del content_dict
    gc.collect()

    return dic


if __name__ == "__main__":
    write_osslog_content("2017", "10", "15")
