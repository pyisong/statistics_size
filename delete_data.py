# -*- coding:utf-8 -*-
import datetime
import logging
import os
from settings import get_mysql_db
from settings import init_log
from shutil import rmtree


def delete_mysql_table(save_days):
    time_str = (datetime.datetime.now() - datetime.timedelta(days=save_days + 1)).strftime("%Y-%m-%d")
    db = get_mysql_db()
    cursor = db.cursor()
    tables_list = ['statistics_mongo_compare_osslog', 'statistics_only_osslog']
    for table in tables_list:
            sql = 'delete from {} where datetime=%s'.format(table)
            result = cursor.execute(sql, time_str)
            if result > 0:
                db.commit()
                logging.warning("Delete %s success! datetime: %s ...", table, time_str)


def delete_log(save_days):
    logs_list = os.listdir('logs/')
    time_str = (datetime.datetime.now() - datetime.timedelta(days=save_days+1)).strftime("%Y-%m-%d")
    for log_name in logs_list:
        if log_name.startswith(time_str):
            os.remove('logs/' + log_name)
            logging.warning("Delete log: %s success...", log_name)


def delete_target_file(save_days):
    target_file_list = os.listdir('target_file/')
    time_str = (datetime.datetime.now() - datetime.timedelta(days=save_days+1)).strftime("%Y%m%d")
    for target_file in target_file_list:
        if target_file.find(time_str) != -1:
            os.remove('target_file/' + target_file)
            logging.warning("Delete target file: %s success...", target_file)


def delete_oss_log(save_days):
    oss_log_list = os.listdir('oss_logs/')
    time_str = (datetime.datetime.now() - datetime.timedelta(days=save_days+1)).strftime("%Y%m%d")
    for oss_log in oss_log_list:
        if oss_log.endswith(time_str):
            rmtree('oss_logs/' + oss_log)
            logging.warning("Delete oss logs: %s success...", oss_log)


def delete_main(save_days):
    delete_mysql_table(save_days)
    delete_oss_log(save_days)
    # delete_log(save_days)
    delete_target_file(save_days)


if __name__ == "__main__":
    init_log("logs/delete.txt")
    delete_main(7)
