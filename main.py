# -*- coding:utf-8 -*-
import argparse
import logging
import datetime
from mongo_to_file import save_not_equal_data
from distinct_mongo_to_file import distinct_mongo_to_file
from download_oss_log import download_oss_log
from osslog_to_file import write_osslog_content
from only_osslog import only_osslog, insert_mysql_only_osslog
from result_table import result_table_main
from delete_data import delete_main
from mongo_compare_osslog import mongo_compare_osslog_main
from find_only_mongo import find_only_mongo
from send_email import send_email, export
from settings import init_log
from settings import MONGO_PARA_N

if __name__ == '__main__':

    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1))
    str_yesterday = yesterday.strftime('%Y-%m-%d')

    parser = argparse.ArgumentParser(description="Analysis data...")
    parser.add_argument("-y", "--year",  type=str, help="year of data", default="2017")
    parser.add_argument("-m", "--month",  type=str, help="month of data")
    parser.add_argument("-d", "--day", type=str,  help="day of date")
    args = parser.parse_args()
    target_month = args.month
    target_day = args.day
    target_year = args.year

    if not target_month and not target_day:
        target_month = str_yesterday.split('-')[1]
        target_day = str_yesterday.split('-')[2]

    start_time = target_year + "-" + target_month + "-" + target_day + " 00:00:00"
    end_time = target_year + "-" + target_month + "-" + target_day + " 23:59:59"

    init_log("logs/" + target_year + "-" + target_month + "-" + target_day + '.log')

    try:
        delete_main(save_days=7)
        logging.warning("Delete program run done....")
    except Exception, e:
        logging.error("Error message: %s", e)

    logging.warning("Start statistics %s-%s-%s data....", target_year, target_month, target_day)

    result, not_equal_dict =\
        save_not_equal_data(paras=MONGO_PARA_N,
                            start_time=start_time,
                            end_time=end_time,
                            month=target_month,
                            day=target_day,
                            year=target_year)
    logging.warning("write_mongo_lines: %s, total_data_count: %s, path_none_count: %s,"
                    " not_equal_count: %s, none_url_count: %s",
                    result["write_lines"], result["total_data_count"],
                    result["path_none_count"], result["not_equal_count"],
                    result["none_url_count"])

    distinct_result = distinct_mongo_to_file(year=target_year, month=target_month, day=target_day)
    logging.warning("news_count: %s, distinct_mongo_lines: %s, every_media_news_count: %s, repetition_count: %s",
                    distinct_result["news_count"],
                    distinct_result["distinct_lines_count"],
                    distinct_result["every_media_news_count"],
                    distinct_result["repetition_count"])

    log_count = download_oss_log(year=target_year, month=target_month, day=target_day)
    logging.warning("oss logs count: %s", log_count)

    write_osslog_result = write_osslog_content(year=target_year, month=target_month, day=target_day)
    logging.warning("write_osslog_lines: %s, osslog_size: %s",
                    write_osslog_result["osslog_lines"], write_osslog_result["osslog_size"])

    count_dict = mongo_compare_osslog_main(year=target_year, month=target_month, day=target_day, read_size=50*1024)
    logging.warning("common_count: %s, common_size: %s, only_mongo_count: %s only_mongo_size: %s, error_count: %s",
                    count_dict["common_count"],
                    count_dict["common_size"],
                    count_dict["only_mongo_count"],
                    count_dict["only_mongo_size"],
                    count_dict["error_count"],)

    only_osslog_count, only_osslog_size = only_osslog(year=target_year, month=target_month, day=target_day)
    logging.warning("only_osslog_count: %s, only_osslog_size: %s", only_osslog_count, only_osslog_size)

    insert_mysql_only_osslog(year=target_year, month=target_month, day=target_day)
    logging.warning("insert only osslog to mysql done! %s-%s-%s",  target_year, target_month, target_day)

    result_table_main(year=target_year, month=target_month, day=target_day,
                      count_dict=count_dict, only_osslog_count=only_osslog_count,
                      _id_media=distinct_result["every_media_news_count"],
                      common_size=count_dict["common_size"], only_osslog_size=only_osslog_size)

    logging.warning("start send email...")
    export(year=target_year, month=target_month, day=target_day)
    send_email(year=target_year, month=target_month, day=target_day)
    logging.warning("send email success...")

    logging.warning("start find only mongo...")
    try:
        not_found_count = find_only_mongo(year=target_year, month=target_month, day=target_day)
        logging.warning("only mongo not found count: %s", not_found_count)
    except Exception, e:
        logging.error("Find only mongo error %s", e)

    logging.warning("Statistics %s-%s-%s data, Success....", target_year, target_month, target_day)
