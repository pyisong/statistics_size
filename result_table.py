# -*- coding:utf-8 -*-
from settings import get_mysql_db, get_mongo, MONGO_PARA_N


def result_table(year, month, day, _id_media):
    db = get_mysql_db()
    cursor = db.cursor()
    params = {}
    select_sql = 'select media, posit, count(*) as count,' \
                 ' sum(file_size) as size from {} where media=%s and flag=1 and datetime=%s group by posit;'\
        .format("statistics_mongo_compare_osslog")

    select_sql_other = 'select media, posit, count(*) as count,' \
                       ' sum(file_size) as size from {} where media=%s and datetime=%s group by posit;'\
        .format("statistics_mongo_compare_osslog")

    client = get_mongo(MONGO_PARA_N)
    dmt_list = client['dmt_jh_data'].collection_names()
    crawled_list = client['crawled_TTH_web_page'].collection_names()
    crawled_list.remove(u"miaopai")
    media_list = dmt_list + crawled_list

    for media in media_list:
        para = (media, year + '-' + month + '-' + day)
        cursor.execute(select_sql, para)
        result = cursor.fetchall()
        params[media] = {}
        for item in result:
            params[media][item[1] + "_count_1"] = item[2]
            params[media][item[1] + "_size_1"] = float(item[3])
        for posit in ["img_location_count_1", "small_img_location_count_1", "video_location_count_1"]:
            if posit  not in params[media]:
                params[media][posit] = 0
        for posit in ["img_location_size_1", "small_img_location_size_1", "video_location_size_1"]:
            if posit  not in params[media]:
                params[media][posit] = 0

        img_location_size_1 =\
            params[media].get("img_location_size_1") if params[media].get("img_location_size_1") else 0
        img_location_count_1 =\
            params[media].get("img_location_count_1") if params[media].get("img_location_count_1") else 0
        small_img_location_size_1 =\
            params[media].get("small_img_location_size_1") if params[media].get("small_img_location_size_1") else 0
        small_img_location_count_1 =\
            params[media].get("small_img_location_count_1") if params[media].get("small_img_location_count_1") else 0
        video_location_size_1 =\
            params[media].get("video_location_size_1") if params[media].get("video_location_size_1") else 0
        video_location_count_1 =\
            params[media].get("video_location_count_1") if params[media].get("video_location_count_1") else 0

        params[media]["total_count_1"] = img_location_count_1 + small_img_location_count_1 + video_location_count_1
        params[media]["total_size_1"] = img_location_size_1 + small_img_location_size_1 + video_location_size_1

        if params[media]["total_count_1"] > 0:
            params[media]["avg_size_1"] =\
                float('%.4f' % (float(params[media]["total_size_1"]) / params[media]["total_count_1"]))

            params[media]["percent_img_location_1"] =\
                float('%.4f' % (float(img_location_count_1) / params[media]["total_count_1"]))

            params[media]["percent_small_img_location_1"] =\
                float('%.4f' % (float(small_img_location_count_1) / params[media]["total_count_1"]))

            params[media]["percent_video_location_1"] =\
                float('%.4f' % (float(video_location_count_1) / params[media]["total_count_1"]))
        else:
            params[media]["avg_size_1"] = 0
            params[media]["percent_img_location_1"] = 0
            params[media]["percent_small_img_location_1"] = 0
            params[media]["percent_video_location_1"] = 0

        if img_location_count_1 > 0:
            params[media]["avg_img_location_size_1"] = \
                float('%.4f' % (float(img_location_size_1) / img_location_count_1))
        else:
            params[media]["avg_img_location_size_1"] = 0

        if small_img_location_count_1 > 0:
            params[media]["avg_small_img_location_size_1"] =\
                float('%.4f' % (float(small_img_location_size_1) / small_img_location_count_1))
        else:
            params[media]["avg_small_img_location_size_1"] = 0

        if video_location_count_1 > 0:
            params[media]["avg_video_location_size_1"] =\
                float('%.4f' % (float(video_location_size_1) / video_location_count_1))
        else:
            params[media]["avg_video_location_size_1"] = 0

    for media_other in media_list:
        para = (media_other, year + '-' + month + '-' + day)
        cursor.execute(select_sql_other, para)
        result = cursor.fetchall()

        for item in result:
            params[media_other][item[1] + "_count"] = item[2]
            params[media_other][item[1] + "_size"] = float(item[3])
        for posit in ["img_location_count", "small_img_location_count", "video_location_count"]:
            if posit  not in params[media_other]:
                params[media_other][posit] = 0
        for posit in ["img_location_size", "small_img_location_size", "video_location_size"]:
            if posit  not in params[media_other]:
                params[media_other][posit] = 0

        img_location_size =\
            params[media_other].get("img_location_size") if params[media_other].get("img_location_size") else 0
        img_location_count =\
            params[media_other].get("img_location_count") if params[media_other].get("img_location_count") else 0
        small_img_location_size =\
            params[media_other].get("small_img_location_size") if params[media_other].get("small_img_location_size") else 0
        small_img_location_count =\
            params[media_other].get("small_img_location_count") if params[media_other].get("small_img_location_count") else 0
        video_location_size =\
            params[media_other].get("video_location_size") if params[media_other].get("video_location_size") else 0
        video_location_count =\
            params[media_other].get("video_location_count") if params[media_other].get("video_location_count") else 0

        params[media_other]["total_count"] = img_location_count + small_img_location_count + video_location_count
        params[media_other]["total_size"] = img_location_size + small_img_location_size + video_location_size

    for media_item in params:
        params[media_item]["media"] = media_item
        params[media_item]["news_count"] = _id_media[media_item]
        insert_sql = "insert into {}(media, news_count, total_count, total_count_1, total_size, total_size_1," \
                     " avg_size_1, img_location_count, img_location_count_1, img_location_size, img_location_size_1," \
                     " percent_img_location_1, avg_img_location_size_1," \
                     " small_img_location_count, small_img_location_count_1, small_img_location_size," \
                     " small_img_location_size_1, percent_small_img_location_1, " \
                     "avg_small_img_location_size_1, video_location_count, video_location_count_1," \
                     " video_location_size, video_location_size_1, percent_video_location_1," \
                     " avg_video_location_size_1, datetime)" \
                     " values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                     " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".\
            format("statistics_result_table")

        temp_params = (
            params[media_item]["media"],
            params[media_item]["news_count"],
            params[media_item]["total_count"],
            params[media_item]["total_count_1"],
            params[media_item]["total_size"],
            params[media_item]["total_size_1"],
            params[media_item]["avg_size_1"],
            params[media_item]["img_location_count"],
            params[media_item]["img_location_count_1"],
            params[media_item]["img_location_size"],
            params[media_item]["img_location_size_1"],
            params[media_item]["percent_img_location_1"],
            params[media_item]["avg_img_location_size_1"],
            params[media_item]["small_img_location_count"],
            params[media_item]["small_img_location_count_1"],
            params[media_item]["small_img_location_size"],
            params[media_item]["small_img_location_size_1"],
            params[media_item]["percent_small_img_location_1"],
            params[media_item]["avg_small_img_location_size_1"],
            params[media_item]["video_location_count"],
            params[media_item]["video_location_count_1"],
            params[media_item]["video_location_size"],
            params[media_item]["video_location_size_1"],
            params[media_item]["percent_video_location_1"],
            params[media_item]["avg_video_location_size_1"],
            year + '-' + month + '-' + day,
                       )

        cursor.execute(insert_sql, temp_params)
        db.commit()

    cursor.close()
    db.close()


def compare_table(year, month, day, count_dict, only_osslog_count):
    params = (count_dict["common_count"], count_dict["only_mongo_count"],
              only_osslog_count, year + '-' + month + '-' + day)
    db = get_mysql_db()
    cursor = db.cursor()
    sql = 'insert into {}(common_count, only_mongo_count, only_osslog_count, datetime) values(%s, %s, %s, %s)'\
          .format("statistics_compare_table")
    cursor.execute(sql, params)
    db.commit()

    cursor.close()
    db.close()



def percent_table(year, month, day):
    db = get_mysql_db()
    cursor = db.cursor()
    sql = 'select media, news_count, total_count_1, img_location_count_1, small_img_location_count_1, ' \
          'video_location_count_1 from {} where datetime=%s'.format('statistics_result_table')

    insert_sql = 'insert into {}(media, news_percent, data_percent, img_location_percent,' \
                 ' small_img_location_percent, video_location_percent, datetime) values(%s, %s, %s, %s, %s, %s, %s)'\
                 .format("statistics_percent_table")

    cursor.execute(sql,  year + '-' + month + '-' + day)
    result = cursor.fetchall()
    total_news_count = 0
    total_data_count = 0
    total_img_location_count = 0
    total_small_img_location_count = 0
    total_video_location_count = 0
    media_count_dict = {}
    for item in result:
        media_count_dict[item[0]] = {}
        total_news_count += item[1]
        media_count_dict[item[0]]["news_count"] = item[1]
        total_data_count += item[2]
        media_count_dict[item[0]]["total_count"] = item[2]
        total_img_location_count += item[3]
        media_count_dict[item[0]]["img_location_count"] = item[3]
        total_small_img_location_count += item[4]
        media_count_dict[item[0]]["small_img_location_count"] = item[4]
        total_video_location_count += item[5]
        media_count_dict[item[0]]["video_location_count"] = item[5]

    for key in media_count_dict:
        news_percent = '%.4f' % (float(media_count_dict[key]["news_count"]) / total_news_count)

        data_percent = '%.4f' % (float(media_count_dict[key]["total_count"]) / total_data_count)

        img_location_percent = '%.4f' % (float(media_count_dict[key]["total_count"]) / total_img_location_count)

        small_img_location_percent =\
            '%.4f' % (float(media_count_dict[key]["small_img_location_count"]) / total_small_img_location_count)

        video_location_percent =\
            '%.4f' % (float(media_count_dict[key]["video_location_count"]) / total_video_location_count)

        params = (
                key,
                float(news_percent),
                float(data_percent),
                float(img_location_percent),
                float(small_img_location_percent),
                float(video_location_percent),
                year + '-' + month + '-' + day,
        )
        cursor.execute(insert_sql, params)
        db.commit()

    cursor.close()
    db.close()


def size_table(year, month, day, common_size, only_osslog_size, danews_data_size, donews_test1_size, wangleilog_size):
    db = get_mysql_db()
    cursor = db.cursor()
    insert_sql = 'insert into {}(common_size, only_osslog_size, datetime,' \
                 ' danews_data_size, donews_test1_size, wangleilog_size) values(%s, %s, %s, %s, %s, %s)'\
        .format("statistics_size_table")
    params = (common_size, only_osslog_size, year + '-' + month + '-' + day,
              danews_data_size, donews_test1_size, wangleilog_size)
    cursor.execute(insert_sql, params)
    db.commit()

    cursor.close()
    db.close()


def result_table_main(year, month, day,
                      count_dict, only_osslog_count,
                      _id_media, common_size, only_osslog_size,
                      danews_data_size, donews_test1_size, wangleilog_size):

    compare_table(year=year, month=month, day=day, count_dict=count_dict, only_osslog_count=only_osslog_count)
    result_table(year=year, month=month, day=day, _id_media=_id_media)
    percent_table(year=year, month=month, day=day)
    size_table(year=year, month=month, day=day,
               common_size=common_size,
               only_osslog_size=only_osslog_size,
               danews_data_size=danews_data_size,
               donews_test1_size=donews_test1_size,
               wangleilog_size=wangleilog_size)

if __name__ == '__main__':
    percent_table(year='2017', month='09', day='20')
