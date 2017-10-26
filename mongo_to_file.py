# -*- coding:utf-8 -*-
import gc
from settings import get_mysql_db, get_mongo
from bson import ObjectId


def mongo_to_file(paras, start_time, end_time, year, month, day):
    count = 0
    path_none_count = 0
    total_data_count = 0
    none_url_count = 0
    position_count_dict = {}
    not_equal_dict = {}
    client = get_mongo(paras)
    dmt_list = client['dmt_jh_data'].collection_names()
    crawled_list = client['crawled_TTH_web_page'].collection_names()
    crawled_list.remove(u"miaopai")

    mongo_info = (("dmt_jh_data", dmt_list,),
                  ("crawled_TTH_web_page", crawled_list))

    with open("target_file/mongo_to_file" + year + month + day + ".txt", "w+") as f:
        for db, colls in mongo_info:
            for coll in colls:
                dic = {"timestamp": {"$gte": start_time, "$lte": end_time}}
                collection = client[db][coll]
                find_result = collection.find(dic)
                result_count = find_result.count()
                if result_count > 0:
                        for rec in find_result:

                            img_count = 0
                            small_img_count = 0
                            video_count = 0

                            img_location_count =\
                                rec.get("img_location_count") if rec.get("img_location_count") else 0
                            small_img_location_count =\
                                rec.get("small_img_location_count") if rec.get("small_img_location_count") else 0
                            video_location_count =\
                                rec.get("video_location_count") if rec.get("video_location_count") else 0

                            _id = rec.get("_id")
                            if isinstance(_id, ObjectId):
                                _id = str(ObjectId(_id))

                            position_count_dict[_id] =\
                                img_location_count + small_img_location_count + video_location_count

                            timestamp = rec.get("timestamp")
                            video_l = rec.get("video_location", [])
                            video_l = video_l if video_l else []
                            for ele in video_l:
                                url = ele.get("video_path")
                                if url:
                                    url = url
                                else:
                                    url = "None"
                                    none_url_count += 1
                                f.write( _id + '\t' + coll + '\t' + "video_location" + "\t" + url
                                         + "\t" + timestamp + '\n')
                                count += 1
                                video_count += 1

                            img_location = rec.get("img_location")
                            img_location = img_location if img_location else []
                            for ele in img_location:
                                url = ele.get("img_path")
                                if url:
                                    url = url
                                else:
                                    url = "None"
                                    none_url_count += 1
                                f.write(_id + '\t' + coll + '\t' + "img_location" + "\t" + url
                                        + "\t" + timestamp + '\n')
                                count += 1
                                img_count += 1

                            small_img_location = rec.get("small_img_location")
                            small_img_location = small_img_location if small_img_location else []

                            for ele in small_img_location:
                                url = ele.get("img_path")
                                if url:
                                    url = url
                                else:
                                    url = "None"
                                    none_url_count += 1
                                f.write(_id + '\t' + coll + '\t' + "small_img_location" + "\t" + url
                                        + "\t" + timestamp + '\n')
                                count += 1
                                small_img_count += 1

                            if img_count != img_location_count or small_img_count != small_img_location_count\
                                    or video_count != video_location_count:

                                not_equal_dict[_id] = coll

                            if video_l == [] and img_location == [] and small_img_location == []:
                                path_none_count += 1

    for key in position_count_dict:
        total_data_count += position_count_dict[key]

    result = {"write_lines": count, "total_data_count": total_data_count, "none_url_count": none_url_count,
              "path_none_count": path_none_count, "not_equal_count": len(not_equal_dict)}
    return result, not_equal_dict


def save_not_equal_data(paras, start_time, end_time, year, month, day):
    result, not_equal_dict = mongo_to_file(paras=paras, start_time=start_time, end_time=end_time,
                                           year=year, month=month, day=day)
    with open("target_file/not_equal_data" + year + month + day + ".txt", 'w') as f:
        for k, v in not_equal_dict.items():
            f.write(k + '\t' + v + '\n')

    del not_equal_dict
    gc.collect()

    return result


def insert_mysql(year, month, day):
    db = get_mysql_db()
    cursor = db.cursor()
    sql = "insert into {}(_id)" \
                     " values (%s)".format("log_to_mongo0911_id")
    f = open("target_file/mongo_to_file" + year + month + day + ".txt", "r")
    content = f.readlines()

    lis = list()
    for line in content:
        line_list = line.split()
        _id = line_list[0].strip()
        param = (_id,)
        lis.append(param)
        if len(lis) >= 2000:
            cursor.executemany(sql, lis)
            db.commit()
            lis = list()
    cursor.executemany(sql, lis)
    db.commit()
    f.close()

if __name__ == "__main__":
    pass
