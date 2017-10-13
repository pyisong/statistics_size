# -*- coding:utf-8 -*-
from settings import get_mongo, MONGO_PARA_N


def distinct_mongo_to_file(year, month, day):
    mongo_to_file = open("target_file/mongo_to_file" + year + month + day + ".txt", "r")
    mongo_to_file_lines = mongo_to_file.readlines()
    distinct_lines_count = 0

    client = get_mongo(MONGO_PARA_N)
    dmt_list = client['dmt_jh_data'].collection_names()
    crawled_list = client['crawled_TTH_web_page'].collection_names()
    crawled_list.remove(u"miaopai")
    media_list = dmt_list + crawled_list

    _id_media = dict(zip(media_list, [0 for i in range(len(media_list))]))
    url_dict = {}
    _id_dict = {}
    repetition_count = dict(zip(media_list, [0 for i in range(len(media_list))]))

    with open("target_file/distinct_mongo_to_file" + year + month + day + ".txt", "w") as f:
        for line in mongo_to_file_lines:
            line_list = line.split()
            _id = line_list[0].strip()
            media = line_list[1].strip()
            url = line_list[3].strip()

            if media not in ["crawl_all_10_page_result_fail_data", "donews_fail_data"] and url != "None":
                if url not in url_dict:
                    url_dict[url] = media
                    f.write(line)
                    distinct_lines_count += 1
                else:
                    repetition_count[media] += 1

            if _id not in _id_dict:
                _id_dict[_id] = media

        mongo_to_file.seek(0, 0)
        mongo_to_file_lines_again = mongo_to_file.readlines()
        for line_again in mongo_to_file_lines_again:
            line_list = line_again.split()
            media = line_list[1].strip()
            url = line_list[3].strip()
            if media in ["crawl_all_10_page_result_fail_data", "donews_fail_data"] and url != "None":
                    if url not in url_dict:
                        url_dict[url] = media
                        f.write(line_again)
                        distinct_lines_count += 1
                    else:
                        repetition_count[media] += 1

    for item in _id_dict:
        _id_media[_id_dict[item]] += 1

    result = {"news_count": len(_id_dict),
              "every_media_news_count": _id_media,
              "repetition_count": repetition_count,
              "distinct_lines_count": distinct_lines_count}
    return result


if __name__ == "__main__":
    pass
