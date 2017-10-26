from settings import get_mongo, MONGO_PARA_N, MyThread


def compare_history(content_only_mongo_lines, f, year, month, day):
    client = get_mongo(MONGO_PARA_N)
    not_found_count = 0
    collection_img = client['JoinMongoCrawledHistory']["article_photo_hash"]
    collection_video = client['JoinMongoCrawledHistory']["article_video_hash"]
    with open("target_file/history_to_only_mongo" + year + month + day + ".txt", "a") as h_f:
        for line in content_only_mongo_lines:
            _id = line.split()[0].strip()
            url = line.split()[1].strip()
            media = line.split()[3].strip()
            result_img = collection_img.find({"saved_img_url": url})
            result_img_count = result_img.count()
            result_video = collection_video.find({"video_path": url})
            result_video_count = result_video.count()
            if result_img_count == 0 and result_video_count == 0:
                f.write(_id + '\t' + url + '\t' + media + '\n')
                not_found_count += 1
            else:
                if result_img_count > 0:
                    date_time = result_img[0].get("date")
                else:
                    date_time = result_video[0].get("date")
                h_f.write(_id + '\t' + url + '\t' + media + '\t' + date_time + '\n')
    return not_found_count


def find_only_mongo(year, month, day):
    f_only = open("target_file/only_mongo" + year + month + day + ".txt", "r")
    not_found_count = 0
    with open("target_file/not_found_only_mongo" + year + month + day + '.txt', 'w') as f:
        content_only_mongo_lines = f_only.readlines(1000*1024)
        thread_list = []
        while len(content_only_mongo_lines) > 0:
            t = MyThread(func=compare_history, args=(content_only_mongo_lines, f, year, month, day))
            thread_list.append(t)
            content_only_mongo_lines = f_only.readlines(1000*1024)

        for item in thread_list:
            item.start()
        for item in thread_list:
            if item.get_result():
                count = item.get_result()
            else:
                count = 0
            if count:
                not_found_count += count

            item.join()
    f_only.close()
    return not_found_count


if __name__ == "__main__":
    find_only_mongo(year='2017', month='10', day='08')
