#!/usr/bin/python
# -*- coding: UTF-8 -*-
import email
import mimetypes
import smtplib
import os
import xlwt
from email.header import Header
from settings import get_mysql_db


def send_email(year, month, day):
    # qbiayrpxpkbebhab
    sender = '421414186@qq.com'
    receivers = [
        'zhujianwei@donews.com',
        'wanshitao@donews.com',
        'jijiazhen@donews.com',
        'lichenguang@donews.com',
        'chenkangjian@donews.com',
        'zhanyanjun@donews.com',
        'yangliu@donews.com',
        'shuyong@donews.com',
    ]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    username = "421414186@qq.com"
    password = "odbtbqrfpqrmbhbj"

    file_name = 'csv_file/statistics_result_table' + year + '-' + month + '-' + day + '.csv'
    # 构造MIMEMultipart对象做为根容器
    main_msg = email.MIMEMultipart.MIMEMultipart()

    # 构造MIMEText对象做为邮件显示内容并附加到根容器
    text_msg = email.MIMEText.MIMEText(year + '-' + month + '-' + day + "统计结果表", _charset="utf-8")
    main_msg.attach(text_msg)

    # 构造MIMEBase对象做为文件附件内容并附加到根容器
    ctype, encoding = mimetypes.guess_type(file_name)
    if ctype is None or encoding is not None:
        ctype = 'application/octet-stream'
    maintype, subtype = ctype.split('/', 1)
    file_msg = email.MIMEImage.MIMEImage(open(file_name, 'rb').read(), subtype)

    # 设置附件头
    basename = os.path.basename(file_name)
    file_msg.add_header('Content-Disposition', 'attachment', filename=basename)  # 修改邮件头
    main_msg.attach(file_msg)

    subject = year + '-' + month + '-' + day + '统计结果表'
    main_msg['Subject'] = Header(subject, 'utf-8')
    main_msg['From'] = 'zhujianwei@donews.com'
    main_msg['To'] = ','.join(receivers)
    smtp = smtplib.SMTP_SSL('smtp.qq.com', 465)
    fullText = main_msg.as_string()
    smtp.login(username, password)
    smtp.sendmail(sender, receivers, fullText)
    smtp.quit()


def export(year, month, day):
    table_name = 'statistics_result_table'
    conn = get_mysql_db()
    cursor = conn.cursor()
    sql1 = """
        select
            media as 库名,
            news_count as 新闻数,
            total_count as 总资源数,
            total_count_1 as Mongo总资源数,
            format(total_size/1024/1024/1024, 2) as OSS日志存储总量G,
            format(total_size_1/1024/1024/1024, 2) as Mongo共有数据存储总量G,
            format(avg_size_1/1024/1024, 2) as 平均大小M,
            format(total_size_1*100/(select sum(total_size_1) from statistics_result_table where
             datetime ='{}'), 2) as 百分占比
        from statistics_result_table where datetime = %s;
    """.format(year + '-' + month + '-' + day)


    sql2 = """
        select
        media as 库名,
        news_count as 新闻数,
        img_location_count_1 as 图片数据量,
        format(img_location_size_1/1024/1024/1024, 2) as 图片大小G,
        small_img_location_count_1 as 缩略图数据量,
        format(small_img_location_size_1/1024/1024/1024, 2) as 缩略图大小G,
        video_location_count_1 as 视频数据量,
        format(video_location_size_1/1024/1024/1024, 2) as 视频大小G
      from {} where datetime = %s;
    """.format("statistics_result_table")

    sql3 = """
            select
            media as 库名,
            ifnull(format(img_location_count_1/news_count, 2), 0) as 平均图片数量,
            ifnull(format(small_img_location_count_1/news_count, 2), 0) as 平均缩略图数量,
            ifnull(format(video_location_count_1/news_count, 2), 0) as 平均视频数量,
            ifnull(format(img_location_size_1/img_location_count_1/1024, 2), 0) as 单条数据平均图片大小kb,
            ifnull(format(small_img_location_size_1/small_img_location_count_1/1024, 2), 0) as 单条数据平均缩略图大小kb,
            ifnull(format(video_location_size_1/video_location_count_1/1024, 2), 0) as 单条数据视频大小kb,
            format(ifnull(img_location_size_1/img_location_count_1/1024, 0) + 
            ifnull(small_img_location_size_1/small_img_location_count_1/1024, 0) + 
            ifnull(video_location_size_1/video_location_count_1/1024, 0), 2) as 平均单条数据
        from {} where datetime = %s;
    """.format("statistics_result_table")

    workbook = xlwt.Workbook()
    count = 1
    for sql in [sql1, sql2, sql3]:
        cursor.execute(sql, year + '-' + month + '-' + day)
        # 重置游标的位置
        # cursor.scroll(0,mode='absolute')
        # 搜取所有结果
        results = cursor.fetchall()
        # 获取MYSQL里面的数据字段名称
        fields = cursor.description
        sheet = workbook.add_sheet('table_' + table_name + str(count), cell_overwrite_ok=True)
        count += 1

        # 写上字段信息
        for field in range(0,len(fields)):
            sheet.write(0, field, fields[field][0])

        # 获取并写入数据段信息
        row = 1
        col = 0
        for row in range(1,len(results)+1):
            for col in range(0,len(fields)):
                value = u'%s' % results[row-1][col]
                sheet.write(row, col, value)

        workbook.save('csv_file/statistics_result_table' + year + '-' + month + '-' + day + '.csv')


# 结果测试
if __name__ == "__main__":
    export(year='2017', month='10', day='09')
    send_email(year='2017', month='10', day='09')
