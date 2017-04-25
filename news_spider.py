#coding=utf-8

# Script Name:      news_spider.py
# Author:           lszero
# Created:          November 12, 2015
# Last Modified:    December 29, 2015
# Version:          1.0
# Description:      The current version only support 'www.chinanews.com'(中国新闻网).

import sys,time,datetime
import MySQLdb
import requests
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf-8')

site_source_list = ["chinanews"]

class NewsSpider:
    def connectDB(self, db_host, db_user, db_passwd, db_name, db_port):
        try:
            self.conn = MySQLdb.connect(host=db_host,user=db_user,passwd=db_passwd,db=db_name,port=db_port, use_unicode=True, charset="utf8")
            self.cur = self.conn.cursor()
            print "Connect database successfully."
        except MySQLdb.Error, e:
            print "MySQL Error %d: %s" % (e.args[0], e.args[1])

    def closeDB():
        self.cur.close()
        self.conn.close()
        print "Close database."

    def getNewsToDB(self, str_start_time, str_end_time):
        count = 0
        date_range = self.dateRange(str_start_time, str_end_time)
        
        for date in date_range:
            self.strYear = str(date.year)
            if date.month < 10:
                self.strMonth = "0" + str(date.month)
            else:
                self.strMonth = str(date.month)

            if date.day < 10:
                self.strDay = "0" + str(date.day)
            else:
                self.strDay = str(date.day)

            for source in site_source_list:
                roll_page_url = self.getRollPageUrl(source, self.strYear, self.strMonth, self.strDay)
                n = self.getNewsInfoFromRollPage(source, roll_page_url)
                count += n

        print str(count) + " data added."

    def getNewsInfoFromRollPage(self, site_source, roll_page_url):
        count = 0
        response = requests.get(roll_page_url)
        response.encoding = "gb18030"
        soup = BeautifulSoup(response.text, "html.parser")

        if site_source == "chinanews":
            newsInfoList = soup.find_all("div", class_="dd_lm")
            for info in newsInfoList:
                theme = info.text[1:-1]
                info = info.next_sibling.next_sibling
                title = info.text
                url = info.a["href"]
                if url[:4] != "http":   #前面自带http的是视频新闻，跳过
                    url = "http://www.chinanews.com" + url

                print url

                b_checked = False
                sql_query = "select * from news where source='" + site_source + "' and url='" + url + "'"
                self.cur.execute(sql_query)
                for item in self.cur.fetchall():
                    b_checked = True
                    break

                if b_checked == True:
                    continue

                news = self.getNewsFromURL(url, site_source)
                content = news[1]

                # print title
                # print theme
                # print content

                if content == "":
                    continue

                time = info.next_sibling.text
                tmp_time = time.split(' ')
                datetime = self.strYear + "-" + self.strMonth + "-" + self.strDay + " " + tmp_time[1] + ":00"

                try:
                    sql_insert = "insert into news(title,content,theme,date,source,url) values(%s,%s,%s,%s,%s,%s)"
                    self.cur.execute(sql_insert, (title, content, theme, datetime, site_source,url));
                    self.conn.commit()
                    count += 1
                except MySQLdb.Error, e:
                    print "MySQL Error %d: %s" % (e.args[0], e.args[1])

        return count
        
    def getNewsFromURL(self, url, site_source='chinanews'):
        response = requests.get(url)
        response.encoding = "gb18030"
        soup = BeautifulSoup(response.text, "html.parser")

        title = soup.find("title").string
        content = ""

        if site_source == "chinanews":
            post = soup.find("div", class_="left_zw")
            if post == None:
                return ""

            pList = post.find_all("p")
            for p in pList:
                for sub_node in p.children:
                    if sub_node.string != None:
                        content += sub_node.string
                        
        return title, content


    def getRollPageUrl(self, site_source, strYear, strMonth, strDay):
        if site_source == "chinanews":
            return "http://www.chinanews.com/scroll-news/" + strYear + "/" + strMonth + strDay + "/news.shtml"

    def dateRange(self, str_start_time, str_end_time):
        t1 = str_start_time.split('-')
        t2 = str_end_time.split('-')
        start_time = datetime.datetime(int(t1[0]), int(t1[1]), int(t1[2]))
        end_time = datetime.datetime(int(t2[0]), int(t2[1]), int(t2[2]))
        for i in range(int((end_time-start_time).days) + 1):
            yield start_time + datetime.timedelta(i)


def main():
    '''
    spider = NewsSpider()
    news = spider.getNewsFromURL('http://www.chinanews.com/cj/2015/12-29/7693062.shtml')
    print news[0]   # title
    print news[1]   # content
    '''
    
    t1 = time.time()
    spider = NewsSpider()
    spider.connectDB(db_host='localhost', db_user='root', db_passwd='root', db_name='ir_news', db_port=3306)
    spider.getNewsToDB("2015-12-20", "2015-12-21")
    spider.closeDB()
    t2 = time.time()
    print "Execution time: %.3f s." % (t2 - t1)

if __name__ == '__main__':
    main()
