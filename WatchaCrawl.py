import pandas as pd
import pymysql
import sys
import requests
import base64
import json
import os
from bs4 import BeautifulSoup


def getMovieComment(movieCode):
    try:
        
        CONTENT_URL = "https://pedia.watcha.com/ko-KR/contents/{}".format(movieCode)
        resp = requests.get(CONTENT_URL)
        soup = BeautifulSoup(resp.content, "lxml")
        name = soup.find("div", class_ = "css-13h49w0-PaneInner").h1.text
        year = soup.find("div", class_ = "css-13h49w0-PaneInner").div.text[:4]
        movieName = "{}({})".format(name, year)
        return movieName
    except:
        print("getMovieName error")
        sys.exit(1)
def sql2csv(table_name):
    # save sql table as csv file
    # 한글 깨지지 않게 하려면 utf-8 사용
    results = pd.read_sql_query('select * from ' + str(table_name), conn)
    results.to_csv(str(table_name) + '.csv', index=False, encoding='utf-8-sig')
if __name__ == '__main__':
    table_name = 'watcha_comments'
    conn = pymysql.connect(host='localhost', user = 'root', password='password', db = 'iab_project',charset = 'utf8mb4') 
    curs = conn.cursor(pymysql.cursors.DictCursor) 
    curs.execute("USE iab_project")
    curs.execute('SHOW TABLES')
    tables = curs.fetchall()
    print(tables)
    table_exists = False
    for table in tables:
        if table['Tables_in_iab_project'] == table_name:
            table_exists = True
    if not table_exists:    # int(size) : 열의 너비가 size인 int
        curs.execute('CREATE TABLE `watcha_comments` (' + 
            '`id` int(11) NOT NULL AUTO_INCREMENT, ' + \
            '`name` varchar(30) NOT NULL, ' + \
            '`rating` int(2) NOT NULL, ' + \
            '`comment` text NOT NULL, ' + \
            '`category` varchar(10) NOT NULL, '
            'PRIMARY KEY (`id`)' + \
            ') CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci'
            )
        conn.commit()
    curs.execute('USE iab_project')
    curs.execute("DELETE FROM " + table_name)
    curs.execute('ALTER TABLE `' + table_name + '` AUTO_INCREMENT=1')
    conn.commit()
    headers = { 
    'accept': "application/vnd.frograms+json;version=20",
    'accept-encoding': "gzip, deflate, br",
    'accept-language': "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    'cache-control': "no-cache,no-cache",
    'cookie': "_ga=GA1.2.1784511024.1585672339; _gid=GA1.2.1170018568.1594139359; _c_drh=true; _gat=1; _s_guit=b940584f6c338f09faef81986606f6ede562b8554510679dda03f512b43a; Watcha-Web-Client-Language=ko; _guinness_session=sv7%2FKtsNPjrMOHQ%2FVwggfJ8J7OP0dXTjaEjVSh3GeykPZVSKW082ymy4WBzNthtDY4hSHrkCZUXptm4D0G8KOoWdllssE%2FbAXAQZGbBdgryPI6cBB7eiEfbhsn5o--d1je2zaGm3nVZhwt--zZ4a3yaxRHg9%2F7vfhjeFfQ%3D%3D",
    'dnt': "1",
    'origin': "https://pedia.watcha.com",
    'pragma': "no-cache",
    'referer': "https://pedia.watcha.com/ko-KR/contents/md6B3Ad/comments",
    'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    'x-watcha-client': "Watcha-WebApp",
    'x-watcha-client-language': "ko",
    'x-watcha-client-region': "KR",
    'x-watcha-client-version': "2.0.0"
    }
    params = {
        'filter' : 'all',
        'order' : 'popular',
        'page' : 1,
        'size' : 20
    }
    # f = open('comments.json', 'w')
    # f.write(json.dumps(json.loads(response.content, encoding='utf-8')))
    category = ''
    with open('movieCodes.txt', 'r', encoding='utf-8') as movieCodeFile:
        try:
            for line in movieCodeFile.readlines():
                line = line.rstrip('\n')
                if (line == 'best100') or (line == 'random') or (line == 'selected'):
                    category = line
                else:
                    movieName, movieCode = line.split('|||')
                    next_uri = 'https://api-pedia.watcha.com/api/contents/' + movieCode + '/comments?filter=all&order=popular&page=1&size=20'
                    sqlInsert = 'INSERT INTO ' + table_name + ' (name, rating, comment, category) VALUES (%s,%s,%s,%s)'
                    with requests.Session() as s:
                        while next_uri:
                            response = s.request("GET", next_uri, headers = headers)
                            response_content = json.loads(response.content)
                            for data in response_content['result']['result']:
                                rating = data['user_content_action']['rating']
                                if rating != None:
                                    comment = data['text']
                                    val = (movieName, rating, comment, category)
                                    curs.execute(sqlInsert, val)
                                    conn.commit()
                            next_uri = 'https://api-pedia.watcha.com' + response_content['result']['next_uri'] if response_content['result']['next_uri'] else None
        except:
            print([line])
            print([movieName, rating, comment, category])
            print(val)
            exit()
    

    sql2csv('watcha_comments')
    conn.close()