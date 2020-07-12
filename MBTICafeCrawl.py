import time 
import pandas as pd # pip install pandas
import os 
import csv
import pymysql # pip install pymysql
import traceback
import lxml
import requests
from requests_html import HTML, HTMLSession
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs # pip install bs4
from fake_useragent import UserAgent # pip install fake_useragent
from datetime import datetime
# Preventing Unicode error
import sys 
import io
import re 
import pyperclip
import uuid 
import rsa 
import lzstring 
from retrying import retry 
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Fake user agent
ua = UserAgent()
ua_random = ua.random 
# save output message in output.txt
# 한글 깨지지 않게 하려면 utf-8 사용
op = open('output_requests.txt','w', encoding = 'utf-8')
sys.stdout = open('output_requests.txt','a', encoding = 'utf-8')
sys.stderr = open('output_requests.txt','a', encoding = 'utf-8')
print('start crawling, time: ', datetime.now())
def sql2csv(table_name):
    # save sql table as csv file
    # 한글 깨지지 않게 하려면 utf-8 사용
    results = pd.read_sql_query('select * from ' + str(table_name), conn)
    results.to_csv(str(table_name) + '.csv', index=False, encoding='utf-8-sig')


def naver_login(id, pw, driver):
    # # driver, sign in in Naver
    # to avoid captcha, use clipboard
    pyperclip.copy(id) #네이버 아이디 
    driver.find_element_by_id('id').send_keys(Keys.CONTROL, 'v')
    pyperclip.copy(pw) #네이버 비밀번호 
    driver.find_element_by_id('pw').send_keys(Keys.CONTROL, 'v')
    driver.find_element_by_css_selector('#log\.login').click()
# db connect and select
conn = pymysql.connect(host='localhost', user = 'root', password='password', db = 'iab_project',charset = 'utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor) 
# clean up SQL file
curs.execute("USE iab_project")
# curs.execute("DELETE FROM mbti_req")
# curs.execute('ALTER TABLE `mbti_req` AUTO_INCREMENT=1')
conn.commit()
myId = 'myId'
myPw = 'myPw'
base_url = 'https://m.cafe.naver.com' 

opts = Options()
opts.add_argument("user-agent=" + ua_random)
opts.add_argument('log-level=2')
# opts.add_argument('headless')
# opts.add_argument("disable-gpu")    # to avoid bug in headless chrome

# STJ, NTJ, SFJ, NFJ, STP, SFP, NTP, NFP
boardIDs = [11, 12, 13, 14, 15, 16, 17, 18]
types = ['STJ', 'NTJ', 'SFJ', 'NFJ', 'STP', 'SFP', 'NTP', 'NFP']
cnt = 0 # number of collected data
typeIE = 'E' # E or I

csv_id = 0  # id of each data
sqlInsert = "INSERT INTO mbti_req (sentence, type) VALUES (%s,%s)" 
# regex to find url from search result
url_regex = re.compile(r'https://m\.cafe\.naver\.com/ArticleRead\.nhn\?clubid=11856775&menuid=[0-9]+&articleid=[0-9]+')

def store_content(mbti_type, quest, driver):
    def store_func(mbti_type, driver, i):
        #제목 추출 
        if driver.current_url.find('https://nid.naver.com/nidlogin.login?svctype=262144') > -1:
            naver_login(myId, myPw, driver)
        try:
            WebDriverWait(driver, 1).until(EC.url_changes(search_url))
        except TimeoutException:
            pass
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR,'#ct > div:nth-child(1) > div > h2'))
            )
        title = driver.find_element_by_css_selector('#ct > div:nth-child(1) > div > h2').text
        #내용 추출 
        if driver.current_url.find('https://nid.naver.com/nidlogin.login?svctype=262144') > -1:
            naver_login(myId, myPw, driver)
        WebDriverWait(driver, 10).until(
            # check_login_sentence
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR,'#postContent > div:nth-of-type(1)'))
            )
        content_div = driver.find_element_by_css_selector('#postContent > div:nth-of-type(1)')
        content_tag_list = content_div.find_elements_by_css_selector("*")
        # list to store content
        content_tags = []
        content_tags.append(content_div)
        content_tags.extend(content_tag_list)
            
        content = '|||'.join([ tags.text for tags in content_tags ]) 
        val = (str(title) + ' ' + str(content), mbti_type) 
        curs.execute(sqlInsert, val) 
        conn.commit() # store in sql
    
    i = 0
    try:    
        search_url = url_regex.search(base_url + quest).group()
        driver.get(search_url)
        store_func(mbti_type, driver, i)
        
    except TimeoutException:
        if i < 5:
            if driver.current_url.find('https://nid.naver.com/nidlogin.login?svctype=') > -1:
                naver_login(myId, myPw, driver)
                i += 1
                store_func(mbti_type, driver, i)
        else:
            print('TimeoutException')
            print('current url: ', driver.current_url)
            traceback.print_exc()
        # exit()
    except NoSuchElementException:
        if i < 5:
            if driver.current_url.find('https://nid.naver.com/nidlogin.login?svctype=') > -1:
                naver_login(myId, myPw, driver)
                i += 1
                store_func(mbti_type, driver, i)
        else:
            print('NoSuchElementException')
            print('current url: ', driver.current_url)
            traceback.print_exc()
        # exit()
    except KeyboardInterrupt:
        exit()
    except : 
        print('Exception')
        traceback.print_exc()
        # exit()
    # finally:
    #     driver.quit()
# naver login on requests
def encrypt(key_str, uid, upw):
    def naver_style_join(l):
        return ''.join([chr(len(s)) + s for s in l])

    sessionkey, keyname, e_str, n_str = key_str.split(',')
    e, n = int(e_str, 16), int(n_str, 16)

    message = naver_style_join([sessionkey, uid, upw]).encode()

    pubkey = rsa.PublicKey(e, n)
    encrypted = rsa.encrypt(message, pubkey)

    return keyname, encrypted.hex()


def encrypt_account(uid, upw):
    key_str = requests.get('https://nid.naver.com/login/ext/keys.nhn').content.decode("utf-8")
    return encrypt(key_str, uid, upw)


def naver_session(nid, npw):
    encnm, encpw = encrypt_account(nid, npw)

    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[500, 502, 503, 504]
    )
    s.mount('https://', HTTPAdapter(max_retries=retries))
    request_headers = {
        'User-agent': ua.random
    }
    bvsd_uuid = uuid.uuid4()
    encData = '{"a":"%s-4","b":"1.3.4","d":[{"i":"id","b":{"a":["0,%s"]},"d":"%s","e":false,"f":false},{"i":"%s","e":true,"f":false}],"h":"1f","i":{"a":"Mozilla/5.0"}}' % (bvsd_uuid, nid, nid, npw)
    bvsd = '{"uuid":"%s","encData":"%s"}' % (bvsd_uuid, lzstring.LZString.compressToEncodedURIComponent(encData))

    resp = s.post('https://nid.naver.com/nidlogin.login', data={
        'svctype': '0',
        'enctp': '1',
        'encnm': encnm,
        'enc_url': 'http0X0.0000000000001P-10220.0000000.000000www.naver.com',
        'url': 'www.naver.com',
        'smart_level': '1',
        'encpw': encpw,
        'bvsd': bvsd
    }, headers=request_headers)
    print(resp.content.decode("utf-8"))
    finalize_url = re.search(r'location\.replace\("([^"]+)"\)', resp.content.decode("utf-8")).group(1)
    s.get(finalize_url)

    return s

if __name__ == '__main__':
    driver = webdriver.Chrome('D:/chromedriver_win32/chromedriver.exe', options=opts)
    with naver_session(myId, myPw) as s:
        params = {
            'search.clubid' : '11856775',
            'search.menuid' : '1',
            'search.boardtype' : 'L',
            'search.query' : 'INTJ',
            'search.searchBy' : '3',
            'search.sortBy' : 'date',
            'search.option' : '0',
            'search.page' : '1'
        }
        for i in range(5, 8):  # i: current type without I/E
            params['search.menuid'] = str(boardIDs[i])
            for j in range(2):  # change I/E
                # id 기준 MBTI 유형 검색
                typeIE = 'E' if j == 0 else 'I'
                mbti_type = typeIE + types[i]
                print('type:' + mbti_type)
                params['search.query'] = mbti_type
                # # 성격 유형 검색
                for page in range(1, 301): # 게시글 페이지 수 
                    quest_urls = []  
                    try : # add personal conditions 
                        params['search.page'] = page
                        # search
                        html = s.get(base_url + '/ArticleSearchList.nhn', params = params).text
                        soup = bs(html, 'lxml')
                        article_num = soup.select_one('#ct > div.search_contents > div.search_sort > div.sort_l > span')
                        print('number of articles: ', article_num)
                        quest_list = soup.select('ul.list_writer > li')
                        # search for other type if no more data found in current type
                        if len(quest_list) == 0:
                            break
                        print('length of quest_list: ', len(quest_list))
                        quest_urls = [ q.select_one('div:nth-of-type(1) > a:nth-of-type(2)').attrs['href'] for q in quest_list]
                        # get content from each link
                        # pool.starmap(store_content, quest_urls)
                        driver.get('https://nid.naver.com/nidlogin.login?svctype=262144&url=http://m.naver.com/') # id & pw 입력 
                        naver_login(myId, myPw, driver)
                        for quest in quest_urls:
                            store_content(mbti_type, quest, driver)
                            # end of forEach quest
                    except TimeoutException:
                        print('TimeoutException')
                        traceback.print_exc()
                    except NoSuchElementException:
                        print('NoSuchElementException')
                        traceback.print_exc()
                    except KeyboardInterrupt:
                        exit()
                    except : 
                        print('Exception')
                        traceback.print_exc()
                        # close driver after using to avoid memory leak
                    print([page]) #진행상황을 알 수 있음 
    # save sql as csv file
    op.close()
    sql2csv('mbti_req')
    # close driver
    # close connection to sql
    conn.close()
    driver.quit()
    # close pool
    print('finish crawling, time: ', datetime.now())