import requests
import json
import os
from bs4 import BeautifulSoup as bs
import multiprocessing



def find_movieCode(movieString):
    movieName = ''
    index = 1
    if movieString.find('|||') > -1:
        movieName, index = movieString.split('|||')
        index = int(index)
    else:
        movieName = movieString
    SEARCH_URL = "https://pedia.watcha.com/ko-KR/search?"
    params = {
            "query" : movieName,
        }
    resp = requests.get(SEARCH_URL, params = params)
    soup = bs(resp.content, "lxml")

    searchRaw = soup.find("ul", class_="css-1xkub6-VisualUl-StyledHorizontalUl-StyledHorizontalUlWithContentPosterList")
    searchResult = [li.a['href'].split("/")[-1] for idx, li in enumerate(searchRaw.find_all("li"))]
    with open('movieCodes.txt', 'a', encoding = 'utf-8') as movieCodes:
        movieCodes.write(movieName + '|||' + searchResult[index - 1] + '\n')

random_url = "https://api-pedia.watcha.com/api/evaluations/movies"
url_best100 = 'https://api-pedia.watcha.com/api/staffmades/267'

payload = ""
headers = { 
    'accept': "application/vnd.frograms+json;version=20",
    'accept-encoding': "gzip, deflate, br",
    'accept-language': "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    'cache-control': "no-cache,no-cache",
    'cookie': "_ga=GA1.2.1784511024.1585672339; _gid=GA1.2.1170018568.1594139359; _c_drh=true; _gat=1; _s_guit=b940584f6c338f09faef81986606f6ede562b8554510679dda03f512b43a; Watcha-Web-Client-Language=ko; _guinness_session=sv7%2FKtsNPjrMOHQ%2FVwggfJ8J7OP0dXTjaEjVSh3GeykPZVSKW082ymy4WBzNthtDY4hSHrkCZUXptm4D0G8KOoWdllssE%2FbAXAQZGbBdgryPI6cBB7eiEfbhsn5o--d1je2zaGm3nVZhwt--zZ4a3yaxRHg9%2F7vfhjeFfQ%3D%3D",
    'dnt': "1",
    'origin': "https://pedia.watcha.com",
    'pragma': "no-cache",
    'referer': "https://pedia.watcha.com/ko-KR/review",
    'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    'x-watcha-client': "Watcha-WebApp",
    'x-watcha-client-language': "ko",
    'x-watcha-client-region': "KR",
    'x-watcha-client-version': "2.0.0"
}
if __name__ == '__main__':
    with open('movieCodes.txt', 'w', encoding='utf-8') as movieCodes:
        # 왓챠피디아 별점 TOP 100
        # https://pedia.watcha.com/ko-KR/staffmades/267
        print('best100')    
        movieCodes.write('best100\n')
        querystring = {
            'page' : 1,
            'size' : 9
        }
        response = requests.request(
            "GET", url_best100, data=payload, headers=headers, params=querystring)
        # print(response.content)
        next_uri = 'https://api-pedia.watcha.com' + \
            json.loads(response.content)['result']['contents']['next_uri']

        for movie in json.loads(response.content, encoding = 'utf-8')['result']['contents']['result']:
            data = movie['title'] + '|||' + movie['code'] + '\n'
            movieCodes.write(data)
        response.close()
        for i in range(4):
            response = requests.request(
                "GET", next_uri, data=payload, headers=headers)
            for movie in json.loads(response.content, encoding='utf-8')['result']['result']:
                movieCodes.write(movie['title'] + '|||' + movie['code'] + '\n')
            if not json.loads(response.content)['result']['next_uri']:
                break
            else:
                next_uri = 'https://api-pedia.watcha.com' + \
                    json.loads(response.content)['result']['next_uri']
            response.close()
        # 왓챠피디아 평가하기 탭에서 임의로 제시된 영화 100개
        # https://pedia.watcha.com/ko-KR/review
        print('random')
        movieCodes.write('random\n')
        querystring = {"size": "100"}
        response = requests.request(
            "GET", random_url, data=payload, headers=headers, params=querystring)
        next_uri = 'https://api-pedia.watcha.com' + \
            json.loads(response.content)['result']['next_uri']
        response.close()

        for movie in json.loads(response.content, encoding = 'utf-8')['result']['result']:
            data = movie['title'] + '|||' + movie['code'] + '\n'
            movieCodes.write(data)
        for i in range(4):
            response = requests.request(
                "GET", next_uri, data=payload, headers=headers, params=querystring)
            for movie in json.loads(response.content, encoding='utf-8')['result']['result']:
                movieCodes.write(movie['title'] + '|||' + movie['code'] + "\n")
            if not json.loads(response.content)['result']['next_uri']:
                break
            else:
                next_uri = 'https://api-pedia.watcha.com' + \
                    json.loads(response.content)['result']['next_uri']
            response.close()

        # personally selected 100 movies
        # picked movies stored in movie_names.txt
        print('selected')
        movieCodes.write('selected\n')
        movieCodes.close()

        movieNameSet = list()
        with open('movie_names_utf8.txt', encoding='utf-8') as movieNames:
            for l in movieNames.readlines():
                movieNameSet.append(l.rstrip('\n'))
        
        cpu_count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=cpu_count)
        pool.map(find_movieCode, movieNameSet)
        pool.close()
        pool.join()
        # for movie in movieNameSet:
            # find_movieCode(movie)
        response.close()
