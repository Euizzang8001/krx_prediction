from pykrx import stock
from airflow.sdk import task
import pendulum
import requests
from dotenv import load_dotenv
import os
from datetime import datetime

stocks = { #반도체 관련 주식 종목 dict
    "삼성전자": ["005930", "20130405"],
    "SK하이닉스": ["000660", "20130405"],
    "한미반도체": ["042700", "20130405"],
    "리노공업": ["058470", "20130405"],
    "젬백스": ["082270", "20130405"],
    "HPSP": ["403870", "20220715"],
    "DB하이텍": ["000990", "20130405"],
    "이오테크닉스": ["039030", "20130405"],
    "주성엔지니어링": ["036930", "20130405"],
    "원익IPS": ["240810", "20160502"]
}

@task
def get_news_data():
    load_dotenv()
    try:
    # 환경변수 가져오기
        naver_client_id = os.getenv('NAVER_CLIENT_ID')
        naver_client_secret = os.getenv('NAVER_CLIENT_SECRET')
        naver_news_url = os.getenv('NAVER_NEWS_URL')

        # 반도체 관련 뉴스 가져오기
        params = {"query": "반도체", "display": 100, "sort": "date"}
        headers = {"X-Naver-Client-Id": naver_client_id, "X-Naver-Client-Secret": naver_client_secret}
        news = requests.get(naver_news_url, headers=headers, params=params).json()

        #지금 시간 가져오기
        now = pendulum.now('Asia/Seoul')
        check_time = now.subtract(hours=1)
        check_hour = check_time.hour

        #뉴스의 데이터를 각각 list로 저장
        titles = []
        links = []
        descriptions = []
        pub_times = []

        for each_news in news['items']:
            #기사 출간 시간이 1시간 전 ~  현재 시간 사이에 있다면, 추출
            pub_time = datetime.strptime(each_news['pubDate'], '%a, %d %b %Y %H:%M:%S %z')
            if pub_time.hour == check_hour:
                titles.append(each_news['title'])
                links.append(each_news['link'])
                descriptions.append(each_news['description'])
                pub_times.append(pub_time.strftime("%Y-%m-%d %H:%M:%S"))
            #만약 그 이전의 기사라면, 이후 뉴스들도 모두 이미 뉴스 점수로 참고한 데이터일 것이므로 break
            elif pub_time.hour == 23 and check_hour == 0:
                break
            elif pub_time.hour < check_hour:
                break
            else:
                continue
        print("News Data Extracted")
        return {"titles": titles, "links": links, "descriptions": descriptions, "pub_times": pub_times, "check_time":check_time.strftime("%Y-%m-%d %H")}

    except Exception as e:
        print(e)