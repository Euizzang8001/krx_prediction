from pykrx import stock
from airflow.sdk import task
import pendulum
import requests
from dotenv import load_dotenv
import os
from datetime import datetime
from airflow.providers.mongo.hooks.mongo import MongoHook

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
def load_news_data(data):
    try:
        # mongo_hook 연결
        hook = MongoHook(mongo_conn_id='mongo_conn_id')
        client = hook.get_conn()
        db = client["news_db"] #news_db에 접근
        db.create_collection(data['check_time']) #뉴스 출간 시간으로 collection 생성
        col = db[data['check_time']] #출간 시간 collection에 접근

        #뉴스 데이터를 순회하면서 title, description, link, pub_time을 dict형식으로 collection에 저장
        for title, description, link, pub_time in zip(data['titles'], data['descriptions'], data['links'], data['pub_times']):
            col.insert_one({
                'title': title,
                'description': description,
                'link': link,
                'pub_time': pub_time,
            })
        print("News Data Loaded")
    except Exception as e:
        print(e)
    return data
