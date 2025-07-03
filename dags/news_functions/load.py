from pykrx import stock
from airflow.sdk import task
import pendulum
import requests
from dotenv import load_dotenv
import os
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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

create_news_table = SQLExecuteQueryOperator( #news_table이 없을 경우 db에 news_table을 만드는 operator
    task_id='create_news_table', #task_id
    conn_id='krx_conn', #conn_id
    #실행하려는 sql 명령문
    sql=""" 
        CREATE TABLE IF NOT EXISTS news_table (
            "title" varchar(100) NOT NULL,
            "description" varchar(500) NOT NULL,
            "link" varchar(100) NOT NULL,
            "pub_time" varchar(25) NOT NULL
        );
    """,
)

@task
def load_news_data(data):
    pg_hook = PostgresHook(postgres_conn_id='krx_conn')  # postgresql 연결 훅
    conn = pg_hook.get_conn()  # 훅으로 postgresql 연결
    cur = conn.cursor()  # 커서 설정

    #반도체 관련 뉴스 db에 저장
    for title, link, description, pub_time in zip(data['titles'], data['links'], data['descriptions'], data['pub_times']):
        print(title, link, description, pub_time)
        cur.execute("""
                        INSERT INTO news_table ("title", "description", "link", "pub_time")
                        VALUES (%s, %s, %s, %s)
                    """, (title, description, link, pub_time)
        )
    # 변경상태 커밋
    conn.commit()
