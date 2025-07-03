from pykrx import stock
from airflow.sdk import task
import pendulum
import requests
from dotenv import load_dotenv
import os
from datetime import datetime
from openai import OpenAI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import ast

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
def get_news_score(data):
    load_dotenv()

    pg_hook = PostgresHook(postgres_conn_id='krx_conn')  # postgresql 연결 훅
    conn = pg_hook.get_conn()  # 훅으로 postgresql 연결
    cur = conn.cursor()  # 커서 설정

    openai_api_key = os.getenv("OPENAI_API_KEY") #openai api key 가져오기

    client = OpenAI(
        api_key=openai_api_key,
    ) #OpenAI library로 client 생성

    #뉴스 점수 총합 list 생성
    score_sum_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    #기사를 순회하면서 해당 기사의 뉴스 점수를 가져와 score_sum_list에 누적합 계산하기
    for title, description in zip(data['titles'], data['descriptions']):
        #뉴스 계산 프롬프트 설정
        prompt = f"""
        당신은 금융 뉴스 분석가입니다.
        input을 보고 이 뉴스가 다음 기업에 긍정적인 뉴스인지 아니인지 판단해주세요.
        반환 형식은 list로 해주세요
        기업 list = ['삼성전자', 'SK하이닉스', '한미반도체', '리노공업', '젬백스', 'HPSP', 'DB하이텍', '이오테크닉스', '주성엔지니어링', '원익IPS']
        - 반도체 사업에 전체적으로 긍정적 영향을 미치는 뉴스, 해당 기업에 긍정적인 뉴스라면 1점
        - 반도체 사업에 전체적으로 부정적인 뉴스, 다른 기업에 긍정적인 뉴스라면 -1점
        - 무관하거나 중립적인 뉴스라면 0점을 반환하세요.
        
        뉴스 점수는 오직 숫자로만 출력하세요 (예: -1, 0, 1).
    
        반환 예시: [1, 0, -1, 1, 1, 0, -1, -1, 1, 0]
        
        전체적 예시1:
        input: 미국 트럼프 대통령, 반도체 기업에 과세율 15% 인상
        output: [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1]
        
        전체적 예시2:
        input: 삼성전자, 새로운 반도체 기술 발표
        output: [1, -1, -1, -1, -1, -1, -1, -1, -1, -1]
            """
        #해당 프롬프트로 뉴스 기사에 대한 점수 list 얻기
        response = client.responses.create(
            instructions = prompt,
            model="gpt-4o",
            input=title + description
        )

        #response를 text화하기
        content = response.output[0].content[0].text
        #text화 한 response를 list로 자료형 변환
        score_list = ast.literal_eval(content)
        #score_sum_list에 누적 뉴스 점수 합 계산
        score_sum_list = [a + b for a, b in zip(score_sum_list, score_list)]

    today = pendulum.now("Asia/Seoul") #오늘의 시간 저장
    yesterday = None  # 주식 시장 기준 전날 날짜의 pendulum 값
    i = 1 # 주식 시장 기준 전날 날짜를 구하기 위한 indicator설정
    while not yesterday:  # 전날을 찾을 때까지 반복문 실행
        subtract_date = today.subtract(days=i)  # 오늘 날짜부터 i일 전의 날짜
        if subtract_date.weekday() < 5:  # i일 전의 날이 주중이면 실행
            if not yesterday:
                yesterday = subtract_date  # 전날 값에 할당
                break
        i += 1  # 전날을 아직 못 찾았으므로 i + 1

    predicting_day = today if today.hour >= 16 else yesterday #16시가 넘으면 -> 오늘 종가로 예측, 안 넘으면 -> 어제 종가로 오늘의 종가를 예측
    news_count = len(data['titles']) #이번 구간동안의 전체 뉴스의 수

    #각 종목 별로 누적합 된 뉴스 점수 및 뉴스의 수 업데이트
    for stock_name, news_score in zip(stocks.keys(), score_sum_list):
        cur.execute("""
                UPDATE krx_table
                SET "뉴스 점수" = (("뉴스 점수" * "뉴스 수") + %s)/("뉴스 수" + %s) , "뉴스 수" = "뉴스 수" + %s
                WHERE "날짜" = %s and "종목" = %s
            """, (news_score, news_count, news_count, predicting_day.strftime("%Y%m%d"), stock_name)
        )
    #DB 변경 사항 커밋
    conn.commit()
    print("News Score Updated")