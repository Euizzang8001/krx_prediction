import os

from airflow.sdk import task
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3
import joblib
import io
import pandas as pd
from xgboost import XGBRegressor
from dotenv import load_dotenv

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
def predict_by_xgboost_model():
    #env파일 불러오기
    load_dotenv()

    # 환경변수 가져오기
    aws_region = os.getenv('AWS_REGION')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    today = pendulum.now("Asia/Seoul")  # 오늘 날짜 선언
    #today = pendulum.datetime(2025, 6, 30) #test용
    i = 1  # 주식 시장 기준으로 전날을 찾기 위한 indicator
    yesterday = None  # 주식 시장 기준 전날 날짜의 pendulum 값
    day_before_yesterday = None #주식 시장 기준 전전날 날짜
    day_before_before_yesterday = None #주식 시장 기준 전전전날 날짜
    while not yesterday or not day_before_yesterday:  # 전날을 찾을 때까지 반복문 실행
        subtract_date = today.subtract(days=i)  # 오늘 날짜부터 i일 전의 날짜
        if subtract_date.weekday() < 5:  # i일 전의 날이 주중이면 실행
            if not yesterday:
                yesterday = subtract_date #전날 값에 할당
                i += 1
                continue
            elif not day_before_yesterday: #전전날 값이 없으면 전전날 값으로 설정
                day_before_yesterday = subtract_date  # 전전날 값에 할당
                i += 1
                continue
            elif not day_before_before_yesterday: #전전전날 값이 없으면 전전날 값으로 설정
                day_before_before_yesterday = subtract_date  # 전전전날 값에 할당
                i += 1
                continue
        i += 1  # 전날과 전전날을 아직 못 찾았으므로 i + 1

    #trained_day: 이미 훈련이 끝마친 모델 날짜 / training_day: 뉴스 점수를 계속 업데이트하여 계속 훈련할 날짜
    #predicting_day: 예측에 사용할 날짜(다음날 주가 변화율이 없는 최신 데이터)
    trained_day, training_day, predicting_day = None, None, None

    if today.hour <16: #주식 시장 종료 이전이면 -> 예측할 종가 날짜: today, 예측에 사용될 날짜: yesterday,
        # 뉴스 점수 없데이트할 날짜: day_before_yesterday, 구현된 최종 모델: day_before_before_yesterday
        trained_day = day_before_before_yesterday
        training_day = day_before_yesterday
        predicting_day = yesterday
    else: #주식 시장 종료 이후면 -> 예측할 종가 날짜: tomorrow, 예측에 사용될 날짜: today,
        # 뉴스 점수 없데이트할 날짜: yesterday, 구현된 최종 모델: day_before_yesterday
        trained_day = day_before_yesterday
        training_day = yesterday
        predicting_day = today

    pg_hook = PostgresHook(postgres_conn_id='krx_conn')  # postgresql 연결 훅
    conn = pg_hook.get_conn()  # 훅으로 postgresql 연결
    cur = conn.cursor()  # 커서 설정

    #s3 session 연결
    s3_client = boto3.client(
        's3',
        region_name=aws_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    #s3 bucket 설정
    try:
        bucket_name = "krx-prediction"
        for stock_name in stocks.keys(): #종목 별 predicting값을 가져와 xgboost 모델로 예측하기
            cur.execute(
                """
                SELECT * FROM krx_table
                WHERE 종목 = %s AND 날짜 = %s;
                """,
                (stock_name, predicting_day.strftime('%Y%m%d'))
            )
            row = cur.fetchone()  # 마지막 결과 하나만 가져오기

            # 예측 데이터 처리
            predict_x = pd.DataFrame([{
                "종가 변화율": float(row[3]),
                "거래량 변화량": float(row[4]),
                "평균 종가 변화율": float(row[5]),
                "뉴스 점수": float(row[6]),
            }])

            # s3로부터 past_model(수정x되는 모델) 가져오기
            response = s3_client.get_object(Bucket=bucket_name, Key=f"models/{stock_name}_past_model.json")
            body = response["Body"].read()

            model = XGBRegressor() #초기 모델 설정
            model.load_model(bytearray(body)) #초기 모델에 저장되어 있던 모델 가져오기

            #예측 데이터로 다음날 종가 변화율 예측
            predict_result = float(model.predict(predict_x)[0])


            #전날 종가 가격 찾기
            cur.execute(
                """
                SELECT 종가 FROM krx_table
                WHERE 종목 = %s AND 날짜 = %s;
                """,
                (stock_name, predicting_day.strftime('%Y%m%d'))
            )
            row = cur.fetchone()
            predicting_day_closing = float(row[0])

            #예측한 종가 변화율과 종가 계산하여 sql에 저장
            cur.execute(
                """
                    UPDATE krx_table
                    SET "예측된 종가" = %s, "예측된 종가 변화율" = %s
                    WHERE 종목 = %s AND 날짜 = %s;
                """, ((100 + predict_result) / 100  * predicting_day_closing, predict_result, stock_name, predicting_day.strftime('%Y%m%d'))
            )

            print(f"{stock_name}의 {predicting_day.strftime('%Y%m%d')} 이후 예측된 종가", (1 + predict_result) * float(row[0]))
            print(f"{stock_name}의 {predicting_day.strftime('%Y%m%d')} 이후 예측된 종가 변화율", predict_result)

        #DB 변경사항 저장
        conn.commit()

    #에러 발생 시 print로 출력
    except Exception as e:
        print(e)






