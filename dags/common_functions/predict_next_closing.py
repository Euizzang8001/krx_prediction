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

    pg_hook = PostgresHook(postgres_conn_id='krx_conn')  # postgresql 연결 훅
    conn = pg_hook.get_conn()  # 훅으로 postgresql 연결
    cur = conn.cursor()  # 커서 설정

    # DB기준으로 저장된 가장 최근 3개의 날짜 가져오기
    cur.execute("""
            SELECT distinct(date)
            FROM stocks 
            ORDER BY date desc 
            LIMIT 3;
        """)
    # select 결과 가져오기
    dates = cur.fetchall()
    # 가장 최근 날짜부터 predicting_day, training_day, trained_day로 선언
    predicting_day, training_day, trained_day = dates[0][0], dates[1][0], dates[2][0]

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
                SELECT * FROM stocks
                WHERE name = %s AND date = %s;
                """,
                (stock_name, predicting_day)
            )
            row = cur.fetchone()  # 마지막 결과 하나만 가져오기

            # 예측 데이터 처리
            predict_x = pd.DataFrame([{
                "closing_changed_ratio": float(row[4]),
                "exchanging_change": float(row[5]),
                "semi_avg_closing_changed_ratio": float(row[6]),
                "news_score": float(row[7]),
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
                SELECT closing FROM stocks
                WHERE name = %s AND date = %s;
                """,
                (stock_name, predicting_day)
            )
            row = cur.fetchone()
            predicting_day_closing = float(row[0])

            #예측한 종가 변화율과 종가 계산하여 sql에 저장
            cur.execute(
                """
                    UPDATE stocks
                    SET predicted_closing = %s, predicted_closing_ratio = %s
                    WHERE name = %s AND date = %s;
                """, ((100 + predict_result) / 100  * predicting_day_closing, predict_result, stock_name, predicting_day)
            )

            print(f"{stock_name}의 {predicting_day} 이후 예측된 종가", (100 + predict_result) / 100  * predicting_day_closing)
            print(f"{stock_name}의 {predicting_day} 이후 예측된 종가 변화율", predict_result)

        #DB 변경사항 저장
        conn.commit()

    #에러 발생 시 print로 출력
    except Exception as e:
        print(e)






