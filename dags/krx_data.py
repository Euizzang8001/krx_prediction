from airflow.sdk import dag, task
import pendulum
from datetime import datetime, timedelta

@dag( #dag 정의
    dag_id="krx prediction", #dag id 설정
    schedule = timedelta(hours=24), #반복 주기 설정
    #시작 날짜 설정
    start_date=pendulum.datetime(
        2021,
        12,
        31,
        16,
        30,
        0,
        0,
        "KST"),
    #catchup 설정 : catchup - 과거부터 시작할 때, 과거 빈 공간을 채워주는 기능
    catchup=False,
    tags=['krx_prediction'] #tag 설정
)
def krx_prediction(): #dag 실행 함수 정의

