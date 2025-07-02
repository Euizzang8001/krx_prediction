from airflow.sdk import dag, task
import pendulum
from datetime import datetime, timedelta
from krx_functions.extract import get_krx_data
from krx_functions.preprocess import preprocess_krx_data
from krx_functions.load import create_krx_table, insert_krx_table

from common_functions.train_xgboost_model import train_xgboost_model
from common_functions.predict_next_closing import predict_by_xgboost_model

@dag( #dag 정의
    dag_id="krx_prediction", #dag id 설정
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
        "Asia/Seoul"),
    #catchup 설정 : catchup - 과거부터 시작할 때, 과거 빈 공간을 채워주는 기능
    catchup=False,
    tags=['krx_prediction']#tag 설정
)
def krx_prediction(): #dag 실행 함수 정의
    get_krx_dag = get_krx_data()
    preprocess_krx_dag = preprocess_krx_data(get_krx_dag)
    get_krx_dag >> preprocess_krx_dag >> create_krx_table >> insert_krx_table(preprocess_krx_dag) >> train_xgboost_model() >> predict_by_xgboost_model()

dag = krx_prediction()
