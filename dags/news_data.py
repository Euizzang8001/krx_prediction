from airflow.sdk import dag
import pendulum
from datetime import timedelta

from common_functions.train_xgboost_model import train_xgboost_model
from common_functions.predict_next_closing import predict_by_xgboost_model

from news_functions.extract import get_news_data
from news_functions.load import load_news_data
from news_functions.get_news_score import get_news_score
@dag( #dag 정의
    dag_id="krx_prediction_by_news", #dag id 설정
    schedule = timedelta(hours=1), #반복 주기 설정
    #시작 날짜 설정
    start_date=pendulum.datetime(
        2025,
        8,
        9,
        1,
        0,
        0,
        0,
        "Asia/Seoul"),
    #catchup 설정 : catchup - 과거부터 시작할 때, 과거 빈 공간을 채워주는 기능
    catchup=False,
    tags=['krx_prediction']#tag 설정
)
def krx_prediction_by_news(): #dag 실행 함수 정의
    get_news_data_dag = get_news_data()
    load_news_data_dag = load_news_data(get_news_data_dag)
    get_news_score_dag = get_news_score(load_news_data_dag)

    train_xgboost_model_dag = train_xgboost_model()
    predict_next_closing_dag = predict_by_xgboost_model()

    get_news_data_dag >> load_news_data_dag >> get_news_score_dag >> train_xgboost_model_dag >> predict_next_closing_dag

dag = krx_prediction_by_news()
