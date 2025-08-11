from airflow.sdk import dag, task
import pendulum
from datetime import timedelta
from krx_functions.extract import get_krx_data
from krx_functions.preprocess import preprocess_krx_data
from krx_functions.load import insert_krx_table

from common_functions.train_xgboost_model import train_xgboost_model
from common_functions.predict_next_closing import predict_by_xgboost_model


@dag( #dag 정의
    dag_id="krx_prediction", #dag id 설정
    schedule = timedelta(hours=24), #반복 주기 설정
    #시작 날짜 설정
    start_date=pendulum.datetime(
        2025,
        8,
        9,
        15,
        40,
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
    insert_krx_table_dag = insert_krx_table(preprocess_krx_dag)

    train_xgboost_model_dag = train_xgboost_model()
    predict_next_closing_dag = predict_by_xgboost_model()

    get_krx_dag >> preprocess_krx_dag >> insert_krx_table_dag >> train_xgboost_model_dag >> predict_next_closing_dag

dag = krx_prediction()
