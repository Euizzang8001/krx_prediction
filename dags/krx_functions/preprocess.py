from pykrx import stock
from airflow.sdk import task
import pendulum

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
def preprocess_krx_data(per_stock_data, avg_change_ratio, yesterday):
    latest_per_stock_dict = {} # XGBoost 모델 훈련을 위하여 가장 최근 data(전날)만 저장하는 dict선언

    for stock_name in stocks.keys(): #종목 별 조회
        df = per_stock_data[stock_name] #종목 별 전전날, 전날, 오늘 데이터
        df["다음날 종가 변화율"] = df["종가 변화율"].shift(-1) #다음 날 종가 변화율을 column으로 저장

        latest_df = df.loc[yesterday.date()] #전날 데이터만 추출
        latest_df['평균 종가 변화율'] = avg_change_ratio #전날 데이터에 평균 종가 변화율 추가
        latest_per_stock_dict[stock_name] = latest_df #가장 최근 data만 dict에 저장

    return latest_per_stock_dict #dict 반환
