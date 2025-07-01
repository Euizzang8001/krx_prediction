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
def preprocess_krx_data(data): #yesterday에 해당하는 다음날 종가 변화율, today data
    per_stock_dict = {} # XGBoost 모델 훈련을 위하여 가장 최근 data(전날)만 저장하는 dict선언

    avg_closing_change_ratio = 0


    for stock_name in stocks.keys(): #종목 별 조회
        df = data["per_stock_data"][stock_name] #종목 별 전전날, 전날, 오늘 데이터

        today_df = df.loc[data["today"].strftime("%Y%m%d")].to_dict() #오늘 데이터만 추출
        yesterday_df = df.loc[data["yesterday"].strftime("%Y%m%d")].to_dict() #전날 데이터만 추출

        today_df["종가 변화율"] = ((today_df["종가"] - yesterday_df["종가"]) / yesterday_df["종가"]) * 100
        today_df["거래량 변화량"] = today_df["거래량"] - yesterday_df["거래량"]

        avg_closing_change_ratio += today_df["종가 변화율"]

        per_stock_dict[stock_name] = today_df #오늘 날자의 data만 dict에 저장

    avg_closing_change_ratio /= len(stocks)

    return {"per_stock_dict": per_stock_dict, "yesterday": data["yesterday"], "today": data["today"], "avg_closing_change_ratio": avg_closing_change_ratio} #dict 반환
