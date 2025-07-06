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
def get_krx_data(): #pykrx 라이브러리로 오늘의 반도체 종목 종가를 얻는 함수
    today = pendulum.now("Asia/Seoul") #오늘 날짜 선언
    yesterday = None #주식 시장 기준 전날 날짜의 pendulum 값

    #today가 주말이라면, 실행 안함
    if today.weekday() >= 5:
        return {"today": today} #dict 반환


    i = 1  # 주식 시장 기준으로 전날과 전전날을 찾기 위한 indicator
    while not yesterday: #전날과 전전날을 찾을 때까지 반복문 실행
        subtract_date = today.subtract(days = i) #오늘 날짜부터 i일 전의 날짜
        if subtract_date.weekday() < 5: #i일 전의 날이 주중이면 실행
            if not yesterday: #전날 날짜 값이 비어 있으면
                yesterday = subtract_date
                break
        i += 1 #전날, 전전날을 아직 못 찾았으므로 i + 1

    #전전날까지의 종목 별 주가 데이터 저장
    per_stock_data = {}
    avg_change_ratio = 0
    for stock_name, stock_info in stocks.items(): #반도체 종목들의 오늘자 종가 얻기
        #전전날부터 오늘까지의 주식 값 얻기
        today_stock_df = stock.get_market_ohlcv(yesterday.strftime("%Y%m%d") , today.strftime("%Y%m%d") , stock_info[0])
        #전일 종가 column 값 추가
        today_stock_df["전일 종가"] = today_stock_df["종가"].shift(1)
        #전일 거래량 column 값 추가
        today_stock_df["전일 거래량"] = today_stock_df["거래량"].shift(1)
        #거래량 변화량 계산하여 column값 추가
        today_stock_df["거래량 변화량"] = today_stock_df["전일 거래량"] - today_stock_df["거래량"]
        #종가 변화량 계산하여 column값 추가
        today_stock_df["종가 변화율"] = ((today_stock_df["종가"] - today_stock_df["전일 종가"]) / today_stock_df["전일 종가"]) * 100

        per_stock_data[stock_name] = today_stock_df #수집 완료한 df를 dict에 추가

    return {"per_stock_data": per_stock_data, "yesterday": yesterday, "today": today}


