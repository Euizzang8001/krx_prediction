from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task

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
def insert_krx_table(data): #새로 생성된 데이터를 추가하는 task
    #오늘이 주말이라면 실행 안함
    if data["today"].weekday() >= 5:
        return None
    pg_hook = PostgresHook(postgres_conn_id='krx_conn') #postgresql 연결 훅
    conn = pg_hook.get_conn() #훅으로 postgresql 연결
    cur = conn.cursor() #커서 설정

    #종목 별로 순회하면서 db에 new data(yesterday) insert
    for stock_name, stock_info in data["per_stock_dict"].items():
        cur.execute("""
                INSERT INTO krx_table ("종목", "날짜", "종가", "종가 변화율", "거래량 변화량", "평균 종가 변화율", "뉴스 점수", "다음날 종가 변화율", "뉴스 수")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (stock_name, data["today"].strftime("%Y%m%d"), stock_info["종가"] ,stock_info["종가 변화율"], stock_info["거래량 변화량"], data["avg_closing_change_ratio"], 0, None, 0)
        )
        cur.execute("""
                UPDATE krx_table 
                SET "다음날 종가 변화율" = %s 
                WHERE "날짜" = %s and "종목" = %s
            """, (stock_info["종가 변화율"],data["yesterday"].strftime("%Y%m%d"), stock_name)
        )
    #변경상태 커밋
    conn.commit()



