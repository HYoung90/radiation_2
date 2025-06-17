from pymongo import MongoClient
import logging
from statistics import mean
import requests  # requests 모듈 추가
import schedule  # 스케줄링 모듈 추가
import time  # 시간 모듈 추가
import os
from dotenv import load_dotenv  # 환경 변수 로드 모듈

# 환경 변수 로드
load_dotenv("telegram_config.env")  # .env 파일에서 로드

# MongoDB 연결
client = MongoClient("mongodb://localhost:27017/")
db = client['Data']
radiation_collection = db['Busan_radiation']

# 텔레그램 설정 - Busan Radiation 봇 사용
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BUSAN_RADIATION_TOKEN")  # .env 파일에서 Busan Radiation 봇의 토큰을 가져옴
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # .env 파일에서 채팅 ID를 가져옴

# 텔레그램 메시지 전송 함수
def send_alert_to_another_bot(message):
    # 'another_bot_token'을 'TELEGRAM_TOKEN'으로 변경
    chat_id = TELEGRAM_CHAT_ID  # 채팅 ID (사용자 또는 그룹)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"  # 수정된 부분

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"  # 메시지 포맷팅을 위해 Markdown 사용
    }

    response = requests.post(url, json=payload)

    # 응답을 출력하여 확인
    # print(response.json())  # 응답 확인

    return response.json()

# 방사선 통계 조회 함수
def fetch_radiation_statistics():
    try:
        # 방사선량 데이터를 가져옴
        data = list(radiation_collection.find({}, {"locNm": 1, "data": 1}))

        # 가장 높은 방사선량을 기록한 지역 찾기
        highest_value = -1
        highest_region = None
        total_radiation = []

        for item in data:
            try:
                radiation_value = float(item['data'])
            except ValueError:
                logging.warning(f"Invalid data for region {item['locNm']}: {item['data']}")
                continue

            if radiation_value > 0:
                total_radiation.append(radiation_value)

                if radiation_value > highest_value:
                    highest_value = radiation_value
                    highest_region = item['locNm']

        if total_radiation:
            average_radiation = mean(total_radiation)
        else:
            average_radiation = 0

        # 결과 메시지 포맷팅 (줄 바꿈 추가 및 강조)
        result_message = (
            f"*부산에서 가장 높은 방사선량 지역:* \n"  # 줄 바꿈 추가
            f"{highest_region} ({highest_value:.2f} nSv/h)\n"  # 줄 바꿈 추가
            f"*부산 전체 평균 방사선량:* {average_radiation:.2f} nSv/h"
        )
        print(result_message)

        # 텔레그램 알림 전송
        send_alert_to_another_bot(result_message)

    except Exception as e:
        logging.error(f"Error fetching radiation statistics: {e}")
        print(f"Error fetching radiation statistics: {e}")

# 매일 2시간마다 (정각에) 작업을 실행하는 스케줄 설정
schedule.every(4).hours.at(":30").do(fetch_radiation_statistics)

if __name__ == "__main__":
    logging.info("Radiation processing started.")
    fetch_radiation_statistics()  # 처음 실행 시 한번 실행

    # 스케줄을 지속적으로 실행
    while True:
        schedule.run_pending()
        time.sleep(1)
