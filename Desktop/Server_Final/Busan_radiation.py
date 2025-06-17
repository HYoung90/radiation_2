# busan_radiation.py
# 이 스크립트는 부산의 환경 방사선 데이터를 공공 API에서 가져와 MongoDB에 저장합니다.
# 데이터 백업 및 오류 발생 시 텔레그램 알림 기능이 포함되어 있으며, 60분마다 실행됩니다.

import requests
from pymongo import MongoClient
import logging
import schedule
import time
import atexit
import sys
import os
from dotenv import load_dotenv
from telegram_notifier import send_telegram_message

# 환경 변수 로드
load_dotenv("telegram_config.env")

# 로그 설정
logging.basicConfig(
    filename="busan_radiation_data_fetch.log",
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# 텔레그램 설정
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BUSAN_RADIATION_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 공공 API URL과 서비스 키
base_url = "http://apis.data.go.kr/6260000/EnvironmentalRadiationInfoService"
service_key = "h%2BQvAtTFBPlY19lErWf4T9JQoowL2d918ciMd6%2B%2FdBFGTV55ykPjAp8V1UWAZPRJHKWawuQOncBubNafaOVwTQ%3D%3D"

# MongoDB 연결 설정
client = MongoClient("mongodb://localhost:27017/")
db = client['Data']
radiation_collection = db['Busan_radiation']
radiation_backup_collection = db['Busan_radiation_backup']

def log_program_exit():
    """프로그램 종료 시 로그를 남김"""
    logging.info("프로그램이 종료되었습니다.")
    print("프로그램이 종료되었습니다.")

atexit.register(log_program_exit)

def backup_existing_data():
    """기존 데이터를 백업 컬렉션으로 이동"""
    try:
        existing_data = list(radiation_collection.find({}))
        if existing_data:
            radiation_backup_collection.insert_many(existing_data)
            radiation_collection.delete_many({})
            logging.info("기존 데이터를 busan_radiation_backup으로 이동 완료.")
        else:
            logging.info("백업할 데이터가 없습니다.")
    except Exception as e:
        logging.error(f"데이터 백업 중 오류 발생: {e}")
        print(f"데이터 백업 중 오류 발생: {e}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"데이터 백업 중 오류 발생:\n{str(e)}")

def fetch_radiation_data():
    """부산 환경방사선 선량률 정보 조회"""
    try:
        url = f"{base_url}/getEnvironmentalRadiationInfoDetail?serviceKey={service_key}&resultType=json&numOfRows=100"
        response = requests.get(url)

        if response.status_code == 200:
            print("방사선 선량률 정보 조회 성공.")
            logging.info("방사선 선량률 정보 조회 성공.")

            data = response.json()
            items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])

            backup_existing_data()

            for item in items:
                try:
                    dose_raw = float(item.get("data", 0))
                    dose_rounded = round(dose_raw, 2)
                except (TypeError, ValueError):
                    dose_rounded = None

                radiation_data = {
                    "checkTime": item.get("checkTime"),
                    "locNm": item.get("locNm"),
                    "data": dose_rounded,
                    "aveRainData": item.get("aveRainData", "0.0"),
                    "lat": item.get("lat", None),
                    "lng": item.get("lng", None),
                }

                print(f"지역명: {radiation_data['locNm']}, 방사선량: {dose_rounded:.2f} μSv/h")
                logging.info(f"지역명: {radiation_data['locNm']}, 방사선량: {dose_rounded:.2f} μSv/h")

                radiation_collection.update_one(
                    {"checkTime": radiation_data["checkTime"], "locNm": radiation_data["locNm"]},
                    {"$set": radiation_data},
                    upsert=True
                )
        else:
            error_msg = f"방사선 선량률 정보 조회 실패. 응답 코드: {response.status_code}"
            logging.error(error_msg)
            print(error_msg)
            send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, error_msg)

    except Exception as e:
        error_msg = f"방사선 선량률 정보 조회 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        print(f"오류 발생: {str(e)}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, error_msg)
        sys.exit(1)

def scheduled_task():
    logging.info("60분마다 데이터 수집 작업 실행 중...")
    print("60분마다 데이터 수집 작업 실행 중...")
    fetch_radiation_data()

schedule.every(60).minutes.do(scheduled_task)

if __name__ == "__main__":
    print("60분마다 방사선 데이터를 확인하는 스케줄을 시작합니다.")
    logging.info("60분마다 방사선 데이터를 확인하는 스케줄을 시작합니다.")

    fetch_radiation_data()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logging.info("프로그램이 수동으로 종료되었습니다.")
        print("프로그램이 수동으로 종료되었습니다.")
        sys.exit(0)
