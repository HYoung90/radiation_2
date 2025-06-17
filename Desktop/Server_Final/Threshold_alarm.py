import requests
from pymongo import MongoClient
import logging
import schedule
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import re  # 정규 표현식 모듈 추가

# 환경 변수 로드
load_dotenv("telegram_config.env")  # .env 파일에서 로드

# 로그 설정
logging.basicConfig(
    filename="threshold_alarm.log",
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# 텔레그램 설정
TELEGRAM_TOKEN = os.getenv("TELEGRAM_NPP_MONITORING_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# MongoDB 연결 설정
client = MongoClient("mongodb://localhost:27017/")
db = client['Data']
radiation_collection = db['nuclear_radiation']
avg_db = client['radiation_statistics']  # 평균을 저장할 새로운 데이터베이스
avg_collection = avg_db['regional_average']  # 평균 데이터 저장 컬렉션

def send_telegram_message(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    response = requests.post(url, data=data)

    if response.status_code == 200:
        print("텔레그램 메시지 전송 성공!")
        logging.info("텔레그램 메시지 전송 성공!")
    else:
        print(f"텔레그램 메시지 전송 실패. 상태 코드: {response.status_code}, 응답: {response.text}")
        logging.error(f"텔레그램 메시지 전송 실패. 상태 코드: {response.status_code}, 응답: {response.text}")

def get_highest_radiation_region(plant_code):
    """주어진 발전소 코드에 대한 방사선이 가장 높은 지역을 찾는 함수"""
    highest_radiation = None
    highest_region = None

    # 해당 발전소 주변 지역의 방사선 데이터 조회
    radiation_data = radiation_collection.find({"genName": plant_code})

    for data in radiation_data:
        expl = data.get("expl")  # 방사선량이 가장 높은 지역 정보
        radiation_value = float(data.get("value", 0))

        if expl:
            # 'ERMS-'를 제거하고 '(MS-00)'을 제거하여 지역 이름만 추출
            region = expl.replace("ERMS-", "").strip()  # 'ERMS-' 제거
            region = re.sub(r"\s*\(MS-\d+\)", "", region)  # '(MS-00)' 형식 제거

        if highest_radiation is None or radiation_value > highest_radiation:
            highest_radiation = radiation_value
            highest_region = region

    return highest_region, highest_radiation

def check_sigma_alert():
    try:
        plant_names = {
            "WS": "월성발전소 (경북 경주)",
            "KR": "고리발전소 (부산 기장)",
            "YK": "한빛발전소 (전남 영광)",
            "UJ": "한울발전소 (경북 울진)",
            "SU": "새울발전소 (울산 울주)"
        }

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"*{current_time} 기준 방사선량 보고*\n\n"

        for plant_code, plant_name in plant_names.items():
            latest_radiation = radiation_collection.find_one({"genName": plant_code}, sort=[("time", -1)])

            if not latest_radiation:
                logging.warning(f"{plant_name}의 최신 방사선 데이터를 찾을 수 없습니다.")
                print(f"{plant_name}의 최신 방사선 데이터를 찾을 수 없습니다.")
                continue

            radiation_value = float(latest_radiation.get("value", 0))
            logging.info(f"{plant_name} 최신 방사선량: {radiation_value} μSv/h")
            print(f"{plant_name} 최신 방사선량: {radiation_value} μSv/h")

            avg_entry = avg_collection.find_one({"genName": plant_code}, sort=[("date", -1)])

            if avg_entry:
                no_rain_avg = round(avg_entry.get("no_rain_avg", 0), 3)
                threshold = round(no_rain_avg + 0.097, 3)

                latest_rain_data = db['plant_measurements'].find_one({"region": plant_code}, sort=[("time", -1)])
                rain_status = "강우 없음"
                if latest_rain_data:
                    rain_value = latest_rain_data.get("rainfall", 0)
                    if rain_value > 0:
                        rain_status = f"강우 발생 (일일 누적 강우량: {rain_value} mm)"

                message += f"발전소: *{plant_name}*\n"
                message += f"측정값: *{radiation_value} μSv/h*\n"
                message += f"평균값: *{no_rain_avg} μSv/h*\n"
                message += f"기준값: *{threshold} μSv/h*\n"
                message += f"강우 여부: {rain_status}\n"

                # 각 발전소 주변에서 가장 높은 방사선 값을 가진 지역 찾기
                highest_region, highest_radiation = get_highest_radiation_region(plant_code)

                if highest_region:
                    message += f"가장 높은 방사선량 지역: *{highest_region}*\n"
                    message += f"방사선량: *{highest_radiation} μSv/h*\n"  # 줄 바꿈 추가
                else:
                    message += "가장 높은 방사선량 지역 정보를 찾을 수 없습니다.\n"

                if radiation_value > threshold:
                    message += "*경고*: 방사선량이 기준값을 초과했습니다!\n\n"
                    logging.info(f"{plant_name} 기준값 초과 알림 전송 완료.")
                    print(f"{plant_name}: 기준값 초과 알림 텔레그램 전송 완료.")
                else:
                    message += "현재 방사선량이 정상 범위입니다.\n\n"
                    logging.info(f"{plant_name} 방사선량 정상: {radiation_value} μSv/h")
                    print(f"{plant_name}: 방사선량 정상 상태입니다.")
            else:
                logging.warning(f"{plant_name}의 평균값을 찾을 수 없습니다.")
                print(f"{plant_name}의 평균값을 찾을 수 없습니다.")
                message += f"{plant_name}의 평균값을 찾을 수 없습니다.\n\n"

        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, message)
        print("텔레그램 메시지가 성공적으로 전송되었습니다.")

    except Exception as e:
        logging.error(f"방사선 데이터 처리 중 오류 발생: {e}")
        error_message = f"방사선 데이터 처리 중 오류 발생:\n{str(e)}"
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, error_message)
        print("오류 발생으로 인해 텔레그램 경고 메시지를 전송했습니다.")

# 매 2시간마다 (정각에) 작업을 실행하는 스케줄 설정
schedule.every(4).hours.at(":30").do(check_sigma_alert)

if __name__ == "__main__":
    logging.info("스크립트 실행 후 매 2시간마다 방사선 데이터를 확인하는 스케줄을 시작합니다.")
    print("스크립트 실행 후 매 2시간마다 방사선 데이터를 확인하는 스케줄을 시작합니다.")

    check_sigma_alert()

    while True:
        schedule.run_pending()
        time.sleep(60)
