import logging
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import sys  # sys 모듈 추가
from datetime import datetime  # 현재 시간 출력을 위한 모듈 추가
import schedule  # 스케줄러 모듈
import time  # 스케줄 실행 대기 시간 조절을 위한 모듈

# .env 파일에서 환경 변수 불러오기
load_dotenv()

# 로깅 설정 (stdout으로 강제하여 모든 출력이 동일하게 처리되도록 설정)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler('radiation_data.log'),
        logging.StreamHandler(sys.stdout)  # sys.stdout을 사용해 stdout으로 로그를 출력
    ]
)


# MongoDB 연결
def get_mongo_connection():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        logging.info("MongoDB 연결 성공")
        print("MongoDB 연결 성공")  # 콘솔에 출력
        return client
    except Exception as e:
        logging.error(f"MongoDB 연결 실패: {e}")
        print(f"MongoDB 연결 실패: {e}")  # 콘솔에 출력
        return None


# datetime 혹은 문자열을 YYYY-MM-DD 형식으로 반환하는 함수
def format_date(date_value):
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%d")
    elif isinstance(date_value, str):
        return date_value[:10]
    else:
        return None


# 방사선 데이터 처리 함수
def process_radiation_data():
    try:
        client = get_mongo_connection()
        if client is None:
            raise Exception("MongoDB 연결 오류로 인해 방사선 데이터를 처리할 수 없습니다.")

        db = client['Data']
        backup_collection = db['nuclear_radiation_backup']
        weather_collection = db['NPP_weather_backup']
        stats_collection = db['radiation_stats']

        # 백업 컬렉션에서 방사선 데이터 가져오기
        backup_data = list(backup_collection.find({}, {"_id": 0}))
        logging.info(f"Backup Data Count: {len(backup_data)}")
        print(f"Backup Data Count: {len(backup_data)}")  # 콘솔에 출력

        # 기상 데이터 가져오기
        weather_data = list(weather_collection.find({}, {"_id": 0}))
        logging.info(f"Weather Data Count: {len(weather_data)}")
        print(f"Weather Data Count: {len(weather_data)}")  # 콘솔에 출력

        # 기상 데이터에서 강우 유무 저장
        rainfall_status = {}
        for entry in weather_data:
            time_field = entry.get('time', None)
            if time_field is None:
                logging.warning(f"기상 데이터에서 time 필드가 None인 항목 스킵: {entry}")
                continue

            date_str = format_date(time_field)
            if date_str is None:
                continue

            gen_name = entry.get('genName')  # region을 genName으로 변경
            rainfall = entry.get('rainfall', 0)  # 강우량

            if gen_name and date_str:
                status = 'rain' if rainfall and rainfall > 0 else 'no_rain'
                rainfall_status[(gen_name, date_str)] = status

        # 일일 평균 계산을 위한 데이터 그룹화
        daily_avg_data = {'rain': {}, 'no_rain': {}}

        for entry in backup_data:
            gen_name = entry.get('genName')  # genName을 기준으로 저장
            time_field = entry.get('time', None)
            value = entry.get('value', None)

            if gen_name is None or time_field is None or value is None:
                logging.warning(f"방사선 데이터에서 유효하지 않은 항목 스킵: {entry}")
                continue

            date_str = format_date(time_field)
            if date_str is None:
                continue

            rain_status = rainfall_status.get((gen_name, date_str), None)

            if rain_status is None:
                logging.warning(f"강우 상태 정보를 찾을 수 없습니다: {gen_name}, {date_str}")
                continue

            if rain_status == 'rain':
                daily_avg_data['rain'].setdefault((gen_name, date_str), []).append(float(value))
            else:
                daily_avg_data['no_rain'].setdefault((gen_name, date_str), []).append(float(value))

        # 평균값 계산 및 결과 저장
        avg_results = []

        def calculate_avg(data, rain_status_flag):
            for key, values in data.items():
                if not values:
                    logging.warning(f"평균값 계산 중 데이터가 없습니다: {key}")
                    continue

                avg_value = sum(values) / len(values)

                # 중복 방지를 위한 기존 데이터 확인
                existing_entry = stats_collection.find_one({"genName": key[0], "date": key[1]})
                if existing_entry:
                    logging.info(f"중복 데이터: {key[0]}, {key[1]} - 삽입하지 않음")
                    print(f"중복 데이터: {key[0]}, {key[1]} - 삽입하지 않음")  # 콘솔에 출력
                else:
                    avg_results.append({
                        "genName": key[0],  # region을 genName으로 변경
                        "date": key[1],
                        "value": avg_value,
                        "rain": rain_status_flag
                    })

        # 강우 시와 강우가 없는 시의 평균값 계산
        calculate_avg(daily_avg_data['rain'], True)
        calculate_avg(daily_avg_data['no_rain'], False)

        # 결과를 stats 컬렉션에 저장
        if avg_results:
            stats_collection.insert_many(avg_results)
            logging.info(f"방사선 데이터가 성공적으로 처리되었습니다. 처리된 데이터 수: {len(avg_results)}")
            print(f"방사선 데이터가 성공적으로 처리되었습니다. 처리된 데이터 수: {len(avg_results)}")  # 콘솔에 출력
        else:
            logging.info("중복된 데이터를 제외하고 처리된 데이터가 없습니다.")
            print("중복된 데이터를 제외하고 처리된 데이터가 없습니다.")  # 콘솔에 출력

        # 완료 시간 로그에 기록
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"방사선 데이터가 radiation_stats 컬렉션에 저장되었습니다. (현재 시간: {current_time})")
        print(f"방사선 데이터가 radiation_stats 컬렉션에 저장되었습니다. (현재 시간: {current_time})")  # 콘솔에 출력

    except Exception as e:
        logging.error(f"Error in process_radiation_data: {e}")
        print(f"Error in process_radiation_data: {e}")  # 콘솔에 출력


# 스케줄러에서 실행할 작업
def job():
    logging.info("스케줄러 작업 시작: process_radiation_data 실행")
    process_radiation_data()


# 수동 실행 및 스케줄러 실행
if __name__ == "__main__":
    # 스케줄러 설정: 매일 새벽 6시에 job 함수를 실행
    schedule.every().day.at("06:00").do(job)
    logging.info("스케줄러가 시작되었습니다. 매일 새벽 6시에 작업이 실행됩니다.")
    print("스케줄러가 시작되었습니다. 매일 새벽 6시에 작업이 실행됩니다.")

    # 스케줄러가 대기하도록 무한 루프 실행
    while True:
        schedule.run_pending()
        time.sleep(60)  # 60초마다 스케줄러 상태 확인
