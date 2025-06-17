from pymongo import MongoClient
import logging
import os
from dotenv import load_dotenv
import sys  # sys 모듈 임포트 추가
from datetime import datetime

# 환경 변수 로드 (여기서는 telegram_config.env 파일을 사용)
load_dotenv("telegram_config.env")

# 로깅 설정 (파일과 콘솔 모두 출력)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("radiation_processing.log"),
        logging.StreamHandler()
    ]
)

# 발전소 코드에 대한 한글 이름 매핑
plant_names = {
    "WS": "월성발전소 (경북 경주)",
    "KR": "고리발전소 (부산 기장)",
    "YK": "한빛발전소 (전남 영광)",
    "UJ": "한울발전소 (경북 울진)",
    "SU": "새울발전소 (울산 울주)"
}

# MongoDB 연결 함수
def get_mongo_connection():
    """
    MongoDB에 연결하고 클라이언트를 반환합니다.
    연결에 실패하면 스크립트를 종료합니다.
    """
    try:
        client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
        logging.info("MongoDB 연결 성공.")
        return client
    except Exception as e:
        logging.error(f"MongoDB 연결 실패: {e}")
        sys.exit(1)

# 방사선 평균값 계산 함수 (모든 데이터를 대상으로, 지역 필드는 genName으로 통일)
def calculate_radiation_averages():
    """
    radiation_stats 컬렉션의 모든 데이터를 가져와
    각 지역(genName)별로 비가 온 날과 비가 오지 않은 날을 카운트하고,
    각각의 평균 방사선량과 증가율을 계산하여
    radiation_statistics 데이터베이스의 regional_average 컬렉션에 저장합니다.
    최신 날짜의 데이터만 남도록 관리합니다.
    """
    try:
        client = get_mongo_connection()

        # 데이터베이스 및 컬렉션 설정
        db = client['Data']
        stats_collection = db['radiation_stats']
        avg_db = client['radiation_statistics']
        avg_collection = avg_db['regional_average']

        # 모든 데이터 가져오기
        data = list(stats_collection.find({}, {"_id": 0}))
        logging.info(f"radiation_stats 컬렉션에서 {len(data)}개의 레코드를 가져왔습니다.")

        if not data:
            logging.info("radiation_stats 컬렉션에 데이터가 없습니다.")
            return

        # rain 필드의 고유 값 확인 (참고용)
        unique_rain_values = set(entry.get('rain') for entry in data)
        logging.info(f"Unique rain values: {unique_rain_values}\n")

        # 강우 여부에 따라 그룹핑 및 카운팅 (지역 식별자는 genName 사용)
        grouped_data = {}
        for entry in data:
            gen_name = entry.get('genName')
            rain = entry.get('rain')
            value = entry.get('value')

            if not gen_name or value is None:
                logging.warning(f"유효하지 않은 데이터 항목 스킵: {entry}")
                continue

            if gen_name not in grouped_data:
                grouped_data[gen_name] = {
                    'rain_days': 0,
                    'no_rain_days': 0,
                    'rain_values': [],
                    'no_rain_values': []
                }

            # rain 필드의 값에 따라 강우 여부 결정
            if isinstance(rain, str):
                rain_lower = rain.lower()
                if rain_lower == 'rain':
                    grouped_data[gen_name]['rain_days'] += 1
                    grouped_data[gen_name]['rain_values'].append(float(value))
                elif rain_lower == 'no_rain':
                    grouped_data[gen_name]['no_rain_days'] += 1
                    grouped_data[gen_name]['no_rain_values'].append(float(value))
                else:
                    logging.warning(f"알 수 없는 rain 상태 스킵: {entry}")
            elif isinstance(rain, bool):
                if rain:
                    grouped_data[gen_name]['rain_days'] += 1
                    grouped_data[gen_name]['rain_values'].append(float(value))
                else:
                    grouped_data[gen_name]['no_rain_days'] += 1
                    grouped_data[gen_name]['no_rain_values'].append(float(value))
            else:
                try:
                    rain_status = float(rain) > 0
                    if rain_status:
                        grouped_data[gen_name]['rain_days'] += 1
                        grouped_data[gen_name]['rain_values'].append(float(value))
                    else:
                        grouped_data[gen_name]['no_rain_days'] += 1
                        grouped_data[gen_name]['no_rain_values'].append(float(value))
                except (TypeError, ValueError):
                    logging.warning(f"비정상적인 rain 값 스킵: {entry}")
                    continue

        # 평균값 및 증가 비율 계산
        results = []
        for gen_name, values in grouped_data.items():
            rain_avg = (sum(values['rain_values']) / len(values['rain_values'])) if values['rain_values'] else 0.0
            no_rain_avg = (sum(values['no_rain_values']) / len(values['no_rain_values'])) if values['no_rain_values'] else 0.0

            if no_rain_avg > 0:
                percentage_increase = ((rain_avg - no_rain_avg) / no_rain_avg) * 100
                percentage_increase = round(percentage_increase, 2)
            else:
                percentage_increase = 'N/A'

            results.append({
                "genName": gen_name,
                "rain_days": values['rain_days'],
                "no_rain_days": values['no_rain_days'],
                "rain_avg": rain_avg,
                "no_rain_avg": no_rain_avg,
                "percentage_increase": percentage_increase
            })

        # 현재 날짜 기준으로 저장 (예: "2025-03-11")
        today_str = str(datetime.now().date())

        # 기존 날짜 데이터 삭제 (최신 데이터만 남기기)
        avg_collection.delete_many({"date": today_str})

        # 데이터 저장
        for result in results:
            avg_collection.update_one(
                {"genName": result["genName"], "date": today_str},
                {"$set": {
                    "rain_days": result["rain_days"],
                    "no_rain_days": result["no_rain_days"],
                    "rain_avg": result["rain_avg"],
                    "no_rain_avg": result["no_rain_avg"],
                    "percentage_increase": result["percentage_increase"]
                }},
                upsert=True
            )

        # 전체 평균을 하나의 문서로 저장 (리포트용)
        avg_collection.update_one(
            {"date": today_str},
            {"$set": {"averages": results}},
            upsert=True
        )

        logging.info("강우 유무에 따른 평균 방사선 데이터가 radiation_statistics 데이터베이스의 regional_average 컬렉션에 저장되었습니다.\n")

    except Exception as e:
        logging.error(f"방사선 평균값 계산 중 오류 발생: {e}", exc_info=True)
    finally:
        client.close()

# 메인 스크립트
if __name__ == "__main__":
    print("Radiation averages manual processing started.\n")
    logging.info("Radiation averages manual processing started.\n")
    try:
        # 즉시 계산 및 저장
        calculate_radiation_averages()
        print("Manual processing 완료.\n")
        logging.info("Manual processing 완료.\n")
    except Exception as e:
        print(f"수동 처리 중 오류 발생: {e}")
        logging.error(f"수동 처리 중 오류 발생: {e}", exc_info=True)
        sys.exit(1)
