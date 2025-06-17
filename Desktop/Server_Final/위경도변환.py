import os
import googlemaps
import pandas as pd
from dotenv import load_dotenv

load_dotenv("telegram_config.env")

# 구글 API 키
gmaps_key = os.getenv("GMAPS_KEY")
gmaps = googlemaps.Client(key=gmaps_key)

# 엑셀 파일 경로
input_file_path = r"E:\논문\On going\auto\임시주거시설1.xlsx"

# 현재 파일이 위치한 폴더 경로를 기준으로 출력 파일 경로 설정
output_file_path = os.path.join(os.path.dirname(input_file_path), "지진임시주거시설_위경도_추가1.xlsx")

# 엑셀 파일 읽기
df = pd.read_excel(input_file_path)

# 도로명 주소 리스트
addresses = df['주소']

# 위도와 경도 리스트 생성
latitudes = []
longitudes = []

# 주소마다 위경도 추출
for address in addresses:
    geocode_result = gmaps.geocode(address)
    if geocode_result:
        lat = geocode_result[0]['geometry']['location']['lat']
        lng = geocode_result[0]['geometry']['location']['lng']
        latitudes.append(lat)
        longitudes.append(lng)
    else:
        latitudes.append(None)
        longitudes.append(None)

# 데이터프레임에 위도와 경도 추가
df['latitude'] = latitudes
df['longitude'] = longitudes

# 수정된 데이터프레임을 같은 폴더에 저장
df.to_excel(output_file_path, index=False)

# 결과 파일 경로 출력
print(f"결과 파일이 저장되었습니다: {output_file_path}")
