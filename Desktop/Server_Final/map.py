import googlemaps
import time

# 구글 API 키
gmaps = googlemaps.Client(key='AIzaSyD8Zr5V98ln07xbmIHFQuzs_ujMJ2pBOrE')

# 도로명 주소 리스트
addresses = [
    '전라남도 장성군 남면 황토단감로 10',
    '전라남도 신안군 지도읍 서촌길 52-1'
]

# 위도와 경도 리스트 생성
latitudes = []
longitudes = []

# 10개씩 끊어서 처리
batch_size = 10

for i in range(0, len(addresses), batch_size):
    batch_addresses = addresses[i:i + batch_size]
    for address in batch_addresses:
        try:
            geocode_result = gmaps.geocode(address)
            if geocode_result:
                lat_lng = geocode_result[0]['geometry']['location']
                latitudes.append(lat_lng['lat'])
                longitudes.append(lat_lng['lng'])
                print(f"Latitude: {lat_lng['lat']}, Longitude: {lat_lng['lng']}")
            else:
                latitudes.append(None)
                longitudes.append(None)
                print(f"{address} 주소를 찾을 수 없습니다.")
        except Exception as e:
            latitudes.append(None)
            longitudes.append(None)
            print(f"주소 처리 중 오류 발생: {e}")
    time.sleep(1)  # API 호출 간 대기 시간 추가

# 결과 출력
print("최종 위도 목록:", latitudes)
print("최종 경도 목록:", longitudes)
