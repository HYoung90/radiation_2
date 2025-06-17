import os
import math
import time
from io import BytesIO

import geopandas as gpd
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from folium.features import GeoJsonTooltip, DivIcon
import branca.colormap as cm
from sklearn.preprocessing import MinMaxScaler
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image
from PIL import Image
import geopy.distance
from pymongo import MongoClient

# ----------------------------
# 발전소 좌표 및 MongoDB 설정
# ----------------------------
power_plants = {
    '고리': (35.321499, 129.291612),
    '월성': (35.713058, 129.475347),
    '한빛': (35.409234, 126.416692),
    '한울': (37.085932, 129.390857)
}

# MongoDB 에 실제 저장된 genName 값으로 맞춰주세요
mapping_codes = {
    '고리': 'KR',
    '월성': 'WS',
    '한빛': 'YK',
    '한울': 'UJ'
}

client = MongoClient('mongodb://localhost:27017')
db = client['Data']
col = db['NPP_weather']

# ----------------------------
# 대기 안정도 문자열 → 카테고리(A-G) 및 가중치 맵핑
# ----------------------------
korean_to_category = {
    '심한 불안정': 'A',
    '불안정': 'B',
    '약간 불안정': 'C',
    '중립': 'D',
    '약간 안정': 'E',
    '안정': 'F',
    '심한 안정': 'G'
}

stab_map = {
    'A': 0.2,
    'B': 0.4,
    'C': 0.6,
    'D': 0.8,
    'E': 1.0,
    'F': 1.2,
    'G': 1.5
}


# ----------------------------
# 1. 함수 정의
# ----------------------------
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1r, lat2r = math.radians(lat1), math.radians(lat2)
    dlr = math.radians(lon2 - lon1)
    x = math.sin(dlr) * math.cos(lat2r)
    y = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlr)
    bearing = math.degrees(math.atan2(x, y))
    return (bearing + 360) % 360


def adjust_wind_direction(wd):
    return (wd + 180) % 360


def calculate_wind_risk(wd, ws, bearing, stability_weight, dist_km, alpha=0.05):
    adj = adjust_wind_direction(wd)
    rel = abs(adj - bearing)
    if rel > 180:
        rel = 360 - rel
    rel_rad = math.radians(rel)
    dist_w = 1 / (1 + alpha * dist_km)
    dil = 1 / stability_weight
    risk = ws * math.cos(rel_rad) * dist_w * dil
    return max(risk, 0)


def generate_sector(lat, lon, bearing, width, radius_km=100, points=30):
    start = bearing - width / 2
    angs = [start + i * (width / points) for i in range(points + 1)]
    coords = [(lat, lon)]
    for ang in angs:
        dest = geopy.distance.distance(kilometers=radius_km).destination((lat, lon), ang)
        coords.append((dest.latitude, dest.longitude))
    return coords


def get_angle_width(stability_weight):
    min_a, max_a = 30, 60
    w = max_a - (stability_weight - 0.2) * (max_a - min_a) / (1.5 - 0.2)
    return max(min_a, min(w, max_a))


def get_sector_style(_):
    return {'fillColor': 'orange', 'color': 'orange', 'weight': 2, 'fillOpacity': 0.4}


def fetch_weather(plant):
    """MongoDB에서 최신 기상 데이터를 조회하여
       wind_direction, wind_speed, stability_category, stability_weight 반환"""
    code = mapping_codes.get(plant)
    if code is None:
        raise KeyError(f"mapping_codes에 '{plant}' 키가 없습니다.")
    # 'timestamp' → 'time' 으로 수정
    doc = col.find_one({'genName': code}, sort=[('time', -1)])
    if not doc:
        raise ValueError(f"genName='{code}'인 문서를 찾을 수 없습니다. "
                         f"컬렉션의 genName 목록: {col.distinct('genName')}")

    # 필드명 확인 ('winddirection' vs 'wind_direction', 'windspeed' vs 'wind_speed')
    wd = doc.get('winddirection') or doc.get('wind_direction')
    ws = doc.get('windspeed') or doc.get('wind_speed')
    stability_str = doc.get('stability', '')

    if wd is None or ws is None:
        raise ValueError(f"불완전한 기상 데이터: {doc}")

    category = korean_to_category.get(stability_str, 'D')
    weight = stab_map[category]

    print(f"Fetched weather for {plant}: wind={wd}°/{ws}m/s, stability='{stability_str}' → {category}({weight})")
    return {
        'wind_direction': wd,
        'wind_speed': ws,
        'stability_category': category,
        'stability_weight': weight
    }

# ----------------------------
# 2. 출력 디렉토리 생성
# ----------------------------
out_map = r'E:\논문\On going\auto\map'
out_cap = os.path.join(out_map, '캡쳐')
out_score = r'E:\논문\On going\auto\점수'
for d in [out_map, out_cap, out_score]:
    os.makedirs(d, exist_ok=True)

# ----------------------------
# 3. 파일 경로 설정
# ----------------------------
regions = {
    '부산':      r'E:\논문\On going\auto\hangjeongdong_부산광역시.geojson',
    '울산':      r'E:\논문\On going\auto\hangjeongdong_울산광역시.geojson',
    '경주':      r'E:\논문\On going\auto\hangjeongdong_경상북도.geojson',
    '영광':      r'E:\논문\On going\auto\hangjeongdong_전라남도.geojson',
    '전라북도':   r'E:\논문\On going\auto\hangjeongdong_전라북도.geojson',
    '경상남도':   r'E:\논문\On going\auto\hangjeongdong_경상남도.geojson',
    '대구광역시': r'E:\논문\On going\auto\hangjeongdong_대구광역시.geojson',
    '광주광역시': r'E:\논문\On going\auto\hangjeongdong_광주광역시.geojson',
}
pop_path = r'E:\논문\On going\auto\population2.xlsx'
shel_path = r'E:\논문\On going\auto\shelter.xlsx'


# ----------------------------
# 4. GeoJSON 읽기 및 병합
# ----------------------------
def read_geojson(p):
    try:
        g = gpd.read_file(p)
        print(f"Loaded {os.path.basename(p)}")
        return g
    except Exception as e:
        print(f"Error reading {p}: {e}")
        exit(1)


gdfs = [read_geojson(path) for path in regions.values()]
gdf = pd.concat(gdfs, ignore_index=True)
print("Merged regions.")


# ----------------------------
# 5-6. 인구, 구호소 데이터 읽기
# ----------------------------
def read_xl(path, desc):
    try:
        df = pd.read_excel(path)
        print(f"Loaded {desc}")
        return df
    except Exception as e:
        print(f"Error reading {desc}: {e}")
        exit(1)


pop_df = read_xl(pop_path, 'population')
shel_df = read_xl(shel_path, 'shelter')

# ----------------------------
# 7. 인구 병합 (adm_nm_full 기준)
# ----------------------------
# GeoJSON adm_nm 과 정확히 맞추는 컬럼 생성
# 1) 광역지자체 명 → GeoJSON과 동일하게 매핑
sido_map = {
    '부산': '부산광역시',
    '울산': '울산광역시',
    '전라남도': '전라남도',
    '경상북도': '경상북도'
}
pop_df['sido_full'] = pop_df['광역지자체'].map(sido_map)

# 2) GeoJSON adm_nm 과 정확히 일치하도록 adm_nm_full 생성
#    (예: '부산광역시 중구 중앙동', '경상북도 포항시남구 구룡포읍' 등)
pop_df['adm_nm_full'] = (
    pop_df['sido_full'] + ' ' +
    pop_df['행정구역']   + ' ' +
    pop_df['adm_cd']
)

# 3) adm_nm_full 기준으로 population merge
gdf = gdf.merge(
    pop_df[['adm_nm_full','population']],
    left_on='adm_nm',
    right_on='adm_nm_full',
    how='left'
).drop(columns=['adm_nm_full'])

# 4) 누락 확인 (머지 후 결측인 행정동만 출력)
missing = gdf[gdf['population'].isna()]
if not missing.empty:
    print("인구 병합 실패한 행정동 예시:", missing['adm_nm'].unique()[:10])
else:
    print("모든 행정동에 population이 정상 채워졌습니다!")


# ----------------------------
# 8-11. 컬러맵, CRS 변환, centroid, shelter join
# ----------------------------
pop_vals = gdf['population'].dropna()
if pop_vals.empty:
    print('No population data.');
    exit(1)
pop_cm = cm.LinearColormap(['lightyellow', 'orange', 'darkred'], vmin=pop_vals.min(), vmax=pop_vals.max())
pop_cm.caption = '인구수'

cap_vals = shel_df['capacity'].dropna()
shel_cm = cm.LinearColormap(['lightblue', 'blue', 'darkblue'], vmin=cap_vals.min(), vmax=cap_vals.max())
shel_cm.caption = '구호소 수용인원'

proj = gdf.to_crs('EPSG:5179')
proj['centroid'] = proj.geometry.centroid
gdf['centroid_lat'] = proj['centroid'].to_crs('EPSG:4326').y
gdf['centroid_lon'] = proj['centroid'].to_crs('EPSG:4326').x

sg = gpd.GeoDataFrame(
    shel_df,
    geometry=gpd.points_from_xy(shel_df.longitude, shel_df.latitude),
    crs='EPSG:4326'
)
sg = gpd.sjoin(sg, gdf[['adm_nm', 'geometry']], how='left', predicate='within')
cap_sum = sg.groupby('adm_nm')['capacity'].sum().reset_index().rename(columns={'capacity': 'capacity_sum'})
gdf = gdf.merge(cap_sum, on='adm_nm', how='left').fillna({'capacity_sum': 0})

# ----------------------------
# 12. Folium Map 생성
# ----------------------------
m = folium.Map(location=[35.3, 129.2], zoom_start=10, tiles=None)
folium.TileLayer(
    tiles='https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
    name='CartoDB Positron',
    attr='Map tiles by CartoDB, CC BY 3.0 — Map data © OpenStreetMap contributors'
).add_to(m)
MarkerCluster(name='구호소').add_to(m)
power_layer = folium.FeatureGroup(name='발전소').add_to(m)

# ----------------------------
# 13. 발전소 선택 및 기상 자동 조회
# ----------------------------
# 발전소 리스트 출력 및 사용자 선택
plants = list(power_plants.keys())
print("사고 발전소를 선택하세요:")
for i, name in enumerate(plants, 1):
    print(f"{i}. {name}")

while True:
    try:
        idx = int(input(f"번호를 입력하세요 (1-{len(plants)}): ").strip())
        if 1 <= idx <= len(plants):
            selected = plants[idx - 1]
            break
        else:
            print(f"1부터 {len(plants)} 사이의 번호를 입력하세요.")
    except ValueError:
        print("유효한 숫자를 입력하세요.")

# 선택된 발전소의 좌표 및 최신 기상 데이터 불러오기
plant_lat, plant_lon = power_plants[selected]
wind_parameters = fetch_weather(selected)

# 화살표 표시
angle = (adjust_wind_direction(wind_parameters['wind_direction']) - 90) % 360
arrow_html = f"""
<div style="transform:rotate({angle}deg);font-size:36px;color:blue">
  <i class="fa fa-arrow-circle-right"></i>
</div>"""
folium.Marker(
    location=[plant_lat, plant_lon],
    icon=DivIcon(icon_size=(50, 50), icon_anchor=(25, 25), html=arrow_html),
    popup=f"Wind: {wind_parameters['wind_direction']}°"
).add_to(power_layer)

# 발전소 마커 추가
folium.Marker(
    location=[plant_lat, plant_lon],
    icon=folium.Icon(color='red', icon='industry'),
    popup=f"{selected} 발전소"
).add_to(power_layer)

# 섹터 생성 및 지도에 추가
width = get_angle_width(wind_parameters['stability_weight'])
bearing = adjust_wind_direction(wind_parameters['wind_direction'])
coords = generate_sector(plant_lat, plant_lon, bearing, width, radius_km=60)
folium.Polygon(
    locations=coords,
    **get_sector_style(None),
    popup=f"{selected} 발전소 풍향 섹터"
).add_to(m)


# ----------------------------
# 14-15. 거리, 풍위험, TOPSIS 분석
# ----------------------------

# — 거리 계산
gdf['distance_to_nearest_plant'] = gdf.apply(
    lambda r: calculate_distance(
        plant_lat, plant_lon, r['centroid_lat'], r['centroid_lon']
    ),
    axis=1
)

# ➕ 구호소가 없는 지역(capacity_sum == 0) 제외
gdf = gdf[gdf['capacity_sum'] > 0].reset_index(drop=True)

# — 100km 이상은 평가 대상에서 제외
gdf = gdf[gdf['distance_to_nearest_plant'] < 100].reset_index(drop=True)

# — 이후 지표 계산
gdf['SC%'] = gdf.apply(
    lambda r: (r['capacity_sum'] / r['population'] * 100) if r['population'] > 0 else 0,
    axis=1
)
gdf['bearing'] = gdf.apply(
    lambda r: calculate_bearing(
        plant_lat, plant_lon, r['centroid_lat'], r['centroid_lon']
    ),
    axis=1
)
gdf['wind_risk'] = gdf.apply(
    lambda r: calculate_wind_risk(
        wind_parameters['wind_direction'],
        wind_parameters['wind_speed'],
        r['bearing'],
        wind_parameters['stability_weight'],
        r['distance_to_nearest_plant'],
        alpha=0.025
    ),
    axis=1
)

# — TOPSIS 계산 준비
criteria = gdf[['distance_to_nearest_plant', 'SC%', 'wind_risk']].fillna(0)
scaler = MinMaxScaler()
norm_vals = scaler.fit_transform(criteria)
norm_df = pd.DataFrame(norm_vals, columns=criteria.columns, index=criteria.index)

# 이상적 해 계산
w3 = 0.33
weighted3 = norm_df * w3
ideal_best3 = {
    'distance_to_nearest_plant': weighted3['distance_to_nearest_plant'].max(),
    'SC%': weighted3['SC%'].max(),
    'wind_risk': weighted3['wind_risk'].min()
}
ideal_worst3 = {
    'distance_to_nearest_plant': weighted3['distance_to_nearest_plant'].min(),
    'SC%': weighted3['SC%'].min(),
    'wind_risk': weighted3['wind_risk'].max()
}

w2 = 0.335
weighted2 = norm_df[['distance_to_nearest_plant', 'SC%']] * w2
ideal_best2 = {
    'distance_to_nearest_plant': weighted2['distance_to_nearest_plant'].max(),
    'SC%': weighted2['SC%'].max()
}
ideal_worst2 = {
    'distance_to_nearest_plant': weighted2['distance_to_nearest_plant'].min(),
    'SC%': weighted2['SC%'].min()
}


def compute_topsis(idx):
    o = criteria.loc[idx]
    n = norm_df.loc[idx]
    if o['wind_risk'] == 0:
        d_best = math.hypot(w2 * n['distance_to_nearest_plant'] - ideal_best2['distance_to_nearest_plant'],
                            w2 * n['SC%'] - ideal_best2['SC%'])
        d_worst = math.hypot(w2 * n['distance_to_nearest_plant'] - ideal_worst2['distance_to_nearest_plant'],
                             w2 * n['SC%'] - ideal_worst2['SC%'])
        return 0.33 + 0.67 * (d_worst / (d_best + d_worst)) if (d_best + d_worst) else 1
    else:
        d_best = math.sqrt(
            (w3 * n['distance_to_nearest_plant'] - ideal_best3['distance_to_nearest_plant']) ** 2 +
            (w3 * n['SC%'] - ideal_best3['SC%']) ** 2 +
            (w3 * n['wind_risk'] - ideal_best3['wind_risk']) ** 2
        )
        d_worst = math.sqrt(
            (w3 * n['distance_to_nearest_plant'] - ideal_worst3['distance_to_nearest_plant']) ** 2 +
            (w3 * n['SC%'] - ideal_worst3['SC%']) ** 2 +
            (w3 * n['wind_risk'] - ideal_worst3['wind_risk']) ** 2
        )
        return d_worst / (d_best + d_worst) if (d_best + d_worst) else 1


gdf['topsis_score'] = norm_df.index.to_series().apply(compute_topsis)

# GeoJSON 레이어에 추가
valid = gdf['topsis_score'].dropna()
if not valid.empty:
    topsis_cm = cm.LinearColormap(['blue', 'white', 'red'], vmin=0, vmax=1)
    topsis_cm.caption = 'TOPSIS Score'
    folium.GeoJson(
        gdf,
        style_function=lambda feat: {
            'fillColor': topsis_cm(feat['properties']['topsis_score']),
            'color': 'black', 'weight': 1, 'fillOpacity': 0.7
        },
        tooltip=GeoJsonTooltip(
            fields=['adm_nm', 'population', 'distance_to_nearest_plant', 'SC%', 'topsis_score'],
            aliases=['행정동:', '인구수:', '거리(km):', 'SC%:', 'TOPSIS Score:'],
            localize=True
        ),
        name='TOPSIS'
    ).add_to(m)
    topsis_cm.add_to(m)


# ----------------------------
# 16. TOP 5 표시
# ----------------------------
top5 = gdf.nlargest(5, 'topsis_score')
for _, row in top5.iterrows():
    folium.Marker(
        location=[row['centroid_lat'], row['centroid_lon']],
        popup=f"TOP5 {row['adm_nm']} (Score: {row['topsis_score']:.3f})",
        icon=folium.Icon(color='darkred', icon='hospital')
    ).add_to(m)

# ----------------------------
# 17. 동심원 및 저장
# ----------------------------
for r in [10000, 30000, 60000]:
    folium.Circle(
        location=[plant_lat, plant_lon],
        radius=r,
        color='black', fill=False, dash_array='5', weight=2,
        popup=f"{r // 1000}km 반경"
    ).add_to(m)
folium.LayerControl().add_to(m)

html_path = os.path.join(out_map, 'NPP_topsis_map.html')
m.save(html_path)
print(f"Saved map HTML: {html_path}")


# ----------------------------
# 18. HTML to 이미지
# ----------------------------
def save_map_as_image(html_path, output_image_path, image_format='png', scale=2):
    chrome_options = webdriver.ChromeOptions()
    for arg in ["--headless", "--hide-scrollbars", "--no-sandbox", "--disable-dev-shm-usage",
                "--window-size=1440,1200"]:
        chrome_options.add_argument(arg)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    try:
        driver.get(f"file:///{html_path}")
        time.sleep(5)
        screenshot = driver.get_screenshot_as_png()
        img = Image.open(BytesIO(screenshot))
        w, h = img.size
        resampling = Image.Resampling.LANCZOS if hasattr(Image, 'Resampling') else Image.LANCZOS
        img = img.resize((w * scale, h * scale), resampling)
        img.save(output_image_path, format=image_format.upper())
        print(f"Saved map image: {output_image_path}")
    except Exception as e:
        print(f"Error saving map image: {e}")
    finally:
        driver.quit()


png_path = os.path.join(out_cap, 'NPP_topsis_map.png')
tiff_path = os.path.join(out_cap, 'NPP_topsis_map.tiff')
save_map_as_image(html_path, png_path, image_format='png', scale=3)

try:
    Image.open(png_path).save(tiff_path, format='TIFF')
    print(f"Converted PNG to TIFF: {tiff_path}")
except Exception as e:
    print(f"Error converting to TIFF: {e}")