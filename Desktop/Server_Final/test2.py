import pandas as pd

school_path = r'E:\논문\On going\auto\학교.csv'

# 1. 파일 불러오기 (인코딩은 cp949 또는 utf-8)
try:
    school_df = pd.read_csv(school_path, encoding='cp949')
except:
    school_df = pd.read_csv(school_path, encoding='utf-8')

# 2. '소재지지번주소'에서 시도/시군구/행정동명 추출
def parse_korean_address(addr):
    try:
        parts = str(addr).strip().split()
        if len(parts) >= 4:
            # 강원특별자치도 정선군 여량면
            return pd.Series({
                '시도명': parts[0],
                '시군구명': parts[1],
                '행정동명': parts[2]
            })
        else:
            return pd.Series({'시도명': '', '시군구명': '', '행정동명': ''})
    except:
        return pd.Series({'시도명': '', '시군구명': '', '행정동명': ''})

school_df[['시도명','시군구명','행정동명']] = school_df['소재지지번주소'].apply(parse_korean_address)

# 3. 결과 미리보기
print(school_df[['학교급구분','소재지지번주소','시도명','시군구명','행정동명','위도','경도']].head(10))

# 4. (선택) 새 파일로 저장
out_path = r'E:\논문\On going\auto\학교_행정동추가.csv'
school_df.to_csv(out_path, index=False, encoding='utf-8-sig')
print(f"행정동 컬럼 추가된 파일 저장 완료: {out_path}")
