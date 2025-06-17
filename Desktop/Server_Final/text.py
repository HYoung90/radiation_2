import json
import os
import re


def clean_extracted_text(text):
    # 1. 페이지 구분자 제거
    text = re.sub(r'=== Page\s+\d+\s*===', '', text)

    # 2. 불필요한 줄바꿈 및 연속된 공백 처리
    text = re.sub(r'\r\n|\n|\r', ' ', text)  # 모든 줄바꿈을 공백으로 통일
    text = re.sub(r'\s+', ' ', text).strip()  # 연속된 공백을 하나로 줄이고 앞뒤 공백 제거

    # 3. 매뉴얼별 특정 머리글/바닥글 제거 (예시, 추가 필요시 수정)
    # 공통적으로 보이는 패턴들
    text = re.sub(r'관리\s*번호\s*사회재난\s*-\s*\d+\s*', '', text)
    text = re.sub(r'Blue\s*관심\s*Yellow\s*주의\s*Orange\s*경계\s*\(백색비상\)\s*Red\s*심각\s*\(청․적색비상\)', '', text)
    text = re.sub(r'Blue\s*관심\s*Yellow\s*주의\s*Orange\s*경계\s*Red\s*심각', '', text)
    text = re.sub(r'원자력안전위원회', '', text)
    text = re.sub(r'울산광역시', '', text)
    text = re.sub(r'경상북도 경주시', '', text)
    text = re.sub(r'경상북도', '', text)
    text = re.sub(r'전라남도', '', text)
    text = re.sub(r'영광군', '', text)
    text = re.sub(r'부산광역시', '', text)
    text = re.sub(r'기상청', '', text)
    text = re.sub(r'해양수산부', '', text)
    text = re.sub(r'식품의약품안전처', '', text)
    text = re.sub(r'해양경찰청', '', text)

    # 페이지 번호 제거 (다양한 형태 고려)
    text = re.sub(r'\s*-\s*\d+\s*-\s*', ' ', text)
    text = re.sub(r'^\s*\d+\s*$', '', text, flags=re.MULTILINE)

    # 양식 관련 문구 제거
    text = re.sub(r'<양식-\d+\.\s*[^>]+>', '', text)
    text = re.sub(r'【제\d+호서식】', '', text)
    text = re.sub(r'발\s*간\s*등\s*록\s*번\s*호\S*', '', text)
    text = re.sub(r'^(재난|사회재난)\s*-\s*\d+\s*$', '', text, flags=re.MULTILINE)

    # 표로 깨진 데이터에서 자주 보이는 패턴들 (추가적인 분석 필요)
    text = re.sub(r'지\s*점\s*피폭선량률\s*\(mSv/hr\)\s*총피폭선량\s*\(mSv\)\s*선량률\s*\(mR/hr\)\s*비\s*고\s*전\s*신\s*갑상선\s*전\s*신\s*갑상선',
                  '', text)
    text = re.sub(r'구\s*분\s*거리\s*\(m\)\s*선량률\s*\(mR/hr\)\s*오염도\s*\(kBq/m3\)\s*비\s*고\s*소\s*내\s*소\s*외', '', text)
    text = re.sub(r'개정번호\s*\(승인일자\)\s*주요\s*개정\s*내용\s*관련근거\s*비고', '', text)
    text = re.sub(
        r'업소명\s*대표자\s*소재지\s*전화번호\s*영업종류\s*식품제조업\s*,\s*식품수입업\s*,\s*식품소분업\s*,\s*기타식품판매업\s*,\s*식품\s*접객업\s*,\s*초등학교주변판매점\s*,\s*도소매업등기타식품판매업',
        '', text)
    text = re.sub(r'구분\s*점검사항\s*결과\s*판매금지\s*식품의취급판매금지식품등의판매또는판매의목적으로채취ㆍ제조ㆍ수입ㆍ가공ㆍ사용ㆍ조리ㆍ저장ㆍ운반또는진열여부', '', text)
    text = re.sub(r'연번\s*소\s*속\s*담당자\s*\(전화번호\)\s*인원\s*배치장소\s*투입시간\s*철수시간\s*비고', '', text)
    text = re.sub(
        r'연번\s*제품정보\s*수거검사\s*제품명\s*식품유형\s*제조일자\s*유통기한\s*제조업소\s*식품등수입판매업소\s*수거장소\s*수거일자\s*검사기관\s*검사완료일자\s*검사결과\s*기준', '',
        text)
    text = re.sub(r'기관명\s*책임자\s*전화번호\s*인원\s*명\s*장비\s*기\s*대\s*배치장소\s*투입시간\s*철수시간\s*비고', '', text)

    # 기타 자주 보이는 불필요한 패턴
    text = re.sub(r'\[Image \d+\]', '', text)
    text = re.sub(r'\"[^\"]*\"', '', text)
    text = re.sub(r'^\s*-\s*\S+\s*-\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'[가-힣]+\s*관리과', '', text)
    text = re.sub(r'[가-힣]+\s*청', '', text)
    text = re.sub(r'[가-힣]+\s*시', '', text)
    text = re.sub(r'[가-힣]+\s*도', '', text)
    text = re.sub(
        r'이 매뉴얼은 재난 및 안전관리 기본법 제34조의5, 국가위기관리기본지침 및 원전안전분야 위기관리 표준매뉴얼에 따라 위기상황을 상정하고 위기 대응을 위한 절차·기준·요령과 각종 양식, 보도자료 등에 관한 사항을 규정하고 있다.',
        '', text)
    text = re.sub(r'이 매뉴얼과 관련된 모든 업무담당자는 이 매뉴얼에 규정된 절차와 내용에 따라 재난상황에 대응하여야 한다.', '', text)
    text = re.sub(
        r'다만, 재난상황이라는 것은 때와 장소, 재난 유형 등 수많은 변수가 있어 이 매뉴얼에서 규정한 것을 참고하여 상황에 따라 융통성을 발휘하여 탄력적으로 재난에 대응하되 적극적으로 대응한다.', '',
        text)
    text = re.sub(r'방사선비상이 발생하면 신속히 대응하는 것이 바람직하며, 이후 상황에 따라 적절히 대응수준을 탄력적으로 조절한다.', '', text)
    text = re.sub(r'매뉴얼 적용 기본원칙', '', text)  # 공통적으로 나오는 이 구절 자체도 제거. 내용이 다른 섹션에 포함될 수 있으므로.

    # 최종적으로 다시 연속 공백을 하나로 줄이고 앞뒤 공백 제거
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def parse_sections(text):
    sections = []

    # 섹션 제목 패턴 정의:
    # 1. 로마 숫자 (I, II, III 등) + . + 공백 + 제목
    # 2. 숫자 (1, 2, 3 등) + . + 공백 + 제목
    # 3. 한글 대분류 (가., 나. 등) + 제목 (현재 매뉴얼에서는 덜 보이지만 추가)

    # 주요 섹션 구분자 (로마 숫자 또는 숫자)
    # 텍스트를 처리하기 쉽게 각 섹션의 시작 부분에 임시 마커를 삽입합니다.
    # 예: __SECTION_START__Ⅰ. 일반사항
    # 예: __SECTION_START__1. 개요

    # 로마 숫자 대분류
    text = re.sub(r'(?<![가-힣])([Ⅰ-Ⅸ]\.\s*[^.\n]+)', r'__SECTION_START__\1', text)
    # 숫자 대분류 (문장 시작 부분, 또는 이미 다른 섹션으로 나뉘지 않은 부분)
    text = re.sub(r'(?<!\d)([1-9]\.\s*[^.\n]+)', r'__SECTION_START__\1', text)

    # 이제 마커로 분리합니다.
    split_content = text.split('__SECTION_START__')

    # 첫 번째 요소는 마커가 없었으므로, 빈 문자열이거나 마커 앞의 내용일 수 있습니다.
    # 유효한 내용이 있다면 "머리말" 등으로 처리하거나 버립니다.
    if split_content and split_content[0].strip():
        # 첫 부분은 대부분 매뉴얼 적용 기본원칙이나 소개글일 수 있습니다.
        # 너무 짧은 내용은 무시하고, 어느 정도 길이가 있다면 "머리말" 또는 "개요"로 처리합니다.
        intro_content = split_content[0].strip()
        if len(intro_content) > 100:  # 적절한 길이 기준 설정
            sections.append({"section_title": "매뉴얼 개요", "content": intro_content})

    for part in split_content[1:]:
        match_roman = re.match(r'([Ⅰ-Ⅸ]\.\s*[^.\n]+)\s*(.*)', part, re.DOTALL)
        match_numeric = re.match(r'([1-9]\.\s*[^.\n]+)\s*(.*)', part, re.DOTALL)

        if match_roman:
            title = match_roman.group(1).strip()
            content = match_roman.group(2).strip()
        elif match_numeric:
            title = match_numeric.group(1).strip()
            content = match_numeric.group(2).strip()
        else:  # 패턴에 맞지 않는 경우, 이전 섹션에 추가하거나 '기타'로 처리
            if sections:  # 이전 섹션이 있다면 그곳에 내용을 추가
                sections[-1]["content"] += " " + part.strip()
            else:  # 이전 섹션이 없다면 '기타' 섹션으로 시작
                sections.append({"section_title": "기타 내용", "content": part.strip()})
            continue  # 다음 부분으로 넘어감

        # 중복 공백 제거
        content = re.sub(r'\s+', ' ', content).strip()

        if title and content:
            sections.append({"section_title": title, "content": content})
        elif title:  # 내용이 없더라도 제목이 있다면 추가 (빈 섹션)
            sections.append({"section_title": title, "content": ""})

    # 내용이 없는 섹션 (페이지 번호만 있던 경우 등) 제거
    sections = [s for s in sections if s["content"].strip()]

    # 여전히 섹션이 없거나, 너무 긴 하나의 섹션만 있다면 전체 내용을 fallback
    if not sections or (len(sections) == 1 and len(sections[0]["content"]) > 2000):  # 긴 내용 판단 기준
        return [{"section_title": "매뉴얼 전체 내용", "content": text}]

    return sections


# 경로 설정
extracted_text_dir = r"C:\Users\user\Desktop\Server_Final\data\extracted_manual_texts"
json_output_dir = r"C:\Users\user\Desktop\Server_Final\data\structured_manual_data"
os.makedirs(json_output_dir, exist_ok=True)

all_manuals_data = []

# 모든 추출된 텍스트 파일에 대해 반복
for filename in os.listdir(extracted_text_dir):
    if filename.lower().endswith(".txt"):
        filepath = os.path.join(extracted_text_dir, filename)

        with open(filepath, 'r', encoding='utf-8') as f:
            raw_text = f.read()

        cleaned_text = clean_extracted_text(raw_text)

        # 섹션 자동 분리 시도
        parsed_sections = parse_sections(cleaned_text)

        # 파일 이름을 기반으로 기본 메타데이터 추출 (필요시 더 정교하게 파싱)
        manual_title = filename.replace(".txt", "").replace("_", " ")
        publisher = "알 수 없음"
        publication_date = "알 수 없음"

        # Publisher 및 Publication Date 추론 로직 (업데이트된 부분)
        if "원자력안전위원회" in filename:
            publisher = "원자력안전위원회"
            if "1907" in filename:
                publication_date = "2019.07"
            elif "1902" in filename:
                publication_date = "2019.02"
        elif "울산시" in filename:
            publisher = "울산광역시"
            publication_date = "2020.05"
        elif "영광군" in filename:
            publisher = "영광군"
            publication_date = "2006.11.30 (최초) / 2010.11.10 (개정)"
        elif "부산광역시" in filename:
            publisher = "부산광역시"
            publication_date = "2020.06"
        elif "경주시" in filename:
            publisher = "경상북도 경주시"
            publication_date = "2020.04"
        elif "기상청" in filename:
            publisher = "기상청"
            publication_date = "2019.07"
        elif "해양수산부" in filename:
            publisher = "해양수산부"
            publication_date = "2019.05"
        elif "식약처" in filename:
            publisher = "식품의약품안전처"
            publication_date = "2019.07"
        elif "해양경찰청" in filename:
            publisher = "해양경찰청"
            publication_date = "2019.08"
        elif "경상북도" in filename:
            publisher = "경상북도"
            publication_date = "2020.04"
        elif "전라남도" in filename:
            publisher = "전라남도"
            publication_date = "2020.01"

        manual_data = {
            "manual_id": os.path.splitext(filename)[0],
            "manual_title": manual_title,
            "publisher": publisher,
            "publication_date": publication_date,
            "재난유형": "방사능 누출",
            "sections": parsed_sections  # 자동 분리된 섹션 사용
        }
        all_manuals_data.append(manual_data)

# 모든 매뉴얼 데이터가 포함된 최종 JSON 파일로 저장
json_output_path = os.path.join(json_output_dir, "모든_방재매뉴얼_데이터.json")
with open(json_output_path, 'w', encoding='utf-8') as f:
    json.dump(all_manuals_data, f, ensure_ascii=False, indent=4)

print(f"\n모든 매뉴얼 데이터가 '{json_output_path}' 파일로 저장되었습니다.")
print(f"총 {len(all_manuals_data)}개의 매뉴얼이 처리되었습니다.")