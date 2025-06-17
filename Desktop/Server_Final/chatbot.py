import json
import os

# 데이터 로딩
data_path = r"C:\Users\user\Desktop\Server_Final\data\structured_manual_data\모든_방재매뉴얼_데이터.json"
with open(data_path, encoding='utf-8') as f:
    manuals = json.load(f)


def search_manuals(query):
    results = []

    for manual in manuals:
        for section in manual['sections']:
            if query in section['section_title'] or query in section['content']:
                results.append({
                    'manual_title': manual['manual_title'],
                    'section_title': section['section_title'],
                    'excerpt': section['content'][:300] + "..."  # 내용 일부만 표시
                })

    return results


# 사용자 상호작용
print("📘 방재 매뉴얼 챗봇에 오신 것을 환영합니다.")
while True:
    user_input = input("\n❓ 질문을 입력하세요 ('exit' 입력 시 종료): ")

    if user_input.lower() in ['exit', 'quit']:
        break

    matches = search_manuals(user_input)

    if not matches:
        print("😢 관련된 내용을 찾을 수 없습니다. 다른 키워드로 질문해 주세요.")
    else:
        for match in matches[:3]:  # 상위 3개만 보여주기
            print(f"\n📗 매뉴얼: {match['manual_title']}")
            print(f"📄 섹션: {match['section_title']}")
            print(f"📝 내용: {match['excerpt']}")
