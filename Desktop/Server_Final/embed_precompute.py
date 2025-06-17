# embed_precompute.py
from sentence_transformers import SentenceTransformer
import json
import torch

# 1) FAQ와 매뉴얼 데이터 로드
with open("faq_templates.json", encoding="utf-8") as f:
    faq_data = json.load(f)
faq_questions = [item["question"] for item in faq_data]

with open("모든_방재매뉴얼_데이터.json", encoding="utf-8") as f:
    manuals = json.load(f)
manual_texts = []
for m in manuals:
    for sec in m["sections"]:
        # section_title + 앞부분 콘텐츠
        manual_texts.append(f"{sec['section_title']} | {sec['content'][:200]}…")

# 2) 모델 로드
model = SentenceTransformer("jhgan/ko-sroberta-multitask")

# 3) 임베딩 계산
faq_embeds    = model.encode(faq_questions,  convert_to_tensor=True)
manual_embeds = model.encode(manual_texts,   convert_to_tensor=True)

# 4) 디스크에 저장
torch.save(faq_embeds,    "faq_embeds.pt")
torch.save(manual_embeds, "manual_embeds.pt")

print("✅ 임베딩 사전계산 완료")
