# query.py
import re
import json
import numpy as np
import faiss
from pathlib import Path
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline

# — 설정(빌드된 파일 경로) —
INDEX_PATH = Path(r"C:\Users\user\Desktop\Server_Final\data\manual\manuals_index.faiss")
META_PATH  = Path(r"C:\Users\user\Desktop\Server_Final\data\manual\manuals_metadata.json")

# — 모델과 인덱스 한 번만 로드 —
st_model = SentenceTransformer("all-MiniLM-L6-v2")
idx       = faiss.read_index(str(INDEX_PATH))
meta      = json.loads(META_PATH.read_text(encoding="utf-8"))

tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-base")
model     = AutoModelForSeq2SeqLM.from_pretrained("google/flan-t5-base")
gen_pipe  = pipeline(
    "text2text-generation",
    model=model,
    tokenizer=tokenizer,
    device=-1,
    do_sample=True,
    top_p=0.9,
    temperature=0.7,
    max_new_tokens=200
)

# — 질문 유형 판별용 정규식 —
EMERGENCY_RE = re.compile(r'비상|관심|주의|경계|심각')
DEPT_RE      = re.compile(r'부서|담당|임무|역할')

def search_chunks(question: str, top_k: int = 10):
    """
    1) FAISS에서 top_k 후보 청크 검색
    2) 질문에 비상/부서 키워드 있으면 해당 유형 청크로 필터
    3) 필터 후 결과 없으면 원래 후보로 fallback
    """
    q_emb = st_model.encode(question, normalize_embeddings=True)
    scores, ids = idx.search(np.array([q_emb], dtype="float32"), top_k)
    candidates = []
    for score, i in zip(scores[0], ids[0]):
        if i < 0 or i >= len(meta):
            continue
        candidates.append({"score": float(score), "text": meta[i]["text"]})

    if not candidates:
        return []

    # 비상 관련 질문일 때
    if EMERGENCY_RE.search(question):
        filtered = [
            c for c in candidates
            if re.search(r'(백색비상|청색비상|적색비상|관심|주의|경계|심각|◦|❍|●)', c["text"])
        ]
    # 부서/담당/임무 관련 질문일 때
    elif DEPT_RE.search(question):
        filtered = [
            c for c in candidates
            if re.search(r'(부서|담당|임무|역할|기관)', c["text"])
        ]
    else:
        filtered = candidates

    # 필터 후 결과가 없으면 fallback
    return (filtered or candidates)[:5]

def generate_answer(question: str, chunks: list):
    """검색된 청크를 참고해 단계별 행동요령 생성"""
    prompt = f"아래 매뉴얼을 참고해 '{question}'에 대해 단계별 행동요령만 정리해줘.\n\n"
    for i, c in enumerate(chunks, 1):
        snippet = c["text"].replace("\n", " ")[:300]
        prompt += f"[{i}] {snippet}...\n\n"
    prompt += "위 내용을 바탕으로 단계별 행동요령을 알려줘."
    return gen_pipe(prompt)[0]["generated_text"]

if __name__ == "__main__":
    while True:
        q = input("\n행동 요령 질문을 입력하세요 (종료하려면 엔터만):\n> ").strip()
        if not q:
            break

        chunks = search_chunks(q, top_k=10)
        if not chunks:
            print("죄송해요, 관련된 지침을 찾지 못했습니다.")
            continue

        print("\n=== 검색된 청크 ===")
        for c in chunks:
            print(f"- [score: {c['score']:.4f}] {c['text'][:120]}...")

        print("\n=== 추천 행동 요령 ===")
        print(generate_answer(q, chunks))
