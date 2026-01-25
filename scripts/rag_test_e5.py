# rag_test_e5.py
# 지금 코드는 매번 임베딩을 다시 한다. 이래서는 안 된다.


import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from config import CANONICAL_ARCHIVE_PATH

MODEL_NAME = "dragonkue/multilingual-e5-small-ko-v2"

TOP_K = 5

def build_corpus(df: pd.DataFrame):
    texts = []
    meta = []

    for _, row in df.iterrows():
        content = row.get("content", "")
        text = f"passage: {row['title']} {content}"
        texts.append(text)
        meta.append({
            "news_id": row["news_id"],
            "title": row["title"]
        })

    return texts, meta


def main():
    print("데이터 로딩")
    df = pd.read_csv(CANONICAL_ARCHIVE_PATH)

    print("모델 로딩")
    model = SentenceTransformer(MODEL_NAME)

    print("코퍼스 구성")
    corpus_texts, meta = build_corpus(df)

    print("임베딩 생성")
    corpus_embeddings = model.encode(
        corpus_texts,
        batch_size=32,
        show_progress_bar=True,
        normalize_embeddings=True
    )

    while True:
        query = input("\n질문 입력 (종료: exit): ")
        if query.lower() == "exit":
            break

        query_text = f"query: {query}"
        query_embedding = model.encode(
            query_text,
            normalize_embeddings=True
        )

        sims = cosine_similarity(
            [query_embedding],
            corpus_embeddings
        )[0]

        top_idx = np.argsort(sims)[::-1][:TOP_K]

        print("\n=== 검색 결과 ===")
        for rank, idx in enumerate(top_idx, 1):
            print(f"\n[{rank}] score={round(float(sims[idx]), 3)}")
            print(meta[idx]["title"])

if __name__ == "__main__":
    main()