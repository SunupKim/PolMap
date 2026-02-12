# python -m politician.step1_related_news

import os
import pandas as pd

# ===== 설정 =====
SOURCE_PATH = "archive/aggregated/canonical_archive.csv"
OUTPUT_DIR = "data/politician/ianju"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "articles.csv")
KEYWORD = "이언주"

def main():
    print("STEP 1 | 이언주 기사 슬라이싱 시작")

    df = pd.read_csv(SOURCE_PATH)

    # 필수 컬럼 확인
    required_cols = {"news_id", "title", "content", "pubDate"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"필수 컬럼 누락: {missing}")

    before = len(df)

    # title + content 키워드 필터
    mask = (
        df["title"].astype(str).str.contains(KEYWORD, na=False) |
        df["content"].astype(str).str.contains(KEYWORD, na=False)
    )
    sliced = df[mask].copy()

    after = len(sliced)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sliced = sliced[["news_id", "title", "content", "pubDate"]]
    sliced.to_csv(OUTPUT_PATH, index=False)

    print(f"전체 기사 수: {before}")
    print(f"이언주 기사 수: {after}")
    print(f"출력 경로: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
