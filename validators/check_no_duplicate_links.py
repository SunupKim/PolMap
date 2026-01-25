# check_no_duplicate_links.py

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CANONICAL_ARCHIVE_PATH

def main():
    try:
        df = pd.read_csv(CANONICAL_ARCHIVE_PATH)
    except FileNotFoundError:
        print(f"파일이 없습니다: {CANONICAL_ARCHIVE_PATH}")
        sys.exit(1)

    if 'link' not in df.columns:
        print("검증 실패: link 컬럼이 없습니다.")
        sys.exit(1)

    dup_mask = df.duplicated(subset=['link'], keep=False)
    dup_count = dup_mask.sum()

    if dup_count == 0:
        print("검증 통과: link 중복 기사 없음")
        print(f"총 기사 수: {len(df)}")
        sys.exit(0)

    # 중복 발견 시
    dup_df = df[dup_mask].sort_values(by='link')

    print("검증 실패: link 중복 기사 발견")
    print(f"중복된 기사 수: {dup_count}")
    print("중복 link 목록 (상위 10개):")
    print(dup_df[['news_id', 'link']].head(10))

    # 필요하면 전체 덤프
    dup_df.to_csv(
        "outputs/final/total_news_archive_link_duplicates.csv",
        index=False,
        encoding="utf-8-sig"
    )
    print("중복 상세 파일 저장됨: outputs/final/total_news_archive_link_duplicates.csv")

    sys.exit(2)

if __name__ == "__main__":
    main()
