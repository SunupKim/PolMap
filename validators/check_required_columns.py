# check_required_columns.py
"""
1. 역할
데이터 파일이 예상한 스키마를 유지하고 있는지 확인한다.

2. 대상
canonical_archive.csv
canonical_archive_meta.csv

3. 확인하는 것
필수 컬럼 존재 여부
예: news_id link title pubDate is_global_canonical global_replaced_by 
컬럼명이 바뀌거나 사라지지 않았는지

타입까지 보려면
link는 문자열
is_global_canonical은 bool 성격인지 등

이게 깨지면 의미하는 것
로직 문제가 아니라 진화 중인 코드가 데이터를 밀어버린 상태다.
"""

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CANONICAL_ARCHIVE_PATH, DUPLICATE_HISTORY_PATH

TARGETS = {
    f"{CANONICAL_ARCHIVE_PATH}": ["news_id", "link", "title", "pubDate"],
    f"{DUPLICATE_HISTORY_PATH}": ["news_id", "link", "global_replaced_by"]
}

def main(): 
    for path, required_cols in TARGETS.items():
        try:
            df = pd.read_csv(path)
        except FileNotFoundError:
            print(f"[FAIL] 파일 없음: {path}")
            sys.exit(1)

        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"[FAIL] {path} 누락 컬럼: {missing}")
            sys.exit(1)

        print(f"[OK] {path} 컬럼 검증 통과")

    print("모든 컬럼 스키마 검증 통과")
    sys.exit(0)

if __name__ == "__main__":
    main()
