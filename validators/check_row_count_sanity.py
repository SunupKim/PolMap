# check_row_count_sanity.py

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CANONICAL_ARCHIVE_PATH, CANONICAL_META_PATH

def main():
    df_main = pd.read_csv(CANONICAL_ARCHIVE_PATH)
    df_meta = pd.read_csv(CANONICAL_META_PATH)

    if len(df_main) == 0:
        print("[FAIL] total_news_archive가 비어 있음")
        sys.exit(1)

    if len(df_meta) < len(df_main):
        print("[FAIL] meta 행 수 < canonical 행 수")
        sys.exit(1)

    dup_ratio = (
        df_meta["is_global_canonical"].value_counts(normalize=True)
        .get(False, 0)
    )

    if dup_ratio > 0.9:
        print("[WARN] 글로벌 중복 비율이 90% 초과")
        sys.exit(2)

    print("행 수 및 비율 sanity check 통과")
    sys.exit(0)

if __name__ == "__main__":
    main()
