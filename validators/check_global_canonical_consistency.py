# check_global_canonical_consistency.py
"""역할
글로벌 canonical 정책이 논리적으로 자기모순이 없는지 확인한다.

대상
outputs/aggregated/canonical_archive_meta.csv
(또는 is_global_canonical, global_replaced_by가 함께 있는 메타 파일)

확인하는 것

is_global_canonical == True 인 행은
반드시 global_replaced_by가 비어 있어야 한다.

is_global_canonical == False 인 행은
반드시 global_replaced_by가 존재해야 한다.

global_replaced_by에 적힌 news_id는
실제로 같은 파일 안에 존재해야 한다.

하나의 link 그룹 안에서
global canonical은 정확히 1개여야 한다.

이게 깨지면 의미하는 것
“대표 기사”라는 개념 자체가 붕괴된 상태다.
데이터는 있어도 정책이 깨진 데이터다.
"""

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CANONICAL_META_PATH

def main():
    df = pd.read_csv(CANONICAL_META_PATH)

    # 1. canonical ↔ replaced_by 관계
    bad_1 = df[(df["is_global_canonical"] == True) & df["global_replaced_by"].notna()]
    bad_2 = df[(df["is_global_canonical"] == False) & df["global_replaced_by"].isna()]

    if not bad_1.empty or not bad_2.empty:
        print("[FAIL] canonical / replaced_by 규칙 위반")
        sys.exit(1)

    # 2. replaced_by 참조 무결성
    valid_ids = set(df["news_id"].astype(str))
    invalid_ref = df[
        df["global_replaced_by"].notna() &
        ~df["global_replaced_by"].astype(str).isin(valid_ids)
    ]

    if not invalid_ref.empty:
        print("[FAIL] 존재하지 않는 news_id를 global_replaced_by로 참조")
        sys.exit(1)

    # 3. link 당 canonical 1개 규칙
    canon_per_link = (
        df[df["is_global_canonical"] == True]
        .groupby("link")
        .size()
    )

    if (canon_per_link != 1).any():
        print("[FAIL] link별 canonical 수가 1이 아님")
        sys.exit(1)

    print("글로벌 canonical 정합성 검증 통과")
    sys.exit(0)

if __name__ == "__main__":
    main()
