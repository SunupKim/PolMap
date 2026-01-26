# check_canonical_policy_execution.py
"""
역할
Canonical 정책이 실제로 제대로 수행되었는지 데이터 기반으로 검증한다.

대상
outputs/aggregated/canonical_archive.csv (최종 선정된 대표 기사들)
outputs/aggregated/canonical_archive_meta.csv (전체 기사 + 매핑 정보)

확인하는 것

1. 각 cluster_id별로 is_global_canonical=True가 정확히 1개인가?
   → 대표 기사 선정이 정확히 이루어졌는가

2. Canonical으로 선정된 기사가 cluster 내 pubDate 최신인가?
   → 선정 로직(pubDate 기준)이 제대로 작동하는가

3. Cluster 내 모든 기사의 canonical이 동일한가?
   → cluster 그룹화가 일관성 있게 이루어졌는가

이게 깨지면 의미하는 것
Canonical 선정 정책 자체가 올바르지 않거나,
선정 후 cluster 할당 과정에서 오류가 발생한 것이다.

참고: 논리적 일관성(is_canonical ↔ global_replaced_by 관계, 참조 무결성)은
check_global_canonical_consistency.py에서 담당한다.
"""

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CANONICAL_ARCHIVE_PATH, CANONICAL_META_PATH


def check_canonical_uniqueness_per_cluster(df_meta: pd.DataFrame) -> tuple:
    """
    각 cluster_id별로 is_global_canonical=True가 정확히 1개인가?
    """
    canonical_counts = df_meta[df_meta["is_global_canonical"] == True].groupby("cluster_id").size()
    
    all_clusters = set(df_meta["cluster_id"].unique())
    clusters_with_canonical = set(canonical_counts.index)
    clusters_without_canonical = all_clusters - clusters_with_canonical
    
    if len(clusters_without_canonical) > 0:
        return False, f"[FAIL] Canonical이 없는 cluster: {list(clusters_without_canonical)[:10]}"
    
    bad_clusters = canonical_counts[canonical_counts > 1].index.tolist()
    if len(bad_clusters) > 0:
        return False, f"[FAIL] Canonical이 2개 이상인 cluster (상위 5개): {bad_clusters[:5]}"
    
    return True, f"[OK] 모든 cluster에 canonical이 정확히 1개씩 존재 (총 {len(clusters_with_canonical)}개)"


def check_canonical_pubdate_is_latest(df_meta: pd.DataFrame) -> tuple:
    """
    Canonical으로 선정된 기사가 cluster 내 최신 pubDate를 가지는가?
    """
    df_meta = df_meta.copy()
    df_meta["pubDate_dt"] = pd.to_datetime(
        df_meta["pubDate"], 
        format="%Y-%m-%d %H:%M:%S", 
        errors="coerce"
    )
    
    errors = []
    
    for cluster_id, group in df_meta.groupby("cluster_id"):
        canonical = group[group["is_global_canonical"] == True]
        
        if len(canonical) != 1:
            continue  # 이미 위에서 검증됨
        
        canonical_pubdate = canonical.iloc[0]["pubDate_dt"]
        latest_in_cluster = group["pubDate_dt"].max()
        
        # Canonical이 cluster 내 최신이 아닌 경우
        if pd.notna(canonical_pubdate) and pd.notna(latest_in_cluster):
            if canonical_pubdate < latest_in_cluster:
                errors.append({
                    "cluster_id": cluster_id,
                    "canonical_news_id": canonical.iloc[0]["news_id"],
                    "canonical_pubdate": str(canonical_pubdate),
                    "latest_pubdate": str(latest_in_cluster),
                })
    
    if len(errors) > 0:
        error_df = pd.DataFrame(errors)
        return False, f"[FAIL] pubDate 검증 실패: {len(errors)}건\n{error_df.head(10).to_string()}"
    
    return True, "[OK] Canonical이 모두 cluster 내 최신 pubDate를 가짐"


def check_cluster_consistency(df_meta: pd.DataFrame, df_archive: pd.DataFrame) -> tuple:
    """
    Archive의 canonical 기사들이 meta의 cluster 정보와 일치하는가?
    """
    # Archive는 canonical만 포함
    canonical_in_meta = set(df_meta[df_meta["is_global_canonical"] == True]["news_id"].unique())
    news_ids_in_archive = set(df_archive["news_id"].unique())
    
    # Archive의 모든 기사가 meta에서 canonical인가?
    not_canonical_in_archive = news_ids_in_archive - canonical_in_meta
    
    if len(not_canonical_in_archive) > 0:
        return False, f"[FAIL] Archive의 기사가 meta에서 canonical이 아님: {list(not_canonical_in_archive)[:5]}"
    
    # Meta의 canonical이 모두 archive에 있는가?
    not_in_archive = canonical_in_meta - news_ids_in_archive
    
    if len(not_in_archive) > 0:
        return False, f"[FAIL] Meta의 canonical이 archive에 없음: {list(not_in_archive)[:5]}"
    
    return True, f"[OK] Archive와 meta의 canonical 기사가 완벽히 일치 ({len(news_ids_in_archive)}개)"


def main():
    # 1. 파일 읽기
    try:
        df_meta = pd.read_csv(CANONICAL_META_PATH)
    except FileNotFoundError:
        print(f"[FAIL] 파일이 없습니다: {CANONICAL_META_PATH}")
        sys.exit(1)
    
    try:
        df_archive = pd.read_csv(CANONICAL_ARCHIVE_PATH)
    except FileNotFoundError:
        print(f"[FAIL] 파일이 없습니다: {CANONICAL_ARCHIVE_PATH}")
        sys.exit(1)
    
    # 2. 필수 컬럼 확인
    required_cols_meta = ["news_id", "cluster_id", "is_global_canonical", "pubDate"]
    for col in required_cols_meta:
        if col not in df_meta.columns:
            print(f"[FAIL] 필수 컬럼 부재: {col}")
            sys.exit(1)
    
    required_cols_archive = ["news_id", "pubDate"]
    for col in required_cols_archive:
        if col not in df_archive.columns:
            print(f"[FAIL] 필수 컬럼 부재 (archive): {col}")
            sys.exit(1)
    
    # 3. 검증 수행
    checks = [
        ("Cluster별 Canonical 유일성", check_canonical_uniqueness_per_cluster(df_meta)),
        ("Canonical pubDate 최신성", check_canonical_pubdate_is_latest(df_meta)),
        ("Archive-Meta 일관성", check_cluster_consistency(df_meta, df_archive)),
    ]
    
    all_passed = True
    
    for check_name, (passed, message) in checks:
        print(f"\n[{check_name}]")
        print(message)
        
        if not passed:
            all_passed = False
    
    print("\n" + "="*60)
    if all_passed:
        print("검증 통과: Canonical 정책이 제대로 수행됨")
        sys.exit(0)
    else:
        print("검증 실패: 위 항목을 확인하세요")
        sys.exit(1)


if __name__ == "__main__":
    main()