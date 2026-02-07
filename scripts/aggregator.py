"""
scripts/aggregator.py
전역 뉴스 데이터 통합 및 중복 제거
실행: python -m scripts.aggregator

역할
- 키워드별로 이미 canonical로 확정된 기사들을 수집해
  전역(global) 단일 뉴스 풀을 구성한다.
- 이 스크립트는 키워드 맥락을 고려하지 않으며,
  전체 뉴스 기준으로 중복을 제거한다.
- 중복 제거는 다음 순서로 수행된다.
  1) link(news_id) 기준 물리적 중복 제거
  2) title OR body 유사도 기준 의미적 중복 제거 (OR + chaining)

입력
- 각 키워드 폴더의 selected_archive.csv
  (각 행은 키워드 단위에서 이미 canonical로 확정된 기사)

출력
- canonical_archive.csv
  → 전역 기준으로 중복 제거된 최종 기사 집합
"""

import os
import sys
import time
import pandas as pd
from utils.dataframe_utils import canonical_df_save
from processors.article_similarity_grouper import ArticleSimilarityGrouper
from processors.canonical_news_policy import CanonicalNewsPolicy
from utils.text_normalizer import NewsTextNormalizer

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    SEARCH_KEYWORDS,
    AGGREGATE_PER_HOURS,
    CANONICAL_ARCHIVE_PATH,    
    OUTPUT_ROOT,
    GLOBAL_TITLE_THRESHOLD,
    GLOBAL_CONTENT_THRESHOLD,
)

def _load_keyword_archives(logger):
    """키워드별 selected_archive.csv 파일 로드"""
    logger.start_step("파일 로드", step_number=1)
    all_dfs = []

    for kw, _, _ in SEARCH_KEYWORDS:
        path = os.path.join(OUTPUT_ROOT, kw, "selected_archive.csv")
        if not os.path.exists(path):
            continue

        try:
            df = pd.read_csv(path)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.add_metric(f"error_{kw}", str(e))

    logger.end_step(result_count=len(all_dfs))
    return all_dfs

def _merge_archives(all_dfs, logger):
    
    """키워드별 selected_archive.csv를 단순 병합하여
    전역 처리 대상 뉴스 풀(df_total)을 생성한다."""

    logger.start_step("데이터 병합", step_number=2)
    df_total = pd.concat(all_dfs, ignore_index=True)
    logger.add_metric("total_articles", len(df_total))
    logger.end_step(result_count=len(df_total))
    # 제외 기사 추적을 위해 원본 복사본 반환
    return df_total, df_total.copy()

def _deduplicate_global(df_total, logger):
    """
    GLOBAL dedup entrypoint
    1) news_id(link) 기준 중복 제거
    2) similarity 기반 OR + chaining 제거
    """

    logger.start_step("Global dedup", step_number=3)

    # pubDate 정렬 (최신 기사 우선 생존을 위함)
    if "pubDate" in df_total.columns:
        df_total["pubDate"] = pd.to_datetime(df_total["pubDate"], errors="coerce")
        df_total = df_total.sort_values("pubDate", ascending=False)

    # 1) link / news_id 기준 중복 제거
    df_after_link = df_total.drop_duplicates(
        subset=["news_id"], keep="first"
    ).copy()

    link_removed_count = len(df_total) - len(df_after_link)

    # 2) similarity 기반 dedup
    df_after_similarity, df_similarity_removed = _deduplicate_global_similarity(
        df_after_link,
        title_threshold=GLOBAL_TITLE_THRESHOLD,
        content_threshold=GLOBAL_CONTENT_THRESHOLD,
    )

    # 3) similarity 제거 기사 저장
    if not df_similarity_removed.empty:
        excluded_path = os.path.join(
            os.path.dirname(CANONICAL_ARCHIVE_PATH),
            "excluded_global_similarity.csv"
        )
        os.makedirs(os.path.dirname(excluded_path), exist_ok=True)
        canonical_df_save(df_similarity_removed, excluded_path)

    logger.add_metric("link_removed", link_removed_count)
    logger.add_metric("similarity_removed", len(df_similarity_removed))
    logger.add_metric("global_canonical_count", len(df_after_similarity))
    
    print(
    f"[GLOBAL] link 중복 제거: {link_removed_count}건 | "
    f"유사도 제거: {len(df_similarity_removed)}건 | "
    f"최종 canonical: {len(df_after_similarity)}건"
    )

    logger.end_step(result_count=len(df_after_similarity))
    return df_after_similarity

def _deduplicate_global_similarity(
    df: pd.DataFrame,
    title_threshold: float,
    content_threshold: float,
):
    """
    GLOBAL similarity-based deduplication
    - OR + chaining
    - connected component 당 1개만 생존
    """

    if df.empty:
        return df, pd.DataFrame()

    df = df.copy().reset_index(drop=True)

    titles = df["title"].fillna("").tolist()
    bodies = df["content"].fillna("").tolist()

    # 1) title / body 각각 OR+chaining 그룹
    title_grouper = ArticleSimilarityGrouper(title_threshold, field_name="GLOBAL_TITLE")
    body_grouper = ArticleSimilarityGrouper(content_threshold, field_name="GLOBAL_BODY")

    # 제목에서 부호 제거 전처리
    titles = [
    NewsTextNormalizer.normalize_title(t)
    for t in df["title"].fillna("").tolist()
    ]

    title_groups = title_grouper.group(titles)
    body_groups = body_grouper.group(bodies)

    # 2) OR 조건으로 union-find
    n = len(df)
    parent = list(range(n))

    def find(i):
        if parent[i] != i:
            parent[i] = find(parent[i])
        return parent[i]

    def union(i, j):
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[ri] = rj

    # title OR body가 같으면 연결
    from collections import defaultdict
    
    title_map = defaultdict(list)
    body_map = defaultdict(list)

    for i, gid in enumerate(title_groups):
        title_map[gid].append(i)

    for i, gid in enumerate(body_groups):
        body_map[gid].append(i)

    # title 기준 union
    for indices in title_map.values():
        for i in range(len(indices) - 1):
            union(indices[i], indices[i + 1])

    # body 기준 union
    for indices in body_map.values():
        for i in range(len(indices) - 1):
            union(indices[i], indices[i + 1])


    # 3) connected component 수집
    components = {}
    for i in range(n):
        root = find(i)
        components.setdefault(root, []).append(i)

    selector = CanonicalNewsPolicy()

    keep_indices = []
    drop_indices = []

    # 4) component 단위로 1개만 생존
    for indices in components.values():
        if len(indices) == 1:
            keep_indices.append(indices[0])
            continue

        group_df = df.iloc[indices]
        rep = selector.select(group_df)
        rep_idx = group_df.index.get_loc(rep.name)
        rep_global_idx = indices[rep_idx]

        keep_indices.append(rep_global_idx)
        for idx in indices:
            if idx != rep_global_idx:
                drop_indices.append(idx)

    kept_df = df.iloc[sorted(keep_indices)].copy()
    dropped_df = df.iloc[sorted(drop_indices)].copy()

    dropped_df["removed_by"] = "global_similarity"

    return kept_df, dropped_df

def _save_canonical_results(df_global_canonical, logger):
    """최종 결과 저장"""
    from utils.logger import verify_file_before_write

    logger.start_step("메타데이터 생성", step_number=4)

    def reorder(df):
        base = ["news_id", "pubDate", "collected_at"]
        cols = list(df.columns)
        rest = [c for c in cols if c not in base]
        return df[[c for c in base if c in cols] + rest]

    df_global_canonical = reorder(df_global_canonical)

    # 1) canonical_archive.csv
    verify_file_before_write(CANONICAL_ARCHIVE_PATH)
    os.makedirs(os.path.dirname(CANONICAL_ARCHIVE_PATH), exist_ok=True)
    canonical_df_save(df_global_canonical, CANONICAL_ARCHIVE_PATH)

    # 2) canonical_archive_copy.csv (신규)
    copy_cols = ["news_id", "pubDate", "title", "description"]
    copy_df = df_global_canonical[
        [c for c in copy_cols if c in df_global_canonical.columns]
    ].copy()

    copy_path = os.path.join(
        os.path.dirname(CANONICAL_ARCHIVE_PATH),
        "canonical_archive_copy.csv"
    )
    copy_df.to_csv(copy_path, index=False, encoding="utf-8-sig")

    logger.end_step(result_count=len(df_global_canonical))
    return df_global_canonical

def run_aggregation():
    """전역 뉴스 데이터 통합 메인 실행 함수"""
    from utils.logger import PipelineLogger

    logger = PipelineLogger(
        log_dir=os.path.join("logs", "aggregator"),
        module_name="aggregator"
    )

    # 1. 파일 로드
    all_dfs = _load_keyword_archives(logger)
    if not all_dfs:
        logger.save()
        return

    # 2. 데이터 병합
    df_total, df_original = _merge_archives(all_dfs, logger)

    # 3. 유사도 병합 및 중복 제거
    df_global_canonical = _deduplicate_global(df_total, logger)

    # 4. 최종 저장
    df_global_canonical = _save_canonical_results(df_global_canonical, logger)

    logger.save()

    print(
        f">>> 통합 완료: 전체 {len(df_total)}건 → "
        f"최종 {len(df_global_canonical)}건"
    )

if __name__ == "__main__":
    INTERVAL_SECONDS = AGGREGATE_PER_HOURS * 60 * 60
    print(f"데이터 통합 스케줄러 가동 중... (주기: {AGGREGATE_PER_HOURS}시간)")

    while True:
        try:
            run_aggregation()
            print(f"다음 통합 작업까지 {AGGREGATE_PER_HOURS}시간 대기합니다...")
            time.sleep(INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\n[INFO] 사용자 중단")
            break
        except Exception as e:
            print(f"\n[ERROR] 집계 오류: {e}")
            print("5초 후 재시작합니다...")
            time.sleep(5)
