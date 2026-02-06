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
  2) title AND body 유사도 기준 의미적 중복 제거

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
from processors.global_news_grouper import GlobalNewsGrouper

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

def _deduplicate_global(df_total, df_original, logger):
    
    """전역 중복 제거
    처리 순서:
    1) news_id(link) 기준으로 동일 기사 제거
    2) title AND body 유사도로 의미적 중복 제거
    3) global_group_id 기준으로 대표 기사 1건만 유지
    4) 제외된 기사 목록 생성 (row 기준) - 최종 canonical에 포함되지 않은 수집 row들
    """
     
    logger.start_step("Global canonical 선정", step_number=3)

    # pubDate 정규화 및 정렬
    if "pubDate" in df_total.columns:
        df_total["pubDate"] = pd.to_datetime(df_total["pubDate"], errors="coerce")
        df_total = df_total.sort_values("pubDate", ascending=False)

    # 1️⃣ link / news_id 기준 물리 중복 제거 (먼저)
    df_after_link = (
        df_total
        .drop_duplicates(subset=["news_id"], keep="first")
        .copy()
    )

    link_excluded_count = len(df_total) - len(df_after_link)

    # 2️⃣ 의미 중복 제거 (title AND body)
    
    grouper = GlobalNewsGrouper(
        title_threshold=GLOBAL_TITLE_THRESHOLD,
        content_threshold=GLOBAL_CONTENT_THRESHOLD
    )

    df_after_link = grouper.group(df_after_link)
    df_before_similarity = df_after_link.copy()

    logger.add_metric(
        "global_similarity_groups",
        df_after_link["global_group_id"].nunique()
    )

    # 사건 단위 축소
    df_global_canonical = (
        df_after_link
        .drop_duplicates(subset=["global_group_id"], keep="first")
        .copy()
    )

    similarity_excluded_count = (
        len(df_before_similarity) - len(df_global_canonical)
    )

    # 제외된 기사 (row 기준)
    excluded_rows = df_total.merge(
        df_global_canonical[["news_id"]],
        on="news_id",
        how="left",
        indicator=True
    )

    df_excluded = excluded_rows[
        excluded_rows["_merge"] == "left_only"
    ].drop(columns=["_merge"]).copy()

    excluded_path = os.path.join(
        os.path.dirname(CANONICAL_ARCHIVE_PATH),
        "excluded_global.csv"
    )
    os.makedirs(os.path.dirname(excluded_path), exist_ok=True)
    canonical_df_save(df_excluded, excluded_path)

    print(f">>> 링크 중복으로 제거된 기사: {link_excluded_count}건")
    print(f">>> 유사도로 병합되어 제거된 기사: {similarity_excluded_count}건")
    print(f">>> 전체 제외 기사(row 기준): {len(df_excluded)}건 ({excluded_path})")

    logger.add_metric("global_canonical_count", len(df_global_canonical))
    logger.end_step(result_count=len(df_global_canonical))

    return df_global_canonical

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

    verify_file_before_write(CANONICAL_ARCHIVE_PATH)
    os.makedirs(os.path.dirname(CANONICAL_ARCHIVE_PATH), exist_ok=True)
    canonical_df_save(df_global_canonical, CANONICAL_ARCHIVE_PATH)

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
    df_global_canonical = _deduplicate_global(df_total, df_original, logger)

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
