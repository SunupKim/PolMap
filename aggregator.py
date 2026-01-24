# aggregator.py가 하는 일
# 입력
# - 각 키워드 폴더의 selected_archive.csv
# - 이미 클러스터링 + canonical 선정까지 끝난 결과
# 처리
# - 모든 키워드 결과를 하나로 concat
# - link 컬럼 기준으로 duplicated() 수행
# - 처음 등장한 link만 글로벌 canonical
# - 나머지는 global_replaced_by로 매핑
# 출력
# - total_news_archive.csv → link 기준으로 중복 없는 기사 집합
# - total_news_archive_meta.csv → 전체 기사 + 글로벌 중복 관계 인덱스

import os
import pandas as pd
import time
from config import SEARCH_KEYWORDS, AGGREGATE_PER_HOURS
from datetime import datetime

# 설정 경로
OUTPUT_ROOT = "outputs"
TOTAL_ARCHIVE_PATH = os.path.join(OUTPUT_ROOT, "final/total_news_archive.csv")
TOTAL_META_PATH = os.path.join(OUTPUT_ROOT, "final/total_news_archive_meta.csv")
DUPLICATE_HISTORY_PATH = os.path.join(OUTPUT_ROOT, "final/duplicate_removal_history.csv")

def run_aggregation():
    print(f"\n{'#'*20} 뉴스 데이터 통합 및 중복 로직 교정 시작 {'#'*20}")
    all_dfs = []

    # 1. 파일 로드
    for kw, _, _ in SEARCH_KEYWORDS:
        file_path = os.path.join(OUTPUT_ROOT, kw, "selected_archive.csv")
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                if not df.empty:
                    df['source_keyword'] = kw
                    all_dfs.append(df)
            except Exception as e:
                print(f"!!! [{kw}] 파일 읽기 오류: {e}")

    if not all_dfs: return

    # 2. 전체 병합 및 기존 중복 컬럼 제거
    df_total = pd.concat(all_dfs, ignore_index=True)
    df_total.drop(columns=['is_canonical', 'replaced_by'], inplace=True, errors='ignore')

    # 3. 중요: link 기준으로 정렬 (가장 먼저 수집된 것을 위로 보내기 위함)
    # pubDate가 있다면 최신순으로 정렬하여 가장 최신 것을 대표로 만듦
    if 'pubDate' in df_total.columns:
        df_total = df_total.sort_values(by='pubDate', ascending=False)

    # 4. 글로벌 중복 판별 및 분리
    # 전체에서 link 기준으로 '첫 번째' 것들만 남긴 데이터 (대표군)
    df_canonical = df_total.drop_duplicates(subset=['link'], keep='first').copy()
    df_canonical['is_global_canonical'] = True
    df_canonical['global_replaced_by'] = None

    # 전체 데이터에서 대표군에 포함되지 않은 '나머지 모든 데이터' (중복군)
    # 인덱스를 기준으로 전체에서 대표군을 제외한 나머지를 정확히 추출합니다.
    df_duplicates = df_total[~df_total.index.isin(df_canonical.index)].copy()
    
    if not df_duplicates.empty:
        # 중복군에게 그들의 'link'와 일치하는 대표군의 'news_id'를 매핑
        mapping_table = df_canonical[['link', 'news_id']].rename(columns={'news_id': 'global_replaced_by'})
        
        # merge 과정에서 중복 데이터의 인덱스를 유지하기 위해 처리
        df_duplicates = df_duplicates.merge(mapping_table, on='link', how='left')
        df_duplicates['is_global_canonical'] = False
        
        # 중복 건수 확정
        duplicate_count = len(df_duplicates)
    else:
        duplicate_count = 0

    # 4-1. 글로벌 중복 제거 이력 저장
    if not df_duplicates.empty:
        history_df = df_duplicates[[
            'source_keyword',
            'news_id',
            'link',
            'global_replaced_by'
        ]].copy()

        history_df['execute_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        os.makedirs(os.path.dirname(DUPLICATE_HISTORY_PATH), exist_ok=True)

        is_new = not os.path.exists(DUPLICATE_HISTORY_PATH)
        history_df.to_csv(
            DUPLICATE_HISTORY_PATH,
            mode='a',
            header=is_new,
            index=False,
            encoding='utf-8-sig'
        )        
    
    # 5. 최종 데이터 합치기 (메타 파일용)
    df_all_processed = pd.concat([df_canonical, df_duplicates], ignore_index=True)

    # 6. 메타 데이터 생성
    meta_columns = ['news_id', 'link', 'source_keyword', 'title', 'pubDate', 'cluster_id', 'title_id', 'body_id', 'is_global_canonical', 'global_replaced_by']
    actual_meta_cols = [c for c in meta_columns if c in df_all_processed.columns]
    df_meta_final = df_all_processed[actual_meta_cols].copy()

    # 7. 저장    
    os.makedirs(os.path.dirname(TOTAL_ARCHIVE_PATH), exist_ok=True)
    df_canonical.to_csv(TOTAL_ARCHIVE_PATH, index=False, encoding='utf-8-sig')
    df_meta_final.to_csv(TOTAL_META_PATH, index=False, encoding='utf-8-sig')

    # 8. 출력 (사용자님이 원하시는 정확한 수치 표시)
    print(f">>> 통합 완료: 전체 {len(df_total)}건 중 {len(df_canonical)}건 선별")
    print(f">>> 중복으로 {duplicate_count}건이 global_replaced_by로 매핑되었습니다.")

    
if __name__ == "__main__":
    # 설정값 로드
    INTERVAL_SECONDS = AGGREGATE_PER_HOURS * 60 * 60
    print(f"데이터 통합 스케줄러 가동 중... (주기: {AGGREGATE_PER_HOURS}시간)")
    
    while True:
        run_aggregation()
        print(f"다음 통합 작업까지 {AGGREGATE_PER_HOURS}시간 대기합니다...")
        time.sleep(INTERVAL_SECONDS)