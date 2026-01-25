# scripts/aggregator.py
# 전역 뉴스 데이터 통합 및 중복 제거
# 실행: 항상 프로젝트 루트에서만 실행하세요! 
#   python scripts/aggregator.py
#   또는
#   python -m scripts.aggregator
# 주의: 다른 디렉토리에서 실행하면 경로 오류 발생합니다

# 입력
# - 각 키워드 폴더의 selected_archive.csv
# - 이미 클러스터링 + canonical 선정까지 끝난 결과
# 처리
# - 모든 키워드 결과를 하나로 concat
# - link 컬럼 기준으로 duplicated() 수행
# - 처음 등장한 link만 글로벌 canonical  
# - 나머지는 global_replaced_by로 매핑  
# 출력
# - canonical_archive.csv → link 기준으로 중복 없는 기사 집합
# - canonical_archive_meta.csv → 전체 기사 + 글로벌 중복 관계 인덱스

import os
import sys
import pandas as pd
import time
from datetime import datetime

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import SEARCH_KEYWORDS, AGGREGATE_PER_HOURS, CANONICAL_ARCHIVE_PATH, CANONICAL_META_PATH, DUPLICATE_HISTORY_PATH, OUTPUT_ROOT
from utils.logger import PipelineLogger, verify_file_before_write

def run_aggregation():
    logger = PipelineLogger(module_name="aggregator")
    
    print(f"\n{'#'*20} 뉴스 데이터 통합 및 중복 로직 교정 시작 {'#'*20}")
    logger.start_step("파일 로드", step_number=1, metadata={"keywords": len(SEARCH_KEYWORDS)})
    all_dfs = []

    # 1. 파일 로드
    loaded_count = 0
    for kw, _, _ in SEARCH_KEYWORDS:
        file_path = os.path.join(OUTPUT_ROOT, kw, "selected_archive.csv")
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                if not df.empty:
                    df['source_keyword'] = kw
                    all_dfs.append(df)
                    loaded_count += 1
            except Exception as e:
                print(f"!!! [{kw}] 파일 읽기 오류: {e}")
                logger.add_metric(f"error_{kw}", str(e))
    
    logger.end_step(result_count=loaded_count)

    if not all_dfs:
        logger.save()
        return

    # 2. 전체 병합
    logger.start_step("데이터 병합 및 정렬", step_number=2)
    df_total = pd.concat(all_dfs, ignore_index=True)
    logger.add_metric("total_articles_before_dedup", len(df_total))
    df_total.drop(columns=['is_canonical', 'replaced_by'], inplace=True, errors='ignore')

    # 3. pubDate 정렬 (가장 최신 기사를 대표로 선정)
    if 'pubDate' in df_total.columns:
        df_total = df_total.sort_values(by='pubDate', ascending=False)
    logger.end_step(result_count=len(df_total))

    # 4. 글로벌 중복 판별 및 분리
    logger.start_step("글로벌 중복 제거", step_number=3)
    df_canonical = df_total.drop_duplicates(subset=['link'], keep='first').copy()
    df_canonical['is_global_canonical'] = True
    df_canonical['global_replaced_by'] = None
    logger.add_metric("canonical_articles", len(df_canonical))

    # [통계 출력] 키워드별 수집 대비 생존율 확인
    print("\n=== 키워드별 통합 생존율 통계 ===")
    stats = []
    if 'source_keyword' in df_total.columns:
        for kw in df_total['source_keyword'].unique():
            total_cnt = len(df_total[df_total['source_keyword'] == kw])
            survived_cnt = len(df_canonical[df_canonical['source_keyword'] == kw])
            rate = (survived_cnt / total_cnt * 100) if total_cnt > 0 else 0
            stats.append({
                "keyword": kw,
                "collected": total_cnt,
                "survived": survived_cnt,
                "rate": f"{rate:.1f}%"
            })
        stats_df = pd.DataFrame(stats).sort_values(by="survived", ascending=False)
        print(stats_df.to_string(index=False))
    print("=================================\n")

    df_duplicates = df_total[~df_total.index.isin(df_canonical.index)].copy()
    
    duplicate_count = 0
    if not df_duplicates.empty:
        mapping_table = df_canonical[['link', 'news_id']].rename(columns={'news_id': 'global_replaced_by'})
        df_duplicates = df_duplicates.merge(mapping_table, on='link', how='left')
        df_duplicates['is_global_canonical'] = False
        duplicate_count = len(df_duplicates)
        logger.add_metric("duplicate_articles", duplicate_count)
    
    logger.end_step(result_count=len(df_canonical) + duplicate_count)

    # 5. 중복 제거 이력 저장
    logger.start_step("중복 제거 이력 저장", step_number=4)
    if not df_duplicates.empty:
        history_df = df_duplicates[[
            'source_keyword',
            'news_id',
            'link',
            'global_replaced_by'
        ]].copy()
        history_df['execute_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            verify_file_before_write(DUPLICATE_HISTORY_PATH)
            os.makedirs(os.path.dirname(DUPLICATE_HISTORY_PATH), exist_ok=True)
            is_new = not os.path.exists(DUPLICATE_HISTORY_PATH)
            history_df.to_csv(
                DUPLICATE_HISTORY_PATH,
                mode='a',
                header=is_new,
                index=False,
                encoding='utf-8-sig'
            )
            logger.end_step(result_count=len(history_df))
        except Exception as e:
            logger.end_step(error=str(e))
            raise
    else:
        logger.end_step(result_count=0)
    
    # 6. 최종 데이터 합치기 (메타 파일용)
    logger.start_step("메타데이터 생성", step_number=5)
    df_all_processed = pd.concat([df_canonical, df_duplicates], ignore_index=True)
    meta_columns = ['news_id', 'link', 'source_keyword', 'title', 'pubDate', 'cluster_id', 'title_id', 'body_id', 'is_global_canonical', 'global_replaced_by']
    actual_meta_cols = [c for c in meta_columns if c in df_all_processed.columns]
    df_meta_final = df_all_processed[actual_meta_cols].copy()
    logger.end_step(result_count=len(df_meta_final))

    # 7. 저장
    logger.start_step("파일 저장", step_number=6)
    try:
        verify_file_before_write(CANONICAL_ARCHIVE_PATH)
        verify_file_before_write(CANONICAL_META_PATH)
        
        os.makedirs(os.path.dirname(CANONICAL_ARCHIVE_PATH), exist_ok=True)
        df_canonical.to_csv(CANONICAL_ARCHIVE_PATH, index=False, encoding='utf-8-sig')
        df_meta_final.to_csv(CANONICAL_META_PATH, index=False, encoding='utf-8-sig')
        
        logger.end_step(result_count=len(df_canonical))
        
        print(f">>> 통합 완료: 전체 {len(df_total)}건 중 {len(df_canonical)}건 선별")
        print(f">>> 중복으로 {duplicate_count}건이 global_replaced_by로 매핑되었습니다.")
        
    except PermissionError as e:
        logger.end_step(error=str(e))
        raise
    
    logger.save()

    
if __name__ == "__main__":
    # 설정값 로드
    INTERVAL_SECONDS = AGGREGATE_PER_HOURS * 60 * 60
    print(f"데이터 통합 스케줄러 가동 중... (주기: {AGGREGATE_PER_HOURS}시간)")
    
    while True:
        try:
            run_aggregation()
            print(f"다음 통합 작업까지 {AGGREGATE_PER_HOURS}시간 대기합니다...")
            time.sleep(INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\n[INFO] 사용자 중단 신호 (Ctrl+C)")
            break
        except Exception as e:
            print(f"\n[ERROR] 집계 오류: {e}")
            print(f"5초 후 재시작합니다...")
            time.sleep(5)
            continue
