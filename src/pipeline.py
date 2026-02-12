# pipeline.py
# 뉴스 수집 파이프라인의 핵심 함수
# 실행: python -c "from pipeline import run_news_pipeline; run_news_pipeline(...)"

from api.naver_news_client import NaverNewsClient
from api.news_repository import NewsRepository
from processors.single_news_pre_post_filter import SingleNewsPrePostFilter
from processors.single_news_scraper import SingleNewsScraper
from processors.single_news_clusterer import SingleNewsClusterer
from config import (
    SINGLE_TITLE_THRESHOLD, 
    SINGLE_CONTENT_THRESHOLD, 
    NAVER_ID, 
    NAVER_SECRET, 
    EXCLUDE_WORDS_STR,
    OUTPUT_ROOT
)
from utils.logger import PipelineLogger
from config import SIMHASH_TITLE_DISTANCE, SIMHASH_BODY_DISTANCE
from processors.simhash_deduplicator import SimHashDeduplicator
from utils.simhash_log import save_simhash_removed
from datetime import datetime
import os

def run_news_pipeline(keyword: str, total_count: int, is_keyword_required: bool, log_dir: str = "logs"):

    logger = PipelineLogger(log_dir=log_dir, module_name=f"pipeline_{keyword}")
    print(f"\n{'='*20} [{keyword}] 파이프라인 가동 {'='*20}")
    
    pipeline_stats = {
        "keyword": keyword,
        "new_raw": 0,
        "final_added": 0,
        "status": "initialized"
    }    

    try:
        # 1. 초기화 및 API 호출
        client = NaverNewsClient(NAVER_ID, NAVER_SECRET)
        repo = NewsRepository(keyword, base_path=OUTPUT_ROOT)
        nf = SingleNewsPrePostFilter(keyword, is_keyword_required=is_keyword_required, exclude_words_str=EXCLUDE_WORDS_STR, base_path=OUTPUT_ROOT)
        ns = SingleNewsScraper(delay=0.1)

        simhash_deduplicator = SimHashDeduplicator(
            title_distance=SIMHASH_TITLE_DISTANCE,
            body_distance=SIMHASH_BODY_DISTANCE
            
        )

        cluster_tool = SingleNewsClusterer(title_threshold=SINGLE_TITLE_THRESHOLD, content_threshold=SINGLE_CONTENT_THRESHOLD)

        raw_items = client.fetch_news_batch(keyword, total_count=total_count)
        df_new = repo.save_raw_and_get_new(raw_items)
        
        if df_new.empty:
            print(f">>> [{keyword}] 새로운 기사가 없습니다.")
            pipeline_stats["status"] = "success"
            logger.save()
            return pipeline_stats

        pipeline_stats["new_raw"] = len(df_new)   
        
        # STEP 3: 사전 필터링                
        df_step3 = nf.apply_pre_filter(df_new)
        
        if df_step3.empty:
            print(f">>> [{keyword}] 사전 필터링 결과 0건. 파이프라인 종료.")
            pipeline_stats["status"] = "success"
            logger.save()
            return pipeline_stats
        
        # STEP 4: 본문 스크래핑
        df_step4 = ns.fetch_contents(df_step3)
        
        # STEP 5: 사후 필터링
        df_step5 = nf.apply_post_filter(df_step4)
        
        # STEP 6: SimHash 전처리 (추가)        
        df_step6, df_simhash_removed = simhash_deduplicator.deduplicate(df_step5)
        save_simhash_removed(df_simhash_removed, keyword)

        # STEP 7: 클러스터링
        df_clustered, stats = cluster_tool.process(df_step6, keyword)
        # df_clustered : 클러스터링 및 대표 기사 선정이 적용된 실제 기사별 클러스터링/대표기사 결과 DataFrame
        # stats : 통계 정보 딕셔너리 (클러스터 개수, 대표 기사 개수 등 요약 통계)        
        
        # STEP 7: Keyword별 최종 병합
        if not df_clustered.empty:
            df_final = df_clustered[df_clustered["is_canon"] == True].copy()
            added_cnt = repo.merge_final_incremental(df_final)
            pipeline_stats["final_added"] = added_cnt
            print(f"[Final] {keyword}: 신규 {added_cnt}건 추가됨.")
        
        # 성공 상태 기록
        pipeline_stats["status"] = "success"  
        logger.save()

    except Exception as e:
        # 실패 시 상태 기록
        pipeline_stats["status"] = f"fail: {str(e)}"
        logger.end_step(error=str(e))
        logger.save()
        print(f"!!! [{keyword}] 파이프라인 실행 중 오류 발생: {e}")
    
    return pipeline_stats
