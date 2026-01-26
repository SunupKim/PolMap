# pipeline.py
# 뉴스 수집 파이프라인의 핵심 함수
# 실행: python -c "from pipeline import run_news_pipeline; run_news_pipeline(...)"

from api.naver_news_client import NaverNewsClient
from api.news_repository import NewsRepository
from processors.news_filter import NewsFilter
from processors.news_scraper import NewsScraper
from processors.news_cluster import NewsCluster
from config import TITLE_THRESHOLD, CONTENT_THRESHOLD, NAVER_ID, NAVER_SECRET, EXCLUDE_WORDS_STR
from utils.logger import PipelineLogger

def run_news_pipeline(keyword: str, total_count: int, is_keyword_required: bool, log_dir: str = "logs"):
    """
    뉴스 수집 파이프라인 실행
    
    Args:
        keyword: 검색 키워드
        total_count: 수집 대상 기사 수
        is_keyword_required: True면 제목에 키워드 포함 필수
        log_dir: 로그 파일이 저장될 디렉토리 경로
    
    Returns:
        dict: {keyword, new_raw, final_added, status}
    """
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
        #logger.start_step("API 호출 및 저장소 초기화", step_number=1, metadata={"is_keyword_required": is_keyword_required})
        client = NaverNewsClient(NAVER_ID, NAVER_SECRET)
        repo = NewsRepository(keyword)
        nf = NewsFilter(keyword, is_keyword_required=is_keyword_required, exclude_words_str=EXCLUDE_WORDS_STR)
        ns = NewsScraper(delay=0.1)
        cluster_tool = NewsCluster(title_threshold=TITLE_THRESHOLD, content_threshold=CONTENT_THRESHOLD)

        # STEP 1 & 2: 뉴스 수집 및 저장
        raw_items = client.fetch_news_batch(keyword, total_count=total_count)
        df_new = repo.save_raw_and_get_new(raw_items)
        #logger.end_step(result_count=len(df_new))
        
        if df_new.empty:
            print(f">>> [{keyword}] 새로운 기사가 없습니다.")
            pipeline_stats["status"] = "success"
            logger.save()
            return pipeline_stats

        pipeline_stats["new_raw"] = len(df_new)
        
        # [DEBUG] 필터링 전 데이터 및 설정 확인
        # print(f"DEBUG: 키워드 필수 포함 여부(is_keyword_required): {is_keyword_required}")
        # print(f"DEBUG: 수집된 기사 제목 예시 (상위 5건):")
        # print(df_new['title'].head().tolist())
        
        # STEP 3: 사전 필터링        
        logger.start_step("사전 필터링 (제목 패턴, 스니펫)", step_number=3)
        df_step3 = nf.apply_pre_filter(df_new)
        logger.add_metric("removed", len(df_new) - len(df_step3))
        logger.end_step(result_count=len(df_step3))

        if df_step3.empty:
            print(f">>> [{keyword}] 사전 필터링 결과 0건. 파이프라인 종료.")
            pipeline_stats["status"] = "success"
            logger.save()
            return pipeline_stats
        
        # STEP 4: 본문 스크래핑
        #logger.start_step("본문 스크래핑", step_number=4)
        df_step4 = ns.fetch_contents(df_step3)
        #logger.add_metric("scrape_success_rate", f"{(len(df_step4) / len(df_step3) * 100):.1f}%")
        #logger.end_step(result_count=len(df_step4))
        
        # STEP 5: 사후 필터링
        logger.start_step("사후 필터링 (본문 길이, 발화체)", step_number=5)
        df_step5 = nf.apply_post_filter(df_step4)
        logger.add_metric("removed", len(df_step4) - len(df_step5))
        logger.end_step(result_count=len(df_step5))
        
        # STEP 6: 클러스터링
        logger.start_step("클러스터링 및 대표 기사 선정", step_number=6)
        df_clustered, stats = cluster_tool.process(df_step5, keyword)
        logger.add_metric("clusters_formed", len(df_clustered["cluster_id"].unique()))
        logger.add_metric("canonical_articles", len(df_clustered[df_clustered["is_canonical"] == True]))
        logger.end_step(result_count=len(df_clustered))
   
        # STEP 7: 최종 병합
        logger.start_step("최종 선정 기사 저장", step_number=7)
        if not df_clustered.empty:
            df_final = df_clustered[df_clustered["is_canonical"] == True].copy()
            added_cnt = repo.merge_final_incremental(df_final)
            pipeline_stats["final_added"] = added_cnt
            logger.end_step(result_count=added_cnt)
            print(f"[최종 완료] {keyword}: 신규 {added_cnt}건 추가됨.")
        else:
            logger.end_step(result_count=0)
        
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
