# main.py

from api.naver_news_client import NaverNewsClient
from api.news_repository import NewsRepository
from processors.news_filter import NewsFilter
from processors.news_scraper import NewsScraper
from processors.news_cluster import NewsCluster
from config import TITLE_THRESHOLD, CONTENT_THRESHOLD, NAVER_ID, NAVER_SECRET, EXCLUDE_WORDS_STR, SEARCH_KEYWORDS, TOTAL_FETCH_COUNT

def run_news_pipeline(keyword: str, total_count: int, is_keyword_required: bool):
    print(f"\n{'='*20} [{keyword}] 파이프라인 가동 {'='*20}")
    
    # 변수명 유지: 사용자님의 의도대로 초기화
    pipeline_stats = {
        "keyword": keyword,
        "new_raw": 0,
        "final_added": 0,
        "status": "initialized" # 성공 확정 전까지는 success를 주지 않음
    }

    try:
        # 1. 초기화 (기존 변수/객체명 유지)
        client = NaverNewsClient(NAVER_ID, NAVER_SECRET)
        repo = NewsRepository(keyword)
        nf = NewsFilter(keyword, is_keyword_required=is_keyword_required, exclude_words_str=EXCLUDE_WORDS_STR)
        ns = NewsScraper(delay=0.1)
        cluster_tool = NewsCluster(title_threshold=TITLE_THRESHOLD, content_threshold=CONTENT_THRESHOLD)

        # STEP 1 & 2 (기존 로직 유지)
        raw_items = client.fetch_news_batch(keyword, total_count=total_count)
        df_new = repo.save_raw_and_get_new(raw_items)
        
        if df_new.empty:
            print(f">>> [{keyword}] 새로운 기사가 없습니다.")
            pipeline_stats["status"] = "success" # 데이터가 없는 것도 정상 종료이므로 success
            return pipeline_stats

        pipeline_stats["new_raw"] = len(df_new)
        
        # STEP 3 ~ 6 실행...
        df_step3 = nf.apply_pre_filter(df_new)
        df_step4 = ns.fetch_contents(df_step3)
        df_step5 = nf.apply_post_filter(df_step4)
        df_clustered, stats = cluster_tool.process(df_step5, keyword)
   
        # STEP 7: 최종 병합
        if not df_clustered.empty:
            df_final = df_clustered[df_clustered["is_canonical"] == True].copy()
            added_cnt = repo.merge_final_incremental(df_final)
            pipeline_stats["final_added"] = added_cnt
            print(f"[최종 완료] {keyword}: 신규 {added_cnt}건 추가됨.")
        
        # 여기까지 무사히 와야 성공입니다.
        pipeline_stats["status"] = "success" 

    except Exception as e:
        # 실패 시 status에 에러 원인을 명확히 기록
        pipeline_stats["status"] = f"fail: {str(e)}"
        print(f"!!! [{keyword}] 파이프라인 실행 중 오류 발생: {e}")
    
    return pipeline_stats
    
if __name__ == "__main__":
    
    for kw, is_required in SEARCH_KEYWORDS:
        try:
            # 파이프라인 함수에 is_required 옵션을 함께 전달
            run_news_pipeline(kw, TOTAL_FETCH_COUNT, is_required)
        except Exception as e:
            print(f"!!! [{kw}] 파이프라인 실행 중 오류 발생: {e}")