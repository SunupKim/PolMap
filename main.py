# main.py

# 우리가 만든 전문 부품들 임포트
from api.naver_news_client import NaverNewsClient
from api.news_repository import NewsRepository
from processors.news_filter import NewsFilter
from processors.news_scraper import NewsScraper
from processors.news_cluster import NewsCluster
from config import TITLE_THRESHOLD, CONTENT_THRESHOLD, NAVER_ID, NAVER_SECRET

def run_news_pipeline(keyword, total_count):
    print(f"\n{'='*20} [{keyword}] 파이프라인 가동 {'='*20}")

    # 1. 초기화 (각 객체에게 필요한 설정 주입)
    client = NaverNewsClient(NAVER_ID, NAVER_SECRET)

    repo = NewsRepository(keyword) # 키워드별 독립 저장소
    nf = NewsFilter(keyword)       # 키워드별 필터 로그 관리
    ns = NewsScraper(delay=0.1)    # 크롤러    
    cluster_tool = NewsCluster(title_threshold=TITLE_THRESHOLD, content_threshold=CONTENT_THRESHOLD)

    # --- STEP 1 & 2: 수집 및 원본 아카이빙 (증분 체크) ---
    
    # NaverNewsClient는 내부에서 NewsArticleModel을 통해 news_id가 생성되어 반환됨
    
    raw_items = client.fetch_news_batch(keyword, total_count=total_count)
    df_new = repo.save_raw_and_get_new(raw_items)
    
    if df_new.empty:
        print(f">>> [{keyword}] 새로운 기사가 없습니다. 공정을 종료합니다.")
        return

    print(f"> 신규 기사 {len(df_new)}건 발견. 정밀 공정 시작...")

    # --- STEP 3: 사전 필터링 (제목/Snippet 기반) ---    
    df_step3 = nf.apply_pre_filter(df_new)

    # --- STEP 4: 본문 크롤링 ---    
    df_step4 = ns.fetch_contents(df_step3)

    # --- STEP 5: 사후 필터링 (본문 품질/말투 기반) ---    
    df_step5 = nf.apply_post_filter(df_step4)

    # STEP 6: 지능형 클러스터링 (유사 기사 그룹화)    
    df_clustered, stats = cluster_tool.process(df_step5, keyword)    

    # --- STEP 7: 최종 분석용 저장소에 병합 ---
    # 클러스터링 결과 중 '대표 기사'만 골라서 selected_archive.csv에 누적
    if not df_clustered.empty:
        df_final = df_clustered[df_clustered["is_canonical"] == True].copy()
        added_cnt = repo.merge_final_incremental(df_final)
        print(f"[최종 완료] {keyword}: 신규 {added_cnt}건이 분석 저장소에 추가됨.")
    else:
        print(f"[최종 완료] {keyword}: 모든 필터링 결과 남은 기사가 없습니다.")

if __name__ == "__main__":
    # 분석 대상 키워드 리스트
    # target_keywords = ["이재명", "한동훈", "더불어민주당", "국민의힘", "개혁신당"]    
    # target_keywords = ["한동훈"]
    
    # target_keywords = ["우원식", "이재명", "한동훈"]
    target_keywords = ["개혁신당", "김동연", "조국신당"]

    total_count = 30  # 키워드 1개당 몇 개 뉴스까지 받을지 설정
    
    for kw in target_keywords:
        try:
            run_news_pipeline(kw, total_count)
        except Exception as e:
            print(f"!!! [{kw}] 파이프라인 실행 중 오류 발생: {e}")

    print("\n" + "="*50)
    print("모든 키워드에 대한 수집 및 정제 공정이 완료되었습니다.")
    print("="*50)