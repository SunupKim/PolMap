# main.py

from news_cluster import NewsCluster
from archive_repository import ArchiveRepository
from pipeline import PoliticalNewsPipeline

# 상수는 프로젝트 구조에 맞게 설정
ARCHIVE_PATH = "outputs/archive/news_archive.csv"
SIMILARITY_THRESHOLD = 0.6 # 제목 유사도 기준 (0.6~0.8 권장)

# config/__init__.py 역할을 대신하여 환경변수 로드
# load_dotenv()

if __name__ == "__main__":
    # 프로젝트 초기화
    pipeline = PoliticalNewsPipeline()
    archive_repo = ArchiveRepository(ARCHIVE_PATH)
    cluster = NewsCluster(SIMILARITY_THRESHOLD)
    
    loop_count = 1
    #keywords = ["이재명", "한동훈", "더불어민주당", "국민의힘", "개혁신당"]    
    #keywords = ["한동훈"]
    keywords = ["우원식"]

    # 1. Raw 데이터 확보 및 물리적 필터링 (개별 기사 단위)
    df_step1 = pipeline.step1_collect_news(keywords, loop_count)
    df_step2 = pipeline.step2_filter_and_id(df_step1, archive_repo)
    df_step3 = pipeline.step3_rbp_filtering(df_step2)
    df_step4 = pipeline.step4_extract_content(df_step3)
    df_step5 = pipeline.step5_final_filtering(df_step4)
    
    # 2. 지능형 중복 제거 (그룹 단위)
    # 이제 이 데이터는 "이미 아카이브에 없고, 본문도 멀쩡한" 깨끗한 기사들입니다.
    print("\n> [STEP 6] 유사도 그룹화 및 아카이브 처리 시작...")
    df_processed, stats = cluster.process(df_step5)

    # --- 최종 결과 저장 ---
    if not df_processed.empty and "is_canonical" in df_processed.columns:
        # is_canonical인 기사들만 골라서 아카이브에 병합
        df_final_to_save = df_processed[df_processed["is_canonical"] == True].copy()
        added_count = archive_repo.merge_incremental(df_final_to_save)
    else:
        added_count = 0
        print("> [공지] 처리할 신규 기사가 없어 저장을 스킵합니다.")
    
    # --- 결과 출력 ---
    print("-" * 30)
    print(f"최종 처리 완료 요약:")
    print(f"- 수집된 원본: {stats.get('fetched', 0)}건")
    print(f"- 유사 그룹 수: {stats.get('similar_groups', 0)}개")
    print(f"- 최종 아카이브 추가: {added_count}건")
    print("-" * 30)