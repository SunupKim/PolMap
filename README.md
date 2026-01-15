💡 한눈에 보는 요약 표

상황,               title_group_id          content_group_id                의미
나홀로 기사,            T-10 (중복 없음)            Unique              해당 사건에 대해 유일하게 보도된 기사
제목만 비슷한 기사들    T-1 (3개 존재)      "Unique, Unique, Unique"    사건은 같으나, 각 언론사가 직접 다르게 작성함
본문까지 베낀 기사들    T-2 (2개 존재)      "C-1, C-1"                  제목도 비슷하고 본문은 복사 붙여넣기 수준임

💡 단계,세부 기능,OOP 적용 위치 (클래스/메서드),비고
Step 1,Naver API 뉴스 수집,NaverNewsClient.fetch_news_batch,검색어 기준 API 호출
Step 2,1) 아카이브 증분 체크2) Naver 뉴스 링크 필터3) 현재 수집 내 중복 제거,NewsRepository.save_raw_and_get_new,link 기준 신규 데이터만 반환
Step 3,1) 제목 패턴 ([포토] 등) 제외2) Snippet 길이 체크3) 언론사별 동일 제목 제거,NewsFilter.apply_pre_filter,본문 수집 전 가벼운 필터링
Step 4,본문 크롤링 (Scraping),NewsScraper.fetch_contents,BeautifulSoup 활용
Step 5,1) 빈 본문 제거2) 본문 길이 체크3) 말투(Speech) 필터링,NewsFilter.apply_post_filter,본문 품질 기준 필터링
Step 6,제목 유사도 그룹핑 (T-번호),NewsCluster._build_title_groups,title_group_id 부여
Step 7,본문 유사도 정밀 체크 (C-번호),NewsCluster._refine_by_body_similarity,content_group_id 부여
Step 8,대표 기사 선정 및 치환,NewsCluster._mark_canonical_articles,"is_canonical, replaced_by"
Step 9,결과물 병합 및 로그 저장,NewsRepository.merge_final_incrementalNewsCluster._save_similarity_debug_log,컬럼 순서 재배치 및 저장
Step 10,[추가] 중복 그룹 전용 검토,NewsCluster._save_grouped_only_log,2개 이상 묶인 그룹만 추출