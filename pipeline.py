# pipeline.py

import os
import hashlib
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from tqdm import tqdm  # 진행률 표시를 위해 추가 (pip install tqdm 필요)
import time
# from news_processing_step import NewsProcessingStep
# from archive_repository import ArchiveRepository

# 상수는 프로젝트 구조에 맞게 설정
ARCHIVE_PATH = "outputs/archive/news_archive.csv"
SIMILARITY_THRESHOLD = 0.6 # 제목 유사도 기준 (0.6~0.8 권장)

# config/__init__.py 역할을 대신하여 환경변수 로드
load_dotenv()

class PoliticalNewsPipeline:
    def __init__(self):
        self.client_id = os.getenv("NAVER_ID")
        self.client_secret = os.getenv("NAVER_SECRET")
        self.headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret
        }
        
        # 메인 출력 경로
        self.output_path = "outputs"
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
            
        # 필터링 로그 경로 (클래스 속성으로 정의)
        self.filter_log_path = os.path.join(self.output_path, "filtered_logs")
        if not os.path.exists(self.filter_log_path):
            os.makedirs(self.filter_log_path)

    def _log_status(self, step_name, before_cnt, after_cnt):
        removed = before_cnt - after_cnt
        print(f"[{step_name}] 수집 전: {before_cnt}건 -> 수집 후: {after_cnt}건 (삭제: {removed}건)")

    def step1_collect_news(self, keywords, loop_count):
        """STEP 1. 뉴스 URL 수집 (Naver API - 검색어당 1,000건)"""

        all_news = []
        collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #display_count = 100
        display_count = 50
        
        for keyword in keywords:
            print(f"> '{keyword}' 검색어 수집 시작...")
            for i in range(loop_count):  # 100건씩 10번 반복 (총 1,000건)
                start_index = (i * display_count) + 1
                url = f"https://openapi.naver.com/v1/search/news.json?query={keyword}&display={display_count}&start={start_index}&sort=date"
                
                try:
                    response = requests.get(url, headers=self.headers, timeout=10)
                    if response.status_code == 200:
                        items = response.json().get('items', [])
                        if not items: # 더 이상 결과가 없으면 중단
                            break

                        for item in items:
                            # API 원본 날짜를 미리 ISO 표준 포맷 문자열로 변환
                            try:
                                std_date = pd.to_datetime(item['pubDate'], utc=True).strftime("%Y-%m-%d %H:%M:%S")
                            except:
                                std_date = item['pubDate'] # 예외 시 원본 유지

                            all_news.append({
                                "pubDate": std_date, # 이제 모든 스텝에서 이 포맷을 사용함                                
                                "title": item['title'],                                
                                "link": item['link'],
                                "originallink": item['originallink'],
                                "press": "", # press 정보는 API 결과에서 직접 제공되지 않으므로 비워둠(또는 snippet에서 추출)                                
                                "collected_at": collected_at,
                                "search_keyword": keyword,
                                "description": item['description'],
                            })
                    else:
                        print(f"  - Error: {keyword} {start_index}번대 수집 실패 (Status: {response.status_code})")
                        break
                except Exception as e:
                    print(f"  - Exception 발생: {e}")
                    break

        df_step1 = pd.DataFrame(all_news)
        df_step1.to_csv(f"{self.output_path}/step1_news_package.csv", index=False, encoding='utf-8-sig')
        self._log_status("STEP 1", len(all_news), len(all_news))
        return df_step1

    
    def step2_filter_and_id(self, df_step1, archive_repo):
        """
        STEP 2. 아카이브 중복 제거, 네이버 뉴스 필터링 및 news_id 생성
        """
        before_cnt = len(df_step1)
        
        # --- 2-0. 아카이브 기반 증분 필터링 (최우선 실행) ---
        # 이미 아카이브에 저장된 마지막 기사 시간보다 이전 기사는 여기서 바로 탈락합니다.
        last_pubdate = archive_repo.get_last_pubdate()
        if last_pubdate and not df_step1.empty:
            df_step1['pubDate_dt'] = pd.to_datetime(df_step1['pubDate'], utc=True)
            df_step1 = df_step1[df_step1['pubDate_dt'] > last_pubdate].copy()
            df_step1 = df_step1.drop(columns=['pubDate_dt'])
            
            # 아카이브 필터링 결과 로그 출력
            after_archive_filter_cnt = len(df_step1)
            print(f"  - [Archive Filter] 기존 아카이브 중복 제거: {before_cnt - after_archive_filter_cnt}건 제외")
        
        if df_step1.empty:
            self._log_status("STEP 2", before_cnt, 0)
            return df_step1

        # --- 2-1. 네이버 뉴스(news.naver.com)가 아닌 기사 분리 ---
        naver_mask = df_step1['link'].str.contains("news.naver.com")
        df_non_naver = df_step1[~naver_mask]
        df_step2_tmp = df_step1[naver_mask].copy()
        
        # --- 2-2. 현재 수집 결과 내 link 기준 중복 데이터 분리 ---
        duplicate_mask = df_step2_tmp.duplicated(subset=['link'], keep='first')
        df_duplicates = df_step2_tmp[duplicate_mask]
        
        # 최종 중복 제거 결과
        df_step2 = df_step2_tmp[~duplicate_mask].copy()
        
        # --- 삭제된 데이터 로그 저장 ---
        df_non_naver.to_csv(f"{self.filter_log_path}/step2_non_naver_links.csv", index=False, encoding='utf-8-sig')
        df_duplicates.to_csv(f"{self.filter_log_path}/step2_link_duplicates.csv", index=False, encoding='utf-8-sig')

        # --- 2-3. news_id (MD5 해시) 생성 ---
        df_step2['news_id'] = df_step2['link'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest()
        )
        
        df_step2.to_csv(f"{self.output_path}/step2_news_package.csv", index=False, encoding='utf-8-sig') 
        self._log_status("STEP 2", before_cnt, len(df_step2))
        return df_step2
        
    def step3_rbp_filtering(self, df_step2):
        """STEP 3. 제목+snippet 기반 1차 필터링 (RBP) 및 필터링 데이터 저장"""
        before_cnt = len(df_step2)
        
        # --- 1. 제목 패턴 필터링 ([포토], [사진] 등) ---
        exclude_patterns = r'\[포토\]|\[사진\]|\[영상\]|\[화보\]|\[그래픽\]|\[속보\]'
        filter1_mask = df_step2['title'].str.contains(exclude_patterns, case=False, na=False)
        df_filtered_pattern = df_step2[filter1_mask] # 제거 대상 1
        df_step3_tmp1 = df_step2[~filter1_mask].copy()
        
        # --- 2. Snippet 길이 필터링 (20자 미만) ---
        filter2_mask = df_step3_tmp1['description'].str.len() < 20
        df_filtered_short = df_step3_tmp1[filter2_mask] # 제거 대상 2
        df_step3_tmp2 = df_step3_tmp1[~filter2_mask].copy()
        
        # --- 3. 동일 언론사 내 동일 제목 반복 기사 제거 ---
        # 임시 언론사 필드 생성
        df_step3_tmp2['temp_press'] = df_step3_tmp2['originallink'].str.split('/').str[2]
        
        # 중복 기사 중 첫 번째만 남기기 (keep='first'가 아닌 것들이 제거 대상)
        filter3_mask = df_step3_tmp2.duplicated(subset=['temp_press', 'title'], keep='first')
        df_filtered_duplicate = df_step3_tmp2[filter3_mask] # 제거 대상 3
        df_step3 = df_step3_tmp2[~filter3_mask].copy()
        
        # 임시 필드 삭제
        df_step3 = df_step3.drop(columns=['temp_press'])
                    
        df_filtered_pattern.to_csv(f"{self.filter_log_path}/step3_exclude_pattern.csv", index=False, encoding='utf-8-sig')
        df_filtered_short.to_csv(f"{self.filter_log_path}/step3_short_snippet.csv", index=False, encoding='utf-8-sig')
        df_filtered_duplicate.to_csv(f"{self.filter_log_path}/step3_duplicate_title.csv", index=False, encoding='utf-8-sig')

        self._log_status("STEP 3", before_cnt, len(df_step3))
        df_step3.to_csv(f"{self.output_path}/step3_news_package.csv", index=False, encoding='utf-8-sig')
        
        return df_step3

    
    def step4_extract_content(self, df_step3):
        """STEP 4. 원문 수집 (상세 모니터링 및 디버깅 추가)"""
        before_cnt = len(df_step3)
        contents = []
        success_cnt = 0
        fail_cnt = 0
        
        print(f"\n> [STEP 4] 총 {before_cnt}건의 기사 본문 수집을 시작합니다.")
        
        # tqdm을 사용하여 프로그레스 바 출력
        for idx, row in tqdm(df_step3.iterrows(), total=before_cnt, desc="Crawling News"):
            url = row['link']
            news_id = row['news_id']
            content_text = ""
            
            try:
                # 개별 기사 접속 전 디버깅 (필요 시 주석 해제)
                # print(f"  - Processing [{news_id}]: {url}") 
                
                response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=7)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 네이버 뉴스 본문 후보 셀렉터
                    targets = [
                        soup.find('div', id='newsct_article'),
                        soup.find('div', id='articleBodyContents'),
                        soup.find('article', id='dic_area') # 최신 네이버 뉴스 구조 추가
                    ]
                    
                    for target in targets:
                        if target:
                            # 노이즈 제거
                            for s in target(['script', 'style', 'iframe', 'span', 'em']):
                                s.decompose()
                            content_text = target.get_text(separator=' ', strip=True)
                            break
                    
                    if content_text:
                        success_cnt += 1
                    else:
                        fail_cnt += 1
                        # print(f"\n[FAIL] 본문 구조 매칭 실패: {url}")
                else:
                    fail_cnt += 1
                    # print(f"\n[HTTP {response.status_code}] 접속 실패: {url}")
                    
            except requests.exceptions.Timeout:
                fail_cnt += 1
                print(f"\n[TIMEOUT] 7초 초과로 스킵: {url}")
            except Exception as e:
                fail_cnt += 1
                print(f"\n[ERROR] {type(e).__name__}: {url}")
            
            contents.append(content_text)
            
            # 네이버 서버 부하 방지를 위한 미세한 지연 (0.1초)
            time.sleep(0.1)

        df_step4 = df_step3.copy()
        df_step4['content'] = contents
        
        # 결과 요약 로그
        print(f"\n> [STEP 4] 완료 정보:")
        print(f"  - 총 시도: {before_cnt}건")
        print(f"  - 성공(텍스트 확보): {success_cnt}건")
        print(f"  - 실패(구조불일치/에러): {fail_cnt}건")
        
        df_step4.to_csv(f"{self.output_path}/step4_news_package.csv", index=False, encoding='utf-8-sig')
        return df_step4

    def step5_final_filtering(self, df_step4):

        # 1. 방어 코드: 데이터가 없으면 즉시 반환
        if df_step4 is None or df_step4.empty:
            self._log_status("STEP 5", 0, 0)
            return pd.DataFrame(columns=df_step4.columns) if df_step4 is not None else pd.DataFrame()
            
        """STEP 5. 기사 원문 기반 2차 필터링 (RBP)"""
        before_cnt = len(df_step4)
        
        # 1. 본문이 비어있지 않은 것만 유지
        df_step5_tmp = df_step4[df_step4['content'].notna() & (df_step4['content'].str.strip() != "")].copy()
        
        # 2. 본문 길이 기준 미달 제거 (200자 미만)
        length_mask = df_step5_tmp['content'].str.strip().str.len() < 200
        df_filtered_short_content = df_step5_tmp[length_mask]
        df_step5_tmp = df_step5_tmp[~length_mask].copy()
        
        # 3. 특정 말투 제외 ("입니다", "습니다" 포함 기사 제거)
        # 보통 뉴스는 "~했다", "~다"로 끝나므로 구어체 기사를 필터링합니다.
        speech_pattern = r'입니다|습니다'
        speech_mask = df_step5_tmp['content'].str.contains(speech_pattern, na=False)
        df_filtered_speech = df_step5_tmp[speech_mask]
        df_step5 = df_step5_tmp[~speech_mask].copy()
        
        # --- 필터링된 데이터 로그 저장 ---
        df_filtered_short_content.to_csv(f"{self.filter_log_path}/step5_short_content.csv", index=False, encoding='utf-8-sig')
        df_filtered_speech.to_csv(f"{self.filter_log_path}/step5_speech_style.csv", index=False, encoding='utf-8-sig')
        
        # 최종 결과 저장
        df_step5.to_csv(f"{self.output_path}/step5_news_package.csv", index=False, encoding='utf-8-sig')
        
        self._log_status("STEP 5", before_cnt, len(df_step5))
        return df_step5
