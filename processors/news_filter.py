# news_filter.py

import os, re
import pandas as pd
from datetime import datetime

class NewsFilter:
    """
    뉴스 필터링 전문 객체
    - Step 3 (수집 직후 필터링)와 Step 5 (본문 수집 후 필터링) 로직 담당
    - 필터링되어 탈락한 기사들을 별도 로그 파일로 저장
    """
    
    def __init__(
        self, 
        keyword: str, 
        is_keyword_required: bool = False, 
        exclude_words_str: str = None, 
        base_path: str = "logs"
        ):

        self.keyword = keyword
        self.is_keyword_required = is_keyword_required # 타입 주입
        self.log_path = os.path.join(base_path, keyword, "filtered_logs")
        os.makedirs(self.log_path, exist_ok=True)

        # 1. 외부에서 주입된 콤마 구분 문자열을 정규표현식 패턴으로 변환
        if exclude_words_str:
            # 콤마로 분리 -> 앞뒤 공백 제거 -> [포토] 등을 안전하게 처리(re.escape) -> |(OR)로 연결
            words = [word.strip() for word in exclude_words_str.split(",") if word.strip()]
            self.exclude_pattern = "|".join([re.escape(word) for word in words])
        else:
            # 기본값 설정
            self.exclude_pattern = ""     

    def _save_log(self, df: pd.DataFrame, filename: str):
        """탈락한 기사들을 추적하기 위해 저장 (파일명에 날짜시간 추가)"""
        if not df.empty:
            # 1. 현재 날짜와 시간 가져오기 (예: 20260115_1459)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            
            # 2. 파일명과 확장자 분리 후 타임스탬프 삽입
            # 예: step5_empty_content.csv -> step5_empty_content_20260115_1459.csv
            name, ext = os.path.splitext(filename)
            new_filename = f"{name}_{timestamp}{ext}"
            
            path = os.path.join(self.log_path, new_filename)
            
            # 3. 파일 저장 (매번 새로운 시간에 생성되므로 사실상 'w' 모드가 되지만, 안정성을 위해 유지)
            mode = 'a' if os.path.exists(path) else 'w'
            header = not os.path.exists(path)
            
            df.to_csv(path, index=False, mode=mode, header=header, encoding='utf-8-sig')

    def apply_pre_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        STEP 3. 제목 및 Snippet 기반 1차 필터링
        - 네이버 뉴스 여부, 제목 패턴, 짧은 요약문, 중복 제목 제거
        """
        if df.empty: return df
        before_cnt = len(df)

        # 1. 네이버 뉴스(news.naver.com)가 아닌 기사 분리
        naver_mask = df['link'].str.contains("news.naver.com", na=False)
        self._save_log(df[~naver_mask], "step3_non_naver_links.csv")
        df = df[naver_mask].copy()
        
        # 2. 제목 자체에도 검색 키워드가 포함되어 있는지 확인
        if self.is_keyword_required:            
            keyword_mask = df['title'].str.contains(self.keyword, case=False, na=False)
            self._save_log(df[~keyword_mask], "step3_missing_keyword_in_title.csv")
            df = df[keyword_mask].copy()

        # 3. 제목 패턴 필터링 ([포토], [사진] 등)
        if self.exclude_pattern:
            pattern_mask = df['title'].str.contains(self.exclude_pattern, case=False, na=False, regex=True)
            self._save_log(df[pattern_mask], "step3_exclude_pattern.csv")
            df = df[~pattern_mask].copy()
        
        # 4. Snippet(description) 길이 필터링 (30자 미만)
        short_mask = df['description'].str.len() < 20
        self._save_log(df[short_mask], "step3_short_snippet.csv")
        df = df[~short_mask].copy()

        # 4. 동일 언론사 내 동일 제목 반복 기사 제거
        # URL에서 도메인 추출하여 임시 언론사 구분선 생성
        df['temp_press'] = df['originallink'].str.split('/').str[2]
        dup_mask = df.duplicated(subset=['temp_press', 'title'], keep='first')
        self._save_log(df[dup_mask], "step3_duplicate_title.csv")
        df = df[~dup_mask].drop(columns=['temp_press']).copy()

        print(f"[Filter] Pre-filtering 완료: {before_cnt}건 -> {len(df)}건 ----------> (삭제: {before_cnt - len(df)}건)")
        #print(f"[Filter] Pre-filtering 완료: {before_cnt}건 -> {len(df)}건")
        return df

    def apply_post_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        STEP 5. 본문 내용 기반 2차 필터링
        - 빈 본문, 본문 길이 미달, 특정 말투(구어체) 제거
        """
        if df.empty: return df
        before_cnt = len(df)

        # 1. 본문이 비어있거나 공백인 것 제거
        empty_mask = df['content'].isna() | (df['content'].str.strip() == "")
        self._save_log(df[empty_mask], "step5_empty_content.csv")
        df = df[~empty_mask].copy()

        # 2. 본문 길이 기준 미달 제거 (200자 이상 5000자 미만만 살린다)
        short_mask = df['content'].str.len() < 200
        long_mask = df['content'].str.len() > 5000
        
        # 로그 기록 (어떤 기사가 너무 짧아서 탈락했는지 추적)
        if not df[short_mask].empty:            
            self._save_log(df[short_mask], "step5_too_short_content.csv")
            print(f"[Filter] step5 너무 짧은 기사(200자 미만) {len(df[short_mask])}건 발견 및 제외")
        
        # 로그 기록 (어떤 기사가 너무 길어서 탈락했는지 추적)
        if not df[long_mask].empty:
            self._save_log(df[long_mask], "step5_too_long_content.csv")
            print(f"[Filter] step5 너무 긴 기사(5000자 초과) {len(df[long_mask])}건 발견 및 제외")
        
        df = df[(~short_mask) & (~long_mask)].copy()

        # # 3. 특정 말투 제외 ("입니다", "습니다" 포함 구어체/안내문 성격 기사)
        # speech_pattern = r'입니다|습니다'
        # speech_mask = df['content'].str.contains(speech_pattern, na=False)
        # self._save_log(df[speech_mask], "step5_speech_style.csv")
        # df = df[~speech_mask].copy()

        print(f"[Filter] Post-filtering 완료: {before_cnt}건 -> {len(df)}건 ---------> (삭제: {before_cnt - len(df)}건)")
        #print(f"[Filter] Post-filtering 완료: {before_cnt}건 -> {len(df)}건")
        return df