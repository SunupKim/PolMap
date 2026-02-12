# news_repository.py

import os
import pandas as pd

class NewsRepository:
    """
    뉴스 데이터 저장소 전용 객체
    - Raw Archive: API에서 가져온 원본 뉴스 누적 (무조건 저장)
    - Final Archive: 필터링 및 본문 수집이 완료된 분석용 뉴스 누적
    - 증분(Incremental) 체크 및 키워드별 경로 관리 담당
    """

    def __init__(self, keyword: str, base_path: str = "archive"):
        self.keyword = keyword
        # 키워드별 전용 디렉토리 설정 (예: archive/우원식/)
        self.dir_path = os.path.join(base_path, keyword)
        os.makedirs(self.dir_path, exist_ok=True)
        
        # 원본 및 최종 저장소 경로
        self.raw_archive_path = os.path.join(self.dir_path, "raw_archive.csv")
        self.selected_archive_path = os.path.join(self.dir_path, "selected_archive.csv")
        self.selected_archive_copy_path = os.path.join(self.dir_path, "selected_archive_copy.csv")

        # 중복 체크 시 비교할 최근 기사 수
        self.lookback_limit = 2000

    # ---------------------------------------------------------
    # 1. Raw Archive 관리 (원본 뉴스 누적)
    # ---------------------------------------------------------
    def save_raw_and_get_new(self, fetched_items: list) -> pd.DataFrame:
        """
        API 수집 직후 호출: 원본 리스트를 저장하고, 기존에 없던 '신규' 데이터만 반환합니다.
        """
        if not fetched_items:
            return pd.DataFrame()

        df_fetched = pd.DataFrame(fetched_items)
        
        # 1. 기존 데이터가 없으면 바로 저장하고 반환 (코드 압축)
        if not os.path.exists(self.raw_archive_path):
            self._finalize_and_save(df_fetched, self.raw_archive_path)
            return df_fetched

        # 2. 기존 데이터가 있는 경우: 증분 체크 및 병합
        df_old = pd.read_csv(self.raw_archive_path, usecols=["link"]).head(self.lookback_limit)
        df_new_only = df_fetched[~df_fetched["link"].isin(df_old["link"])].copy()

        # 전체 로드 후 병합 및 정렬 (가장 안전한 방식)
        if not df_new_only.empty:
            full_raw = pd.read_csv(self.raw_archive_path)
            updated_raw = pd.concat([df_new_only, full_raw], ignore_index=True)
            self._finalize_and_save(updated_raw, self.raw_archive_path)
                        
        return df_new_only

    # ---------------------------------------------------------
    # 2. Canonical Archive 관리 (분석용 뉴스 누적)
    # ---------------------------------------------------------
    def merge_final_incremental(self, df_final: pd.DataFrame) -> int:
        if df_final.empty: return 0

        # 기존 데이터 로드 및 증분 필터링
        if os.path.exists(self.selected_archive_path):
            df_old = pd.read_csv(self.selected_archive_path)
            incremental = df_final[~df_final["link"].isin(df_old.head(self.lookback_limit)["link"])].copy()
            if incremental.empty: return 0
            df_total = pd.concat([incremental, df_old], ignore_index=True)
        else:
            df_total = df_final
            incremental = df_final

        # 공통 저장 로직 호출
        self._finalize_and_save(df_total, self.selected_archive_path, reorder=True)

        # ✅ selected_archive_copy.csv 저장
        self._save_copy_selected(df_total)

        return len(incremental)

    # ---------------------------------------------------------
    # 3. 유틸리티 메서드
    # ---------------------------------------------------------
    def get_last_pubdate(self, target='raw'):
        """저장된 기사 중 가장 최신 날짜 반환 (raw 또는 final 선택 가능)"""
        path = self.raw_archive_path if target == 'raw' else self.selected_archive_path
        
        if not os.path.exists(path):
            return None

        try:
            df = pd.read_csv(path, usecols=["pubDate"])
            if df.empty: return None
            
            pub_dates = pd.to_datetime(df["pubDate"], errors="coerce")
            return pub_dates.max()
        except Exception:
            return None

    # 3. 유틸리티 메서드 섹션에 추가
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """분석 편의를 위해 컬럼 순서 재배치"""
        # 디버그 로그와 동일한 핵심 컬럼을 앞으로 배치
        desired_order = [
            "news_id", "pubDate", "collected_at", "title", "link", "originallink", "description", "content"
        ]
        # 실제 존재하는 컬럼만 골라내기 (KeyError 방지)
        existing_cols = [col for col in desired_order if col in df.columns]
        # 리스트에 없는 나머지 컬럼들도 뒤에 붙여주기
        remaining_cols = [col for col in df.columns if col not in existing_cols]
         
        return df[existing_cols + remaining_cols]                    
    
    def _finalize_and_save(self, df: pd.DataFrame, path: str, reorder: bool = False):
        """정렬, (선택적) 순서 재배치 후 CSV 저장"""
        df = self._sort(df)
        if reorder:
            df = self._reorder_columns(df)

        #df.to_csv(path, index=False, encoding='utf-8-sig')를 아래로 대체        
        from utils.dataframe_utils import raw_df_save        
        raw_df_save(df, path)        

    def _save_copy_selected(self, df: pd.DataFrame):
        """
        사람이 빠르게 훑기 위한 요약본
        """
        copy_cols = ["news_id", "pubDate", "title", "description"]
        existing = [c for c in copy_cols if c in df.columns]

        if not existing:
            return

        # 저장 전 정렬 수행
        copy_df = self._sort(df[existing].copy())

        copy_df.to_csv(
            self.selected_archive_copy_path,
            index=False,
            encoding="utf-8-sig"
        )


    def _sort(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        temp_dt = pd.to_datetime(df["pubDate"], errors="coerce")
        df["_sort_pubDate"] = temp_dt
        df = df.sort_values("_sort_pubDate", ascending=False, na_position="last")
        return df.drop(columns=["_sort_pubDate"])
    