# archive_repository.py

import os
import pandas as pd
#from common.config import ARCHIVE_LOOKBACK_LIMIT

ARCHIVE_LOOKBACK_LIMIT = 2000

class ArchiveRepository:
    """
    기사 아카이브 저장소 전용 객체
    - 중복 기준 보유
    - 병합 규칙 보유
    - 정렬 규칙 보유
    - Pipeline은 내부 구현을 모른다
    """

    def __init__(self, archive_path: str):
        self.archive_path = archive_path

    def load_recent(self, limit: int = ARCHIVE_LOOKBACK_LIMIT) -> pd.DataFrame:
        if not os.path.exists(self.archive_path):
            return pd.DataFrame()

        return pd.read_csv(self.archive_path).head(limit)

    def merge_incremental(self, new_df: pd.DataFrame) -> int:
        """
        신규 기사 병합
        반환값: 실제로 새로 추가된 기사 수
        """
        if new_df.empty:
            return 0

        if not os.path.exists(self.archive_path):
            self._save(new_df)
            return len(new_df)

        recent_df = self.load_recent()
        incremental = new_df[~new_df["link"].isin(recent_df["link"])]

        if incremental.empty:
            return 0

        full_archive = pd.read_csv(self.archive_path)
        updated = pd.concat([incremental, full_archive], ignore_index=True)

        updated = self._sort(updated)
        self._save(updated)

        return len(incremental)

    def _sort(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["pubDate_dt"] = pd.to_datetime(df["pubDate"], errors="coerce", utc=True)
        df = df.sort_values(by="pubDate_dt", ascending=False)
        return df.drop(columns=["pubDate_dt"])

    def _save(self, df: pd.DataFrame):
        os.makedirs(os.path.dirname(self.archive_path), exist_ok=True)
        df.to_csv(self.archive_path, index=False)

    def get_last_pubdate(self):
        """
        archive에 저장된 기사 중 가장 최신 pubDate를 datetime으로 반환
        없으면 None
        """
        if not os.path.exists(self.archive_path):
            return None

        try:
            df = pd.read_csv(self.archive_path)
        except Exception:
            return None

        if df.empty or "pubDate" not in df.columns:
            return None

        # pubDate 파싱 (RFC 2822 대응)        
        pub_dates = pd.to_datetime(df["pubDate"], errors="coerce", utc=True)

        pub_dates = pub_dates.dropna()
        if pub_dates.empty:
            return None

        return pub_dates.max()        
