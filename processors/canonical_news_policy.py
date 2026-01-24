# canonical_news_policy.py
# 자체 중복기사를 셀렉팅하는 정책이다.
# 피곤하다
      
from datetime import datetime
import pandas as pd

class CanonicalNewsPolicy:
    AGENCY_DOMAINS = ("newsis.com", "yna.co.kr", "news1.kr")

    def __init__(self):
        pass

    def select(self, group_df):
        """
        group_df: 동일한 similar_group_id를 가진 DataFrame
        반환: 대표 기사 row (Series)
        """

        def is_agency(originallink):
            if not originallink:
                return False
            return any(domain in originallink for domain in self.AGENCY_DOMAINS)

        df = group_df.copy()
        df["is_agency"] = df["originallink"].apply(is_agency)
        df["pubDate_dt"] = pd.to_datetime(
            df["pubDate"],
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce"
            ) 

        agency_df = df[df["is_agency"]]

        # 1) 통신사 기사 1개
        if len(agency_df) == 1:
            return agency_df.iloc[0]

        # 2) 통신사 기사 2개 이상 → 가장 오래된 통신사 기사
        if len(agency_df) >= 2:
            return agency_df.sort_values("pubDate_dt", ascending=True).iloc[0]

        # 3) 통신사 기사 0개 → 전체 중 가장 오래된 기사
        return df.sort_values("pubDate_dt", ascending=True).iloc[0]  
