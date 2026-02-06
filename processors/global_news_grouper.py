# processors/global_news_grouper.py

import pandas as pd
from processors.article_similarity_grouper import ArticleSimilarityGrouper


class GlobalNewsGrouper:
    """
    전역 뉴스 그룹핑 전용 클래스

    목적
    - 서로 다른 키워드에서 수집된 canonical 기사들 중
      실제로 동일 사건인 것들을 묶는다.
    - 판단은 title AND content 유사도만 사용한다.
    - chaining은 구조적으로 발생하지 않는다.
    - canonical을 재선정하지 않는다.

    결과
    - global_group_id 컬럼만 부여한다.
    """

    def __init__(self, title_threshold: float, content_threshold: float):
        self.title_threshold = title_threshold
        self.content_threshold = content_threshold

    def group(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        # 필수 컬럼 확인
        if "title" not in df.columns:
            raise ValueError("GlobalNewsGrouper requires 'title' column")

        content_col = "content" if "content" in df.columns else "content"
        if content_col not in df.columns:
            raise ValueError("GlobalNewsGrouper requires 'content' or 'content' column")

        titles = df["title"].fillna("").tolist()
        bodies = df[content_col].fillna("").tolist()

        # 1. title / content 각각 그룹핑
        title_grouper = ArticleSimilarityGrouper(self.title_threshold, field_name="TITLE")
        content_grouper  = ArticleSimilarityGrouper(self.content_threshold, field_name="CONTENT")

        title_groups = title_grouper.group(titles)
        content_groups = content_grouper.group(bodies)

        # 2. (title_group, content_group) AND 조합으로 전역 그룹 생성
        pair_to_group_id = {}
        global_group_ids = []

        current_group = 0
        for t_gid, b_gid in zip(title_groups, content_groups):
            key = (t_gid, b_gid)  # title 그룹이 같고 content 그룹도 같을 때만

            if key not in pair_to_group_id:
                pair_to_group_id[key] = f"G-{current_group}"
                current_group += 1
            global_group_ids.append(pair_to_group_id[key])

        df["global_group_id"] = global_group_ids

        return df
