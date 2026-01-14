# news_cluster.py

import pandas as pd
import hashlib
from canonical_news_policy import CanonicalNewsPolicy
from article_similarity_grouper import ArticleSimilarityGrouper

class NewsCluster:
    """
    뉴스 도메인 처리 객체
    - 기존 아카이브 이후 기사 필터링
    - 제목 유사도 기반 그룹 생성
    - 그룹별 canonical 기사 선정
    """

    def __init__(self, similarity_threshold):
        
        # 이제 archive_repository는 필요 없습니다. (Pipeline에서 썼으므로)
        # 유사도 그룹 생성 임계값
        self.similarity_threshold = similarity_threshold

    def process(self, df: pd.DataFrame):

        if df.empty:
            return df, {"fetched": 0, "similar_groups": 0, "canonical_count": 0}

        # main.py에서 통계를 위해 fetched_count 추가
        fetched_count = len(df)

        # 제목 유사도 기준 그룹 생성
        df = self._build_similar_groups(df)
        similar_groups = (
            df["similar_group_id"].nunique() if not df.empty else 0
        )

        # 그룹별 canonical 기사 선정
        df = self._mark_similar_articles(df)
        canonical_count = (
            int(df["is_canonical"].sum()) if not df.empty else 0
        )

        # 유사도 처리로 대체(사실상 제거)된 기사 수
        total_after_grouping = len(df)
        removed_by_similarity = total_after_grouping - canonical_count

        print(
            f"[similarity] total={total_after_grouping}, "
            f"canonical={canonical_count}, "
            f"removed={removed_by_similarity}"
        )

        stats = {
            "fetched": fetched_count, # main.py에서 호출하므로 추가
            "similar_groups": similar_groups,
            "canonical_count": canonical_count,
        }
        
        return df, stats

    # def _filter_after_last_archived(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     기존 archive의 마지막 발행 시각 이후 기사만 남긴다
    #     """
    #     if df.empty:
    #         return df

    #     last_pubdate = self.archive_repository.get_last_pubdate()
    #     if not last_pubdate:
    #         return df

    #     df = df.copy()
    #     df["pubDate_dt"] = pd.to_datetime(df["pubDate"], utc=True)
    #     df = df[df["pubDate_dt"] > last_pubdate]
    #     df = df.drop(columns=["pubDate_dt"])

    #     return df

    def _build_similar_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        기사 제목 유사도를 기준으로 그룹을 생성하고 저장용 식별자(similar_group_id)를 부여한다
        본문 유사도까지 판단하는 방식으로 업그레이드 해야 한다
        """
        if df.empty:
            return df

        df = df.copy()
        titles = df["title"].fillna("").tolist()

        grouper = ArticleSimilarityGrouper(threshold=self.similarity_threshold)
        group_indexes = grouper.group(titles)

        # 디버그용 인덱스 → 저장용 해시 ID 변환
        df["similar_group_index"] = group_indexes
        df["similar_group_id"] = df["similar_group_index"].apply(
            lambda x: hashlib.md5(str(x).encode()).hexdigest()[:8]
        )

        return df

    def _mark_similar_articles(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        유사 그룹별로 canonical 기사 1개를 선택하고
        나머지 기사와의 대체 관계를 기록한다
        """
        if df.empty:
            return df

        if "similar_group_id" not in df.columns:
            raise RuntimeError(
                "similar_group_id가 없습니다. build_similar_groups가 선행돼야 합니다."
            )

        df = df.copy()

        # canonical 기사 선정 규칙
        selector = CanonicalNewsPolicy()

        df["is_canonical"] = False
        df["replaced_by"] = None

        for group_id, group_df in df.groupby("similar_group_id", sort=False):
            rep = selector.select(group_df)
            rep_news_id = rep["news_id"]

            # canonical 기사 표시
            df.loc[
                df["news_id"] == rep_news_id,
                ["is_canonical", "replaced_by"],
            ] = [True, rep_news_id]

            # 나머지 기사는 canonical 기사로 대체 관계 설정
            df.loc[
                (df["similar_group_id"] == group_id)
                & (df["news_id"] != rep_news_id),
                "replaced_by",
            ] = rep_news_id

        return df
