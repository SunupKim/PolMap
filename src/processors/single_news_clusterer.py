# python -m processors.single_news_clusterer

import pandas as pd
from processors.canonical_news_policy import CanonicalNewsPolicy
from processors.article_similarity_grouper import ArticleSimilarityGrouper
from collections import defaultdict
from datetime import datetime
import os
from utils.text_normalizer import NewsTextNormalizer

class SingleNewsClusterer:
    """
    SINGLE 키워드 단위 뉴스 집합 관리 객체

    책임:
    - 기사 중복/유사 판단
    - 기사 묶음 생성 (내부 grouping)
    - canonical 기사 선정
    - replaced_by 관계 부여

    비책임:
    - 이슈 클러스터링
    - 임베딩 기반 구조 분석
    - 전역 이슈 해석
    """

    def __init__(self, title_threshold: float, content_threshold: float, test_mode = False):
        self.title_threshold = title_threshold
        self.content_threshold = content_threshold
        self.test_mode = test_mode

    def process(self, df: pd.DataFrame, keyword: str):
        if df.empty:
            print("데이터가 비어 있어 프로세스를 중단합니다.")
            return df, {"fetched": 0, "similar_groups": 0, "canonical_count": 0}

        # 1. 제목 기반 그룹핑 (T-번호)
        title_grouper = ArticleSimilarityGrouper(threshold=self.title_threshold, field_name="기사제목", test_mode=self.test_mode)
        normalized_titles = [
            NewsTextNormalizer.normalize_title(t)
            for t in df["title"].fillna("").tolist()
        ]
        title_indices = title_grouper.group(normalized_titles)

        # 결과: 각 기사에 T-번호가 붙음 / 같은 T-번호 = 제목 유사

        # 2. 본문 기반 그룹핑 (B-번호)
        target_col = "content" if "content" in df.columns else "body"
        bodies = df[target_col].fillna("").tolist() if target_col in df.columns else []

        body_grouper = ArticleSimilarityGrouper(threshold=self.content_threshold, field_name="기사본문", test_mode=self.test_mode)
        body_indices = body_grouper.group(bodies)
        # 결과: 각 기사에 B-번호가 붙음 / 같은 B-번호 = 본문 유사

        # 3-1. [핵심] OR 조건 통합 및 직관적 로그 데이터 생성
        df = self._merge_groups_or_condition(df, title_indices, body_indices)

        # 3-2. [핵심] AND 조건 통합 및 직관적 로그 데이터 생성
        #df = self._merge_groups_and_condition(df, title_indices, body_indices)

        # 4. 그룹별 대표 기사(Canonical) 선정
        df = self._mark_canonical_articles(df)

        # 5. 상세 프로세스 로그 저장
        self._save_similarity_debug_log(df, keyword)

        # 6. 통계 계산
        fetched_count = len(df)

        # [해석 주의] cluster_id의 유니크 개수는 곧 대표 기사(is_canon=True)의 개수와 일치함
        similar_groups = df["cluster_id"].nunique() if "cluster_id" in df.columns else 0
        canonical_count = int(df["is_canon"].sum()) if "is_canon" in df.columns else 0

        print(f"[Cluster] 제목 및 내용 유사도 검사 완료 {canonical_count}건 <---------- (삭제: {fetched_count - canonical_count}건)")

        stats = {
            "fetched": fetched_count,
            "similar_groups": similar_groups,  # 클러스터(최종 그룹)의 총 개수
            "canonical_count": canonical_count,  # 최종 생존 기사 수 (similar_groups와 1:1 대응)
        }

        return df, stats

    """
    _merge_groups_or_condition를 쓰는 경우
    제목(T) 혹은 본문(B)이 겹치는 기사들을 하나의 cluster_id(C)로 통합.
    [징검다리 통합(Chaining Effect) 안내]
    수평적 합집합(OR) 로직에 따라, 직접적으로 제목이나 본문이 겹치지 않더라도
    중간에 연결고리 역할을 하는 기사가 있다면 하나의 그룹으로 묶입니다.
    예: 기사A(T1, B1) - 기사B(T1, B2) - 기사C(T2, B2)
        -> 기사A와 기사C는 직접 겹치는 속성이 없으나 기사B를 통해 C-그룹으로 통합됨.
    ※ cluster_id는 유사도 기반 클러스터 경계를 식별하기 위한 내부 식별자이며,
    의미적 대표(canonical)를 나타내지 않는다.
    외부 노출이나 해석을 전제로 한 값이 아니다.

    _merge_groups_and_condition를 쓰는 경우
    제목(T)과 본문(B)이 모두 겹치는 기사들만 하나의 cluster_id(C)로 통합.
    - OR 조건 미사용
    - chaining 효과 없음
    - 기사 형식/연재 템플릿으로 인한 과잉 묶기 방지
    """

    def _merge_groups_or_condition(self, df, title_idx, body_idx):

        n = len(df)
        parent = list(range(n))

        def find(i):
            if parent[i] == i: return i
            parent[i] = find(parent[i])
            return parent[i]

        def union(i, j):
            root_i, root_j = find(i), find(j)
            if root_i != root_j: parent[root_i] = root_j

        # 1. 제목 그룹 기반 통합
        t_map = defaultdict(list)
        for i, tid in enumerate(title_idx): t_map[tid].append(i)
        for indices in t_map.values():
            for i in range(len(indices) - 1): union(indices[i], indices[i+1])

        # 2. 본문 그룹 기반 통합
        c_map = defaultdict(list)
        for i, cid in enumerate(body_idx): c_map[cid].append(i)
        for indices in c_map.values():
            for i in range(len(indices) - 1): union(indices[i], indices[i+1])

        # [수정] 모든 기사에 대해 고유한 T-번호와 B-번호를 그대로 노출
        # 분석 시 어떤 연결고리로 묶였는지 추적하기 위함
        df["cluster_id"] = [f"C-{find(i)}" for i in range(n)]
        df["title_id"] = [f"T-{idx}" for idx in title_idx]
        df["body_id"] = [f"B-{idx}" for idx in body_idx]

        return df

    # def _merge_groups_and_condition(self, df, title_idx, body_idx):
    #     n = len(df)
    #     parent = list(range(n))

    #     def find(i):
    #         if parent[i] == i:
    #             return i
    #         parent[i] = find(parent[i])
    #         return parent[i]

    #     def union(i, j):
    #         ri, rj = find(i), find(j)
    #         if ri != rj:
    #             parent[ri] = rj

    #     # AND 조건: title_id도 같고 body_id도 같은 경우만 union
    #     for i in range(n):
    #         for j in range(i + 1, n):
    #             if title_idx[i] == title_idx[j] and body_idx[i] == body_idx[j]:
    #                 union(i, j)

    #     df["cluster_id"] = [f"C-{find(i)}" for i in range(n)]
    #     df["title_id"] = [f"T-{idx}" for idx in title_idx]
    #     df["body_id"] = [f"B-{idx}" for idx in body_idx]

    #     return df

    def _mark_canonical_articles(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        selector = CanonicalNewsPolicy()
        df["is_canon"] = False
        df["replaced_by"] = None

        for group_id, group_df in df.groupby("cluster_id", sort=False):
            if group_df["news_id"].duplicated().any():
                print(f"[WARN] cluster_id={group_id}에 중복 news_id 존재")

            rep = selector.select(group_df)
            rep_id = rep["news_id"]

            df.loc[df["news_id"] == rep_id, ["is_canon", "replaced_by"]] = [True, rep_id]
            df.loc[
                (df["cluster_id"] == group_id) & (df["news_id"] != rep_id),
                "replaced_by"
            ] = rep_id

        return df

    def _save_similarity_debug_log(self, df: pd.DataFrame, keyword: str):

        if keyword == "test":
            return

        # 1. 로그에 남길 주요 컬럼만 추출
        target_cols = ["news_id", "pubDate", "collected_at", "is_canon", "cluster_id", "title_id", "body_id", "replaced_by", "title"]

        available_cols = [col for col in target_cols if col in df.columns]
        debug_df = df[available_cols].copy()

        # 2. 컬럼 순서 재정렬: news_id, pubDate, collected_at이 1~3번째로 오도록
        cols = list(debug_df.columns)
        for col in ["pubDate", "collected_at"]:
            if col not in cols:
                cols.append(col)
        def reorder(cols):
            base = ["news_id", "pubDate", "collected_at"]
            rest = [c for c in cols if c not in base]
            return [c for c in base if c in cols] + rest
        debug_df = debug_df[reorder(cols)]

        # pubDate 포맷 통일
        if "pubDate" in debug_df.columns:    
            debug_df["pubDate_str"] = (
                pd.to_datetime(debug_df["pubDate"], errors="coerce")
                .dt.strftime("%Y-%m-%d %H:%M:%S%z")
            )
                        
        # 3. cluster_id 기준 정렬(가독성)
        if "cluster_id" in debug_df.columns:
            debug_df = debug_df.sort_values("cluster_id")

        # 4. 로그 파일 경로 생성
        from config import OUTPUT_ROOT
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_dir = os.path.join(OUTPUT_ROOT, keyword, "similarity_logs")
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, f"{timestamp}.csv")
        debug_df.to_csv(path, index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    # 샘플 데이터 생성
    data = [
        {
            "news_id": "a1", "pubDate": "2026-01-28 10:00:00", "collected_at": "2026-01-28 10:01:00",
            "title": "제목1", "description": "설명1", "link": "url1", "originallink": "ourl1", "content": "본문1"
        },
        {
            "news_id": "a2", "pubDate": "2026-01-28 10:00:01", "collected_at": "2026-01-28 10:01:00",
            "title": "제목1", "description": "설명2", "link": "url2", "originallink": "ourl2", "content": "2222"
        },
        {
            "news_id": "a3", "pubDate": "2026-01-28 10:00:02", "collected_at": "2026-01-28 10:01:00",
            "title": "제목3", "description": "설명3", "link": "url3", "originallink": "ourl3", "content": "2222"
        },
        {
            "news_id": "a3", "pubDate": "2026-01-28 10:00:02", "collected_at": "2026-01-28 10:01:00",
            "title": "제목3", "description": "설명3", "link": "url3", "originallink": "ourl3", "content": "본문3"
        }
    ]
    df = pd.DataFrame(data)
    cluster = SingleNewsClusterer(title_threshold=0.2, content_threshold=0.15)
    df_clustered, stats = cluster.process(df, keyword="테스트")
    print(df_clustered)
    print(stats)