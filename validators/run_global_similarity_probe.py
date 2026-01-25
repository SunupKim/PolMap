# 실행법 python -m validators.run_global_similarity_probe

"""
**“이미 만들어진 최종 뉴스 묶음에서,
link로는 못 잡은 ‘사실상 같은 기사일 가능성’을 제목·본문 유사도로 찾아서
사람이 볼 수 있게 목록으로 뽑아주는 도구”**다.

1. 입력
outputs/aggregated/canonical_archive.csv (이미 키워드별 클러스터링 끝났고 글로벌 link dedup까지 끝난 데이터)
2. 계산
기사 제목 유사도, 기사 본문 유사도 두 기준을 OR (제목이 비슷하거나 본문이 비슷하면) AND (제목도 비슷하고 본문도 비슷하면) 중 하나로 묶어서 테스트

3. 결과
“2개 이상 묶인 그룹”만 골라서 중복 의심 후보 리스트를 만든다. 아무 것도 없으면 성공이다.

"""

import pandas as pd
import os
from collections import defaultdict
from processors.article_similarity_grouper import ArticleSimilarityGrouper
from datetime import datetime
from time import perf_counter

from config import CANONICAL_ARCHIVE_PATH, PROBE_TITLE_THRESHOLD, PROBE_CONTENT_THRESHOLD

class GlobalSimilarityProbe:
    """
    total_news_archive.csv 대상 글로벌 유사도 탐지기
    - 제목 / 본문 유사도만 계산
    - OR / AND union 선택 가능
    - 대표 선정, 삭제는 하지 않음
    """

    def __init__(
        self,
        title_threshold: float,
        content_threshold: float,
        union_mode: str = "OR",  # "OR" or "AND"
        output_dir: str = "outputs/global_similarity_test"
    ):
        assert union_mode in ("OR", "AND")
        self.title_threshold = title_threshold
        self.content_threshold = content_threshold
        self.union_mode = union_mode
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def run(self, df: pd.DataFrame):
        if df.empty:
            print("데이터가 비어 있습니다.")
            return

        n = len(df)

        # 1. 제목 유사도 그룹
        title_grouper = ArticleSimilarityGrouper(self.title_threshold)
        title_ids = title_grouper.group(df["title"].fillna("").tolist())

        # 2. 본문 유사도 그룹
        body_col = "content" if "content" in df.columns else "body"
        body_grouper = ArticleSimilarityGrouper(self.content_threshold)
        body_ids = body_grouper.group(df[body_col].fillna("").tolist())

        # 3. union-find 초기화
        parent = list(range(n))

        def find(i):
            if parent[i] != i:
                parent[i] = find(parent[i])
            return parent[i]

        def union(i, j):
            ri, rj = find(i), find(j)
            if ri != rj:
                parent[ri] = rj

        # 4. 연결 구조 기록용
        edges = []

        # title 기준
        title_map = defaultdict(list)
        for i, gid in enumerate(title_ids):
            title_map[gid].append(i)

        # body 기준
        body_map = defaultdict(list)
        for i, gid in enumerate(body_ids):
            body_map[gid].append(i)

        # 5. union 로직
        if self.union_mode == "OR":
            maps = [("title", title_map), ("body", body_map)]
        else:  # AND
            maps = [("both", None)]

        if self.union_mode == "OR":
            for label, m in maps:
                for indices in m.values():
                    for i in range(len(indices) - 1):
                        a, b = indices[i], indices[i + 1]
                        union(a, b)
                        edges.append({
                            "src": df.iloc[a]["news_id"],
                            "dst": df.iloc[b]["news_id"],
                            "via": label
                        })

        else:  # AND
            pair_map = defaultdict(list)
            for i in range(n):
                pair_map[(title_ids[i], body_ids[i])].append(i)

            for indices in pair_map.values():
                for i in range(len(indices) - 1):
                    a, b = indices[i], indices[i + 1]
                    union(a, b)
                    edges.append({
                        "src": df.iloc[a]["news_id"],
                        "dst": df.iloc[b]["news_id"],
                        "via": "title+body"
                    })

        # 6. cluster_id 생성
        df = df.copy()
        df["global_sim_cluster"] = [f"G-{find(i)}" for i in range(n)]

        # 7. 중복 후보만 필터링 (cluster size >= 2)
        counts = df["global_sim_cluster"].value_counts()
        dup_clusters = counts[counts >= 2].index.tolist()
        df_candidates = df[df["global_sim_cluster"].isin(dup_clusters)]

        # 8. 로그 저장
        ts = datetime.now().strftime("%Y%m%d_%H%M")        
        cand_path = os.path.join(
            self.output_dir, f"candidates_{self.union_mode}_{ts}.csv" # 중복 의심 후보 목록
        )
        edge_path = os.path.join(
            self.output_dir, f"edges_{self.union_mode}_{ts}.csv" # 연결구조
        )

        df_candidates.to_csv(cand_path, index=False, encoding="utf-8-sig")
        pd.DataFrame(edges).to_csv(edge_path, index=False, encoding="utf-8-sig")

        print(f"[완료] 중복 후보 {len(df_candidates)}건")
        # print(f"[저장] 후보: {cand_path}")
        # print(f"[저장] 연결 구조: {edge_path}")

if __name__ == "__main__":
    start_ts = perf_counter()
    df = pd.read_csv(f"{CANONICAL_ARCHIVE_PATH}")

    # OR : 제목 유사 OR 본문 유사
    # AND : 제목 유사 AND 본문 유사

    probe = GlobalSimilarityProbe(
        title_threshold=PROBE_TITLE_THRESHOLD,
        content_threshold=PROBE_CONTENT_THRESHOLD,
        union_mode="OR"  # 또는 "AND"
    )

    probe.run(df)
    elapsed_sec = round(perf_counter() - start_ts, 3)
    print(f"[완료] 실행 시간: {elapsed_sec}초")

    