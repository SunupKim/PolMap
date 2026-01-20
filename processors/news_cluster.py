import pandas as pd
from processors.canonical_news_policy import CanonicalNewsPolicy
from processors.article_similarity_grouper import ArticleSimilarityGrouper
from collections import defaultdict
from datetime import datetime
import os

class NewsCluster:
    """
    뉴스 도메인 처리 객체
    - 수평적 합집합(OR) 구조로 변경
    - 제목 유사도(T) 혹은 본문 유사도(B) 중 하나라도 충족 시 동일 그룹(C)으로 통합
    """

    def __init__(self, title_threshold: float, content_threshold: float):                                        
        self.title_threshold = title_threshold
        self.content_threshold = content_threshold

    def process(self, df: pd.DataFrame, keyword: str):        
        if df.empty: 
            print("데이터가 비어 있어 프로세스를 중단합니다.")
            return df, {"fetched": 0, "similar_groups": 0, "canonical_count": 0}

        # 1. 제목 기반 그룹핑 (T-번호)
        title_grouper = ArticleSimilarityGrouper(threshold=self.title_threshold)
        title_indices = title_grouper.group(df["title"].fillna("").tolist())
        
        # 2. 본문 기반 그룹핑 (B-번호)
        target_col = "content" if "content" in df.columns else "body"
        bodies = df[target_col].fillna("").tolist() if target_col in df.columns else []
        
        body_grouper = ArticleSimilarityGrouper(threshold=self.content_threshold)
        body_indices = body_grouper.group(bodies)

        # 3. [핵심] OR 조건 통합 및 직관적 로그 데이터 생성
        df = self._merge_groups_or_condition(df, title_indices, body_indices)
        
        # 4. 그룹별 대표 기사(Canonical) 선정
        df = self._mark_canonical_articles(df)

        # 5. 상세 프로세스 로그 저장
        self._save_similarity_debug_log(df, keyword)
        
        # 6. 통계 계산
        fetched_count = len(df)
        # [해석 주의] cluster_id의 유니크 개수는 곧 대표 기사(is_canonical)의 개수와 일치함
        similar_groups = df["cluster_id"].nunique() if "cluster_id" in df.columns else 0
        canonical_count = int(df["is_canonical"].sum()) if "is_canonical" in df.columns else 0
        
        print(f"[Cluster] 제목 및 내용 유사도 검사 완료 ----------> (삭제: {fetched_count - canonical_count}건)")


        stats = {
            "fetched": fetched_count,
            "similar_groups": similar_groups, # 클러스터(최종 그룹)의 총 개수
            "canonical_count": canonical_count, # 최종 생존 기사 수 (similar_groups와 1:1 대응)
        }
        
        return df, stats

    """
        제목(T) 혹은 본문(B)이 겹치는 기사들을 하나의 cluster_id(C)로 통합.
        
        [징검다리 통합(Chaining Effect) 안내]
        수평적 합집합(OR) 로직에 따라, 직접적으로 제목이나 본문이 겹치지 않더라도 
        중간에 연결고리 역할을 하는 기사가 있다면 하나의 그룹으로 묶입니다.
        예: 기사A(T1, B1) - 기사B(T1, B2) - 기사C(T2, B2) 
           -> 기사A와 기사C는 직접 겹치는 속성이 없으나 기사B를 통해 C-그룹으로 통합됨.
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

    def _mark_canonical_articles(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        df = df.copy()
        selector = CanonicalNewsPolicy()
        df["is_canonical"] = False
        df["replaced_by"] = None

        # cluster_id 기준으로 대표 기사 선정
        for group_id, group_df in df.groupby("cluster_id", sort=False):
            rep = selector.select(group_df)
            rep_id = rep["news_id"]
            df.loc[df["news_id"] == rep_id, ["is_canonical", "replaced_by"]] = [True, rep_id]
            df.loc[(df["cluster_id"] == group_id) & (df["news_id"] != rep_id), "replaced_by"] = rep_id
        return df

    def _save_similarity_debug_log(self, df: pd.DataFrame, keyword: str):
        target_cols = ["news_id", "is_canonical", "cluster_id", "title_id", "body_id", "replaced_by", "title"]
        available_cols = [col for col in target_cols if col in df.columns]          
        debug_df = df[available_cols].copy()
        
        if "cluster_id" in debug_df.columns:
            debug_df = debug_df.sort_values("cluster_id")        

        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_dir = f"outputs/{keyword}/similarity_logs"
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, f"{timestamp}.csv")
        debug_df.to_csv(path, index=False, encoding='utf-8-sig')
        #print(f"[Log] 유사도 분석 상세 로그 생성 완료: {path}")