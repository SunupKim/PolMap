import pandas as pd
from processors.canonical_news_policy import CanonicalNewsPolicy
from processors.article_similarity_grouper import ArticleSimilarityGrouper
from datetime import datetime
import os

class NewsCluster:
    """
    뉴스 도메인 처리 객체
    - STEP 6: 제목 유사도 기반 그룹 생성 (T-번호 부여)
    - STEP 7: 그룹 내 본문 유사도 기반 정밀 중복 체크 (C-번호 부여)
    """

    def __init__(self, title_threshold: float, content_threshold: float):                
        # 클러스터링을 위한 제목 및 본문 임계값(Threshold) 초기화
        self.title_threshold = title_threshold
        self.content_threshold = content_threshold

    def process(self, df: pd.DataFrame, keyword: str):        
        # 1. 입력 데이터가 비어있는 경우 처리
        if df.empty:
            empty_stats = {
                "fetched": 0, 
                "similar_groups": 0, 
                "canonical_count": 0
            }
            # main.py의 언패킹 구조(df, stats)를 유지하기 위해 빈 객체들 반환
            return df, empty_stats

        # 2. 핵심 클러스터링 파이프라인 실행
        # [STEP 6] 제목 유사도 기반 1차 그룹핑
        df = self._build_title_groups(df)
        # [STEP 7] 동일 제목 그룹 내 본문 유사도 기반 2차 중복 체크
        df = self._refine_by_body_similarity(df)
        # [STEP 8] 그룹별 대표 기사(Canonical) 선정
        df = self._mark_canonical_articles(df)

        # 3. 분석가 검토를 위한 상세 프로세스 로그 저장
        self._save_similarity_debug_log(df, keyword)
        
        # 4. 최종 실행 통계 계산
        fetched_count = len(df)
        # 생성된 제목 그룹(T-번호)의 개수 카운트
        similar_groups = df["title_group_id"].nunique() if "title_group_id" in df.columns else 0
        # 최종적으로 살아남은 대표 기사의 총 개수
        canonical_count = int(df["is_canonical"].sum()) if "is_canonical" in df.columns else 0

        stats = {
            "fetched": fetched_count,
            "similar_groups": similar_groups,
            "canonical_count": canonical_count,
        }
        
        return df, stats

    def _save_similarity_debug_log(self, df: pd.DataFrame, keyword: str):
        """실제 데이터 컬럼 및 그룹핑 결과(T/C)를 포함한 상세 로그 저장"""
        
        # 1. 출력하고 싶은 대상 컬럼 리스트
        target_cols = ["news_id", "title", "is_canonical", "title_group_id", "content_group_id", "replaced_by"]
        
        # 2. 실제 데이터프레임(df)에 존재하는 컬럼만 필터링
        available_cols = [col for col in target_cols if col in df.columns]          
        debug_df = df[available_cols].copy()
        
        # --- [지적 사항 반영] 정렬 컬럼 존재 여부 체크 ---
        # 정렬 우선순위 정의
        sort_priority = ["title_group_id", "content_group_id"]
        # 위 우선순위 중 실제로 존재하는 컬럼만 추출
        existing_sort_cols = [c for c in sort_priority if c in debug_df.columns]
        
        # 존재하는 컬럼이 있을 때만 정렬 실행 (KeyError 방지)
        if existing_sort_cols:
            debug_df = debug_df.sort_values(existing_sort_cols)        

        # 3. 저장 실행
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_dir = f"outputs/{keyword}/similarity_logs"
        os.makedirs(log_dir, exist_ok=True)

        path = os.path.join(log_dir, f"{timestamp}.csv")
        debug_df.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"[Log] 유사도 분석 상세 로그가 생성되었습니다: {path}")

    def _build_title_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """제목 유사도를 분석하여 T-1, T-2 등 사건 단위 그룹 아이디 부여"""
        df = df.copy()
        titles = df["title"].fillna("").tolist()

        # 제목 전용 임계값을 사용하여 유사 그룹 산출
        title_grouper = ArticleSimilarityGrouper(threshold=self.title_threshold)
        indices = title_grouper.group(titles)
        
        # 직관적인 식별을 위해 'T-' 접두사 부여
        df["title_group_id"] = [f"T-{idx + 1}" for idx in indices]
        return df    

    def _refine_by_body_similarity(self, df: pd.DataFrame) -> pd.DataFrame:
        """동일 제목 그룹 내에서 본문 유사도를 검사하여 C-1, C-2 등 중복 아이디 부여"""
        if df.empty: return df
        df = df.copy()
        
        # 기본값 설정: 어떤 그룹에도 속하지 않으면 Unique(독자적 본문)로 표시
        df["content_group_id"] = "Unique" 

        # 1차로 묶인 제목 그룹(title_group_id) 내에서만 비교 수행
        for _, group_df in df.groupby("title_group_id"):
            # 그룹 내 기사가 2개 이상일 때만 비교 의미가 있음
            if len(group_df) < 2: 
                continue
            
            target_col = "content" if "content" in group_df.columns else "body" # 유연한 대응
            bodies = group_df[target_col].fillna("").tolist() if target_col in group_df.columns else []

            # 본문 전용 임계값을 사용하여 정밀 중복 체크
            body_grouper = ArticleSimilarityGrouper(threshold=self.content_threshold)
            body_indices = body_grouper.group(bodies)
            
            # 같은 제목 그룹 내에서도 본문이 비슷하면 동일한 C-번호 부여
            formatted_body_idx = [f"C-{idx + 1}" for idx in body_indices]
            df.loc[group_df.index, "content_group_id"] = formatted_body_idx
            
        return df

    def _mark_canonical_articles(self, df: pd.DataFrame) -> pd.DataFrame:
        """그룹 내에서 정책에 따라 가장 적합한 기사 1개 선정 (대표 기사 마킹)"""
        if df.empty: return df

        df = df.copy()
        selector = CanonicalNewsPolicy()
        df["is_canonical"] = False
        df["replaced_by"] = None

        # 제목 그룹(title_group_id)을 기준으로 대표 기사 선정 루프
        for group_id, group_df in df.groupby("title_group_id", sort=False):
            # 정책 객체(Policy)를 통해 그룹 내 베스트 기사 선정
            rep = selector.select(group_df)
            rep_id = rep["news_id"]

            # 선정된 기사는 대표(is_canonical)로 표시
            df.loc[df["news_id"] == rep_id, ["is_canonical", "replaced_by"]] = [True, rep_id]
            # 선정되지 못한 나머지 기사들은 어떤 기사로 대체되었는지 명시
            df.loc[(df["title_group_id"] == group_id) & (df["news_id"] != rep_id), "replaced_by"] = rep_id

        return df