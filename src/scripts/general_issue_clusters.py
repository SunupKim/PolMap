# RULE: 디버그 로그(print) 및 주석 삭제 금지.
# scripts/general_issue_clusters.py

# 임베딩은 기사 자체의 좌표(누적)고 클러스터링은 특정 시간창에서 그 좌표들로 ‘이슈 지도’를 다시 그리는 작업
# 정치 뉴스라는 거대한 책더미를 내용별로 12개(N_CLUSTERS)의 바구니에 나누어 담는 작업을 자동화
# 실행 방법: python -m src.scripts.general_issue_clusters
# 임베딩 → KMeans로 클러스터링 → 대표기사 선택 → LLM 통해 라벨링

import os
import json
import pickle
import pandas as pd
import numpy as np
import time
#from pathlib import Path

from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from datetime import datetime
from sklearn.metrics.pairwise import cosine_similarity
from src.llm.issue_labeler import generate_issue_label
#from src.utils.dataframe_utils import canonical_df_save
from src.config import (
    #ROOT_DIR,
    DATA_DIR,
    PROMPTS_DIR,
    CANONICAL_ARCHIVE_PATH,
    LLM_PROVIDER,
    # Gemini 관련
    gen_client,
    GEMINI_MODEL_2_5,
    GEMINI_CONFIG_NORMAL,
    # OpenAI 관련
    openai_client,
    OPENAI_MODEL,
    NORMAL_TEMPERATURE,
)

"""
이 주석은 지우지 않는다. 기록이다.
HOURS_WINDOW
의미: 이슈 판을 구성할 때 포함할 뉴스의 시간 범위(최근 N시간)
역할: 어떤 이슈가 '현재의 정치 이슈'로 취급되는지를 결정
성격: 설계 결정값(판의 시간적 경계)
주의: 이 값을 변경하면 이슈 클러스터 전체를 재생성해야 한다

N_CLUSTERS
의미: 이슈 클러스터 개수 K
역할: 정치판을 몇 개의 이슈 공간으로 나눌지 결정
성격: 설계 결정값이지, 모델 파라미터가 아니다
왜 필요한가: KMeans는 반드시 K를 알아야 한다. 
원칙: 테스트 중에는 바꿔도 되지만 한 번 고정하면 덮어쓰지 않는다. 바꾸는 순간 판이 바뀐 것으로 취급한다.
권장값 연구: 기사수 350개 -> 12, 500개 -> 20 
테스트 
HOURS_WINDOW = 8 → 기사수 350 → N_CLUSTERS = 12 적절
HOURS_WINDOW = 8 → 기사수 350~650 → N_CLUSTERS = ?
HOURS_WINDOW = 12 → 기사수 700~1300건 → N_CLUSTERS = 16
HOURS_WINDOW = 24 → 기사수 2000건 → N_CLUSTERS = 16
하루 뉴스는 약 2000건 정도?
N_CLUSTERS = 기사수/30 정도가 적절해 보이는데 졸라 테스트를 해봐야겠다
"""

FIXED_BASE_DATE = "2026-02-12T16:20:00+09:00"  # 기준 시점

HOURS_WINDOW = 16  # 최근 N시간 치 뉴스로 이슈 판 구성

# 경로 설정
PROMPT_PATH = PROMPTS_DIR / "general_issue_clusters.txt"
ISSUE_CLUSTERS_ROOT = DATA_DIR / "issue_clusters"
ARTICLE_EMBEDDINGS_CACHE = ISSUE_CLUSTERS_ROOT / "embeddings_cache.pkl"

# 웹 서비스용 최신 데이터 경로
CURRENT_ISSUE_DIR = ISSUE_CLUSTERS_ROOT / "current"

MODEL_NAME = "dragonkue/multilingual-e5-small-ko-v2"
RANDOM_STATE = 42

def build_corpus(df: pd.DataFrame):
    texts = []
    article_ids = []

    for _, row in df.iterrows():
        content = str(row.get("content", ""))
        text = f"passage: {row['title']} {content}"
        texts.append(text)
        article_ids.append(row["news_id"])

    return texts, article_ids

# 임베딩 생성 함수 (캐시 미사용 버전)
def create_embeddings(texts):
    if not texts:
        return None
    
    print(f"임베딩 생성 시작... (총 {len(texts)}개 기사)")
    model = SentenceTransformer(MODEL_NAME)
    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        normalize_embeddings=True
    )
    return embeddings

# 임베딩 생성 함수 (캐시 사용 버전)
def load_or_create_embeddings(texts):
    # 캐시가 있어도 현재 텍스트 개수와 다르면 새로 생성해야 합니다.
    # 하지만 더 안전하게 하기 위해, 이번에는 캐시를 쓰지 않거나 
    # 파일명에 텍스트 개수를 포함하는 방식이 좋습니다.

    if os.path.exists(ARTICLE_EMBEDDINGS_CACHE):
        print("임베딩 캐시 로드")
        with open(ARTICLE_EMBEDDINGS_CACHE, "rb") as f:
            cached_embeddings = pickle.load(f)
            # 캐시된 임베딩 개수와 현재 분석할 텍스트 개수가 일치할 때만 로드
            if len(cached_embeddings) == len(texts):
                print(f"임베딩 캐시 로드 완료 (개수: {len(texts)})")
                return cached_embeddings
            else:
                print(f"캐시 불일치 (캐시: {len(cached_embeddings)}, 현재: {len(texts)}). 새로 생성합니다.")

    print(f"임베딩 생성 중... ({len(texts)}개 기사)")
    model = SentenceTransformer(MODEL_NAME)
    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        normalize_embeddings=True
    )

    os.makedirs(os.path.dirname(ARTICLE_EMBEDDINGS_CACHE), exist_ok=True)
    with open(ARTICLE_EMBEDDINGS_CACHE, "wb") as f:
        pickle.dump(embeddings, f)

    return embeddings

def select_representative_titles(
    embeddings,
    cluster_ids,
    article_titles,
    issue_centers,
    top_k=5
):
    cluster_to_titles = {}

    for cid, center in issue_centers.items():
        idxs = np.where(cluster_ids == cid)[0]
        if len(idxs) == 0:
            cluster_to_titles[cid] = []
            continue

        sims = cosine_similarity(
            embeddings[idxs],
            center.reshape(1, -1)
        ).flatten()

        top_idxs = idxs[np.argsort(sims)[::-1][:top_k]]
        cluster_to_titles[cid] = [article_titles[i] for i in top_idxs]

    return cluster_to_titles

def main():

    start_time = time.time()
    df = pd.read_csv(CANONICAL_ARCHIVE_PATH)    # 데이터셋 로드
    print(f"데이터셋 로드 완료 → 전체 기사 수: {len(df)}")

    # [경로 설정] 실행 시점 기준 날짜-시간 폴더 생성
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    current_output_dir = os.path.join(ISSUE_CLUSTERS_ROOT, run_timestamp)
    
    issue_centers_path = os.path.join(current_output_dir, "centers.json")
    issue_meta_path = os.path.join(current_output_dir, "meta.json")
    article_issue_map_path = os.path.join(current_output_dir, "article_map.csv")

    # === 기준 날짜 및 시간 설정 (이 시점부터 과거 N시간을 추적) ===
    # 1. 날짜 데이터 전처리 (시간대 유지)    
    # 방법: 모두 KST(Asia/Seoul)로 통일하여 비교
    df["pubDate"] = pd.to_datetime(df["pubDate"], errors="coerce").dt.tz_convert('Asia/Seoul')
    base_timestamp = pd.Timestamp.now(tz='Asia/Seoul')
    
    # [디버그용] 데이터셋의 실제 시간 범위 출력
    print(f"데이터셋 시간 범위: {df['pubDate'].min()} ~ {df['pubDate'].max()}")

    # 데이터의 시간대 정보와 동기화
    # 2. 현재 시각을 기사 데이터와 동일한 타임존(+0900)으로 생성
    # pd.Timestamp.now()에 tz 정보를 추가하여 데이터와 비교 가능하게 만듭니다.
    base_timestamp = pd.Timestamp.now(tz='Asia/Seoul')

    # 고정 기준 시점 사용시 주석 해제
    #base_timestamp = pd.to_datetime(FIXED_BASE_DATE)
    
    # HOURS_WINDOW 거리만큼 과거 시점 계산
    cutoff_date = base_timestamp - pd.Timedelta(hours=HOURS_WINDOW)

    before_count = len(df)
    df_filtered = df[(df["pubDate"] >= cutoff_date) & (df["pubDate"] <= base_timestamp)].copy()
    after_count = len(df_filtered)
    
    # [동적 설정] 기사 수에 따라 클러스터 개수 결정 (약 40개당 1개 이슈)
    # 너무 적으면 클러스터링 의미가 없으므로 최소 2개로 설정
    N_CLUSTERS = int(after_count / 20)
    if N_CLUSTERS < 2:
        N_CLUSTERS = 2

    print(f"기사 수 변화: {before_count} → {after_count} (기준: {FIXED_BASE_DATE}, 최근 {HOURS_WINDOW}시간), N_CLUSTERS={N_CLUSTERS}")

    if after_count < N_CLUSTERS:
        print(f"경고: 필터링된 기사 수({after_count})가 클러스터 수({N_CLUSTERS})보다 적습니다.")
        # 필요한 경우 여기서 return 하거나 N_CLUSTERS를 조정하는 로직을 넣을 수 있습니다.
        return
      
    ############# 이슈 클러스터링 본체 ###############
    # 기사 제목 리스트 확보 (중요)
    # 5. [중요] 필터링된 데이터셋으로 코퍼스와 제목 리스트 재구성
    # 인덱스 에러 방지를 위해 반드시 df_filtered를 사용해야 합니다.
    article_titles = df_filtered["title"].tolist()
    texts, article_ids = build_corpus(df_filtered)
        
    #embeddings = load_or_create_embeddings(texts)
    embeddings = create_embeddings(texts)
    
    kmeans = KMeans(
        n_clusters=N_CLUSTERS,
        random_state=RANDOM_STATE,
        n_init="auto"
    )
    cluster_ids = kmeans.fit_predict(embeddings)

    print("클러스터링 완료→클러스터 중심 계산 및 라벨 생성")
    issue_centers = []   # 기계용
    issue_meta = []      # 사람용

    for cid in range(N_CLUSTERS):
        idxs = np.where(cluster_ids == cid)[0]
        if len(idxs) == 0:
            continue

        center_embedding = embeddings[idxs].mean(axis=0)

        # 대표 기사 선택 (중심과 가장 가까운 5개)
        sims = cosine_similarity(
            embeddings[idxs],
            center_embedding.reshape(1, -1)
        ).flatten()

        top_local_idxs = np.argsort(sims)[::-1][:5]
        top_article_idxs = idxs[top_local_idxs]
        representative_titles = [article_titles[i] for i in top_article_idxs]

        print(f"- 이슈 클러스터 {cid}: 대표 기사 제목들")
        for title in representative_titles:
            print(f"  - {title}")

        # LLM 제공자에 따라 적절한 파라미터 전달
        if LLM_PROVIDER == "GEMINI":
            issue_label = generate_issue_label(
                titles=representative_titles,
                provider="GEMINI",
                prompt_path=str(PROMPT_PATH),
                gen_client=gen_client,
                model=GEMINI_MODEL_2_5,
                config=GEMINI_CONFIG_NORMAL,
            )
        elif LLM_PROVIDER == "OPENAI":
            issue_label = generate_issue_label(
                titles=representative_titles,
                provider="OPENAI",
                prompt_path=str(PROMPT_PATH),
                openai_client=openai_client,
                model=OPENAI_MODEL,
                temperature=NORMAL_TEMPERATURE,
            )
        else:
            print(f"경고: 지원하지 않는 LLM 제공자 '{LLM_PROVIDER}'. 기본 라벨 사용.")
            issue_label = ""

        if not issue_label:
            issue_label = f"issue_{cid}"

        issue_centers.append({
    "issue_cluster_id": int(cid),
    "issue_center_embedding": center_embedding.tolist()
})
        issue_meta.append({
            "issue_cluster_id": int(cid),
            "issue_label": issue_label,
            "cluster_size": int(len(idxs)),
            "representative_titles": representative_titles
        })

    print("결과 저장")
    os.makedirs(current_output_dir, exist_ok=True)
    os.makedirs(CURRENT_ISSUE_DIR, exist_ok=True)

    # 1. 타임스탬프 폴더에 저장 (아카이브용)
    with open(issue_centers_path, "w", encoding="utf-8") as f:
        json.dump(issue_centers, f, ensure_ascii=False, indent=2)

    with open(issue_meta_path, "w", encoding="utf-8") as f:
        json.dump(issue_meta, f, ensure_ascii=False, indent=2)

    # 2. current 폴더에 저장 (웹 서비스용 최신 데이터)
    current_meta_path = CURRENT_ISSUE_DIR / "meta.json"
    with open(current_meta_path, "w", encoding="utf-8") as f:
        json.dump(issue_meta, f, ensure_ascii=False, indent=2)
    
    current_centers_path = CURRENT_ISSUE_DIR / "centers.json"
    with open(current_centers_path, "w", encoding="utf-8") as f:
        json.dump(issue_centers, f, ensure_ascii=False, indent=2)
    
    print(f"웹 서비스용 최신 데이터 업데이트 완료: {CURRENT_ISSUE_DIR}")

    article_issue_df = pd.DataFrame({
        "news_id": article_ids,
        "issue_cluster_id": cluster_ids
    })

    article_issue_df.to_csv(article_issue_map_path, index=False)   

    end_time = time.time()
    elapsed = end_time - start_time    
    print(f"- 총 실행시간: {elapsed:.1f}초")
    print(f"- 이슈 클러스터: {issue_centers_path}")
    print(f"- 기사 매핑: {article_issue_map_path}")

if __name__ == "__main__":    
    main()
    
