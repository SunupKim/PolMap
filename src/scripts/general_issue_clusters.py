"""
RULE: 디버그 로그(print) 및 주석 삭제 금지.
scripts/general_issue_clusters.py
임베딩은 기사 자체의 좌표(누적)고 클러스터링은 특정 시간창에서 그 좌표들로 ‘이슈 지도’를 다시 그리는 작업
정치 뉴스라는 거대한 책더미 속에서 밀도가 높은(유사한 내용이 모인) 영역을 찾아 자동으로 이슈 그룹화
실행 방법: python -m src.scripts.general_issue_clusters
임베딩 → HDBSCAN(밀도 기반 클러스터링) → 노이즈 제거 → 대표기사 선택 → LLM 라벨링
"""

import os
import json
import pickle
import pandas as pd
import numpy as np
import time
from datetime import datetime

from sentence_transformers import SentenceTransformer
from sklearn.metrics import silhouette_score
from sklearn.cluster import HDBSCAN
from sklearn.metrics.pairwise import cosine_similarity

from src.llm.issue_labeler import generate_issue_label
from src.config import (    
    DATA_DIR,
    PROMPTS_DIR,
    CANONICAL_ARCHIVE_PATH,
    LLM_PROVIDER,    
    gen_client,
    GEMINI_MODEL_2_5,
    GEMINI_CONFIG_NORMAL,    
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

MIN_CLUSTER_SIZE
의미: 하나의 독립된 '이슈'로 인정하기 위한 최소 기사 수 
역할: 클러스터의 최소 크기를 결정. 이보다 작은 모임은 노이즈(-1)로 분류됨
주의: 너무 낮으면 파편화된 이슈가 많아지고, 너무 높으면 중요한 소수 기사 이슈가 묻힘

MIN_SAMPLES
의미: 클러스터 형성을 위한 핵심 포인트 주변의 이웃 수
역할: 클러스터링의 '보수성'을 결정. 값이 커질수록 노이즈 분류가 엄격해짐
주의: 이 값이 커지면 군집 경계에 있는 기사들이 대거 노이즈로 빠질 수 있음

왜 HDBSCAN인가: 
1. 이슈의 개수(K)를 미리 정할 필요 없이 데이터 밀도에 따라 자동 결정
2. 어떤 이슈에도 속하지 않는 기사를 노이즈(-1)로 분리하여 클러스터 순도 향상
3. 모양이 불규칙한 이슈 군집도 효과적으로 포착 가능
"""

DISABLE_LLM_FOR_TEST = True  # 테스트 시 True, 운영 시 False
FIXED_BASE_DATE = None #None이면 현재 시각으로 테스트한다.
#FIXED_BASE_DATE = "2026-02-14T06:00:00+09:00"

HOURS_WINDOW = 12  # 최근 N시간 치 뉴스로 이슈 판 구성

# 아래 2개 값의 조합이 가장 중요하다. (3,3) (3,4) (4,3) (4,4)
MIN_CLUSTER_SIZE = 3
MIN_SAMPLES  = 4

"""
2026-02-14 16:42:15 | 2026-02-13 11:20:51  테스트 결과 (3,4)가 가장 마음에 든다
2026-02-14 17:02:23 | 2026-02-14 17:02:11  테스트 결과는 (3,3)만 적절하다ㅠ

"""

# 경로 설정
PROMPT_PATH = PROMPTS_DIR / "general_issue_clusters.txt"
ISSUE_CLUSTERS_ROOT = DATA_DIR / "issue_clusters"
ARTICLE_EMBEDDINGS_CACHE = ISSUE_CLUSTERS_ROOT / "embeddings_cache.pkl"
EXPERIMENT_LOG_PATH = ISSUE_CLUSTERS_ROOT / "clustering_experiments.log"


# 웹 서비스용 최신 데이터 경로
CURRENT_ISSUE_DIR = ISSUE_CLUSTERS_ROOT / "current"

MODEL_NAME = "dragonkue/multilingual-e5-small-ko-v2"
# MODEL_NAME = "intfloat/multilingual-e5-base"

RANDOM_STATE = 42

def build_corpus(df: pd.DataFrame):
    texts = []
    article_ids = []

    for _, row in df.iterrows():
        content = str(row.get("content", ""))

        if MODEL_NAME == "dragonkue/multilingual-e5-small-ko-v2":            
            #text = f"{row['title']}"            
            text = f"{row['title']} {content[:300]}"
            #text = f"{row['title']} {content[:500]}"
            #text = f"passage: {row['title']} {content}"

        elif MODEL_NAME == "intfloat/multilingual-e5-base":
            text = f"{row['title']} {content[:500]}"

        else:
            print("모델을 찾을 수 없습니다")
            return

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
    # [디버그용] 데이터셋의 실제 시간 범위 출력
    print(f"데이터셋 시간 범위: {df['pubDate'].min()} ~ {df['pubDate'].max()}")
    
    # 2. 현재 시각을 기사 데이터와 동일한 타임존(+0900)으로 생성
    # pd.Timestamp.now()에 tz 정보를 추가하여 데이터와 비교 가능하게 만듭니다.
        
    # === 기준 날짜 및 시간 설정 부분 ===
    if FIXED_BASE_DATE is not None:
        print(f"FIXED_BASE_DATE를 사용합니다 =>", {FIXED_BASE_DATE})
        # ISO 형식을 명확히 파싱
        base_timestamp = pd.to_datetime(FIXED_BASE_DATE)
        if base_timestamp.tzinfo is None:
            base_timestamp = base_timestamp.tz_localize("Asia/Seoul")
        else:
            base_timestamp = base_timestamp.tz_convert("Asia/Seoul")
    else:
        base_timestamp = pd.Timestamp.now(tz='Asia/Seoul')

    # 구간 계산 로그 출력 (매우 중요)
    cutoff_date = base_timestamp - pd.Timedelta(hours=HOURS_WINDOW)
    print(f"--- 필터링 디버그 ---")
    print(f"설정된 기준시: {base_timestamp}")
    print(f"윈도우 시작시: {cutoff_date}")

    before_count = len(df)
    df_filtered = df[(df["pubDate"] >= cutoff_date) & (df["pubDate"] <= base_timestamp)].copy()
    after_count = len(df_filtered)

    print(f"기사 수 변화: {before_count} → {after_count} (최근 {HOURS_WINDOW}시간)")

    ############# 이슈 클러스터링 본체 ###############
    # 기사 제목 리스트 확보 (중요)
    # 5. [중요] 필터링된 데이터셋으로 코퍼스와 제목 리스트 재구성
    # 인덱스 에러 방지를 위해 반드시 df_filtered를 사용해야 합니다.
    article_titles = df_filtered["title"].tolist()
    texts, article_ids = build_corpus(df_filtered)
        
    #embeddings = load_or_create_embeddings(texts)
    embeddings = create_embeddings(texts)
    
    clusterer = HDBSCAN(
        min_cluster_size=MIN_CLUSTER_SIZE,   # 최소 MIN_CLUSTER_SIZE개의 기사는 있어야 군집으로 인정
        min_samples=MIN_SAMPLES,
        metric="cosine",                                     # 임베딩 정합
        copy=False  
    )

    cluster_ids = clusterer.fit_predict(embeddings)    
    print(f"HDBSCAN 파라미터 → min_cluster_size={MIN_CLUSTER_SIZE}, min_samples={MIN_SAMPLES}")

    #=============== HDBSCAN을 위한 점수 계산

    unique_all = set(cluster_ids)
    n_clusters = len([l for l in unique_all if l != -1])
    n_noise = int(np.sum(cluster_ids == -1))
    noise_ratio = (n_noise / len(cluster_ids)) * 100   

    # 실루엣 점수 계산
    valid_mask = cluster_ids != -1
    unique_labels = set(cluster_ids[valid_mask])
    score = 0.0
    if valid_mask.sum() >= 2 and len(unique_labels) >= 2:
        score = silhouette_score(embeddings[valid_mask], cluster_ids[valid_mask], metric="cosine")
    
    # 결과 출력
    print(f"HDBSCAN 결과 → 군집 수: {n_clusters}, 노이즈 기사 수: {n_noise} ({noise_ratio:.1f}%)")
    if score > 0:
        print(f"★ 실루엣 점수: {score:.4f}")

    # ================= LOG 파일 기록 추가 (들여쓰기 밖으로 이동) =================
    log_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "base_date": base_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "total_articles": len(texts), # 하나만 남김
        "silhouette_score": f"{score:.4f}"
    }

    os.makedirs(ISSUE_CLUSTERS_ROOT, exist_ok=True)
    
    file_exists = os.path.isfile(EXPERIMENT_LOG_PATH)
    with open(EXPERIMENT_LOG_PATH, "a", encoding="utf-8") as f:
        if not file_exists:
            # 헤더와 데이터 컬럼 순서를 일치시킴
            f.write("      Time       |       BaseDate       | Size | Samples | Total | Clusters | Noise(%) | Silhouette | Model\n")               

            f.write("-" * 120 + "\n")
        
        # f-string 공백을 조절하여 표 형태 유지
        log_line = (f"{log_entry['timestamp']} | {log_entry['base_date']} | "
                    f"{MIN_CLUSTER_SIZE:^4} | {MIN_SAMPLES:^4} | {log_entry['total_articles']:^5} | "
                    f"{n_clusters:^5} | {n_noise:>3}({noise_ratio:>4.1f}%) | "
                    f"{log_entry['silhouette_score']:^5} | {MODEL_NAME[:12]}\n")
        f.write(log_line)  
    
    # =======================================================

  
    issue_centers = []   # 기계용
    issue_meta = []      # 사람용

    valid_labels = sorted([l for l in set(cluster_ids) if l != -1])
    for cid in valid_labels:
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

        if DISABLE_LLM_FOR_TEST:
            issue_label = f"issue_{cid}"  # 테스트용 더미 라벨
        # LLM 제공자에 따라 적절한 파라미터 전달
        else :
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
    
