import simhash
import pandas as pd
from typing import List, Tuple


class SimHashDeduplicator:
    """
    SimHash 기반 near-duplicate 제거 전용 클래스 (제목 + 본문 기준)

    책임 범위
    - title / content 각각에 대해 SimHash 계산
    - Hamming distance 기준 near-duplicate 판별
    - '제목과 본문이 모두 거의 동일한 기사'만 제거
    - 이슈 유사도 판단이나 클러스터링은 수행하지 않음
    """

    def __init__(self, body_distance: int, title_distance: int):
        """
        Parameters
        ----------
        body_distance : int
            본문 SimHash 간 Hamming distance 임계값.
            이 값 이하일 경우 본문이 거의 동일한 것으로 판단한다.

        title_distance : int
            제목 SimHash 간 Hamming distance 임계값.
            이 값 이하일 경우 제목이 거의 동일한 것으로 판단한다.
        """
        self.body_distance = body_distance
        self.title_distance = title_distance

    def _build_simhash(self, text: str) -> simhash.Simhash:
        """
        주어진 텍스트로부터 SimHash 생성
        """
        if not text:
            text = ""
        return simhash.Simhash(text)

    def deduplicate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df.empty:
            return df, df

        df = df.copy()

        # 필수 컬럼 확인
        if "content" not in df.columns or "title" not in df.columns:
            print("[SimHash] 'title' 또는 'content' 컬럼이 없어 중복 제거를 건너뜁니다.")
            return df, pd.DataFrame()

        # SimHash 계산
        df["_body_simhash"] = df["content"].fillna("").apply(self._build_simhash)
        df["_title_simhash"] = df["title"].fillna("").apply(self._build_simhash)

        # 시간순 정렬 (먼저 수집된 기사를 유지)
        if "collected_at" in df.columns:
            df = df.sort_values("collected_at", ascending=True)
        elif "pubDate" in df.columns:
            df = df.sort_values("pubDate", ascending=True)

        keep_indices: List[int] = []
        dropped_indices: set[int] = set()

        rows = df.to_dict("records")

        for i, row_i in enumerate(rows):
            if i in dropped_indices:
                continue

            keep_indices.append(i)

            for j in range(i + 1, len(rows)):
                if j in dropped_indices:
                    continue

                # 본문 + 제목 모두 비교
                body_dist = row_i["_body_simhash"].distance(rows[j]["_body_simhash"])
                title_dist = row_i["_title_simhash"].distance(rows[j]["_title_simhash"])

                body_similar = body_dist <= self.body_distance
                title_similar = title_dist <= self.title_distance

                # AND 조건: 둘 다 거의 동일한 경우만 제거
                if body_similar and title_similar:

                    print(
                        f"\n[SimHash 중복 발견]"
                        f"\n - 본문 거리: {body_dist} (≤ {self.body_distance})"
                        f"\n - 제목 거리: {title_dist} (≤ {self.title_distance})"
                    )
                    print(f" > 기준 기사: {row_i['title']}")
                    print(f"   본문: {row_i['content'][:100]}...")
                    print(f" > 삭제 대상: {rows[j]['title']}")
                    print(f"   본문: {rows[j]['content'][:100]}...")
                    print("-" * 60)

                    dropped_indices.add(j)

        kept_df = df.iloc[keep_indices].copy()
        removed_df = df.iloc[list(dropped_indices)].copy()

        # 내부 컬럼 제거
        kept_df.drop(columns=["_body_simhash", "_title_simhash"], inplace=True, errors="ignore")
        removed_df.drop(columns=["_body_simhash", "_title_simhash"], inplace=True, errors="ignore")

        removed_df["removed_by"] = "simhash"

        print(
            f"[SimHash] near-duplicate 인풋 {len(df)}건 → "
            f"아웃풋 {len(kept_df)}건 (삭제 {len(removed_df)}건)"
        )

        return kept_df, removed_df
