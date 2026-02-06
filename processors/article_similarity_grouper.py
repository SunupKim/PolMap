# processors/article_similarity_grouper.py
# 모듈테스트 방법 python -m processors.article_similarity_grouper

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

class ArticleSimilarityGrouper:
    def __init__(self, threshold, field_name=None, test_mode=False):
        self.threshold = threshold
        self.field_name = field_name
        self.test_mode = test_mode

    def group(self, texts: list[str]) -> list[int]:
        if not texts:
            return []

        vectorizer = TfidfVectorizer(
            ngram_range=(1, 2),
            min_df=1
        )
        tfidf = vectorizer.fit_transform(texts)
        sim_matrix = cosine_similarity(tfidf)

        group_ids = [-1] * len(texts)
        current_group = 0

        for i in range(len(texts)):
            if group_ids[i] != -1:
                continue

            group_ids[i] = current_group

            label = self.field_name or ""
            print("")
            for j in range(i + 1, len(texts)):

                # 모든 비교 쌍에 대해 점수를 출력하고 싶다면 이 위치에 작성
                similarity_score = sim_matrix[i, j]

                if self.test_mode:
                    print(f"[TEST][{label} 유사도 {similarity_score:.4f}] {i}번 <-> {j}번")
                    print(f"  [{i}] {texts[i][:80]}...")
                    print(f"  [{j}] {texts[j][:80]}...")               

                if similarity_score >= self.threshold:
                    print(f"아래 2개 기사는 {label}이 유사합니다 {similarity_score:.4f}")
                    print(f"  [{i}] {texts[i][:150]}...")
                    print(f"  [{j}] {texts[j][:150]}...\n")
                    group_ids[j] = current_group

            current_group += 1

        return group_ids
