# article_similarity_grouper.py

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

class ArticleSimilarityGrouper:
    def __init__(self, threshold, field_name=None):
        self.threshold = threshold
        self.field_name = field_name

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
            for j in range(i + 1, len(texts)):
                if sim_matrix[i, j] >= self.threshold:
                    # 여기에 print 추가
                    label = self.field_name or ""
                    print(f"[{label} 유사도 {sim_matrix[i, j]:.4f}] {i}번 <-> {j}번")
                    # print(f"[유사도 {sim_matrix[i, j]:.4f}] {i}번 <-> {j}번")
                    print(f"  [{i}] {texts[i][:50]}...")
                    print(f"  [{j}] {texts[j][:50]}...")
                    group_ids[j] = current_group

            current_group += 1

        return group_ids
