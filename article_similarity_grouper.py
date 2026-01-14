# article_similarity_grouper.py

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

class ArticleSimilarityGrouper:
    def __init__(self, threshold):
        self.threshold = threshold

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
                    group_ids[j] = current_group

            current_group += 1

        return group_ids
