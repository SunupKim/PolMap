
import pandas as pd
# scripts/lookup_general_issue.py
# 특정 이슈 클러스터(예: 5번)가 어떤 기사들로 구성되어 있는지 조회
# 실행 방법: py -m scripts.lookup_general_issue

def main():
    
    ISSUE_ID = 5

    articles = pd.read_csv("outputs/final/test_total_news_archive.csv")
    mapping = pd.read_csv("outputs/issue_clusters/article_map.csv")

    merged = articles.merge(mapping, on="news_id")
    subset = merged[merged["issue_cluster_id"] == ISSUE_ID]

    print(f"5번 클러스터 기사 수: {len(subset)}")
    print()
    print(subset["title"].to_string(index=False))

if __name__ == "__main__":    
    main()
    
