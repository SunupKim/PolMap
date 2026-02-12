# scripts/view_issue_clusters.py
import os
import json
from pprint import pprint

ISSUE_CLUSTERS_ROOT = "outputs/issue_clusters"

def get_latest_run():
    runs = sorted(os.listdir(ISSUE_CLUSTERS_ROOT))
    if not runs:
        raise RuntimeError("이슈 클러스터 결과가 없습니다")
    return os.path.join(ISSUE_CLUSTERS_ROOT, runs[-1])

def main():
    latest_dir = get_latest_run()
    meta_path = os.path.join(latest_dir, "meta.json")

    with open(meta_path, "r", encoding="utf-8") as f:
        issue_meta = json.load(f)

    print(f"\n=== 이슈 판 ({os.path.basename(latest_dir)}) ===\n")

    for issue in issue_meta:
        print(f"[이슈 {issue['issue_cluster_id']}] {issue['issue_label']}")
        print(f"- 기사 수: {issue['cluster_size']}")

        for title in issue["representative_titles"]:
            print(f"  · {title}")

        print("-" * 60)

if __name__ == "__main__":
    main()
