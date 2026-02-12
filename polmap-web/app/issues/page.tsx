import fs from "fs/promises";
import path from "path";

type IssueMeta = {
  issue_cluster_id: number;
  issue_label: string;
  cluster_size: number;
  representative_titles: string[];
};

async function getIssues() {
  const res = await fetch(
    "http://localhost:3000/issue_clusters/current/meta.json",
    { cache: "no-store" }
  );

  if (!res.ok) {
    throw new Error("Failed to load issue meta");
  }

  return res.json();
}


export default async function IssuesPage() {
  const issues = await getIssues();

  return (
    <main style={{ padding: 24 }}>
      <h1>이슈 목록</h1>
      <ul>
        {issues.map(issue => (
          <li key={issue.issue_cluster_id}>
            <strong>{issue.issue_label}</strong>
            <div>기사 수: {issue.cluster_size}</div>
            <ul>
              {issue.representative_titles.slice(0, 3).map((t, i) => (
                <li key={i}>{t}</li>
              ))}
            </ul>
          </li>
        ))}
      </ul>
    </main>
  );
}
