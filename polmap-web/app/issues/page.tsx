// app/issues/page.tsx

import React from "react";
import Link from "next/link";

type IssueMeta = {
  issue_cluster_id: number;
  issue_label: string;
  cluster_size: number;
  representative_titles: string[];
};

async function getIssues(): Promise<IssueMeta[]> {
  const res = await fetch("http://localhost:3000/api/issues", {
    cache: "no-store",
  });

  if (!res.ok) {
    console.error("Failed to fetch issues");
    return [];
  }

  return res.json();
}

export default async function IssuesPage() {
  const issues = await getIssues();

  return (
    <div className="min-h-screen bg-gray-50 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-5xl mx-auto">
        <header className="mb-14 text-center">
          <h1 className="text-4xl font-extrabold text-gray-900 tracking-tight mb-3">
            실시간 정치 이슈 지도
          </h1>
          <p className="text-lg text-gray-600">
            AI가 클러스터링한 오늘의 핵심 정치 현안입니다.
          </p>
        </header>

        {issues.length === 0 ? (
          <div className="text-center p-20 bg-white rounded-2xl shadow-sm border border-gray-100">
            <p className="text-gray-500 text-xl">
              데이터가 아직 없습니다. 백엔드 분석을 실행해 주세요.
            </p>
          </div>
        ) : (
          <div className="grid gap-8 md:grid-cols-2">
            {issues.map((issue) => (
              <Link
                key={issue.issue_cluster_id}
                href={`/issues/${issue.issue_cluster_id}`}
                className="group bg-white rounded-2xl shadow-sm border border-gray-100 overflow-hidden hover:shadow-md hover:-translate-y-1 transition-all duration-300"
              >
                <div className="p-6 flex flex-col h-full">
                  {/* 상단 메타 */}
                  <div className="flex items-center justify-between mb-5">
                    <span className="text-xs font-semibold text-blue-600 bg-blue-50 px-3 py-1 rounded-full">
                      ID {issue.issue_cluster_id}
                    </span>
                    <span className="text-sm font-medium text-gray-500">
                      기사 {issue.cluster_size}건
                    </span>
                  </div>

                  {/* 이슈 제목 */}
                  <h2 className="text-xl font-bold text-gray-900 mb-5 leading-snug group-hover:text-blue-700 transition-colors">
                    {issue.issue_label}
                  </h2>

                  {/* 대표 기사 */}
                  <div className="flex-1">
                    <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">
                      대표 기사
                    </p>

                    <ul className="space-y-3">
                      {issue.representative_titles
                        .slice(0, 3)
                        .map((title) => (
                          <li
                            key={title}
                            className="text-sm text-gray-700 leading-relaxed line-clamp-2"
                          >
                            {title}
                          </li>
                        ))}
                    </ul>
                  </div>

                  {/* 하단 CTA */}
                  <div className="mt-6 pt-4 border-t border-gray-100 flex justify-end">
                    <span className="text-sm font-semibold text-blue-600 group-hover:text-blue-800 transition-colors">
                      상세보기 →
                    </span>
                  </div>
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
