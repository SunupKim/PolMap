import fs from "fs/promises";
import path from "path";
import { NextResponse } from "next/server";

export async function GET() {
  try {
    const filePath = path.join(
      process.cwd(),
      "..",
      "data",
      "issue_clusters",
      "current",
      "meta.json"
    );

    const raw = await fs.readFile(filePath, "utf-8");
    const parsed = JSON.parse(raw);

    // 🔹 여기서 필터링
    const filtered = parsed.filter(
      (issue: any) => issue.issue_label !== "[불가]"
    );

    return NextResponse.json(filtered);

  } catch (err) {
    return NextResponse.json(
      { error: "Failed to load issue meta" },
      { status: 500 }
    );
  }
}
