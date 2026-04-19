"""Weekly profit margin report — orchestrator.

Flow:
    1. Figure out the ISO week to report on (last completed week).
    2. Fetch raw daily × store rows for that week AND the prior week.
    3. **Quality gate** — inspect raw data. If critical issues, post alert
       and ABORT. If warnings only, proceed but flag in final report.
    4. Aggregate into weekly snapshots (company + per-store).
    5. Run 4-level decomposition (pure, no I/O).
    6. Archive the raw decomposition JSON for history.
    7. Hand to Claude → Korean markdown narrative.
    8. Publish to Slack.
"""

from __future__ import annotations
import json
import os
import sys
import traceback
from datetime import date
from pathlib import Path

import psycopg2

from data import (fetch_raw_weeks, aggregate_week, aggregate_stores,
                  previous_iso_week)
from decompose import full_decomposition
from quality import (check_week_quality, format_issues_for_slack,
                     has_critical_issues)
from narrative import generate_narrative
from publish import post_to_slack, post_failure_alert


ROOT = Path(__file__).resolve().parent
ARCHIVE = ROOT / "archive"
PROMPTS = ROOT / "prompts"


def run() -> None:
    today = date.today()
    curr_year, curr_week = previous_iso_week(today, offset_weeks=1)
    prev_year, prev_week = previous_iso_week(today, offset_weeks=2)
    curr_label = f"{curr_year}-W{curr_week:02d}"
    prev_label = f"{prev_year}-W{prev_week:02d}"

    # --- 1. Fetch raw (single query for both weeks) ---
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn:
        raw = fetch_raw_weeks(conn, [(prev_year, prev_week), (curr_year, curr_week)])

    if raw.empty:
        post_to_slack(
            title=f"⚠️ {curr_label} 데이터 없음",
            body="DB에서 raw 데이터를 찾을 수 없습니다. 수집 파이프라인 점검 요망.",
        )
        return

    # --- 2. Quality gate ---
    issues = check_week_quality(raw, curr_year, curr_week)
    if has_critical_issues(issues):
        post_to_slack(
            title=f"⚠️ {curr_label} 품질 게이트 실패 — 리포트 건너뜀",
            body=format_issues_for_slack(issues, curr_label),
        )
        # Also archive the issues for audit
        ARCHIVE.mkdir(exist_ok=True)
        (ARCHIVE / f"{curr_label}_quality_fail.json").write_text(
            json.dumps([i.__dict__ for i in issues], ensure_ascii=False, indent=2)
        )
        print(f"Critical issues found for {curr_label}. Aborting.")
        return

    # Also check prev week — if prev is bad, comparison is meaningless
    prev_issues = check_week_quality(raw, prev_year, prev_week)
    if has_critical_issues(prev_issues):
        post_to_slack(
            title=f"⚠️ 전주({prev_label}) 품질 이슈 — {curr_label} 비교 불가",
            body=format_issues_for_slack(prev_issues, prev_label)
                 + "\n\n비교 기준이 되는 전주 데이터에 이슈가 있어 분해 불가.",
        )
        return

    warnings_note = ""
    if issues:  # warnings only
        warnings_note = (f"\n\n*데이터 품질 경고 {len(issues)}건* "
                        f"(critical 아님, 분해는 진행)")

    # --- 3. Aggregate ---
    curr_agg = aggregate_week(raw, curr_year, curr_week, exclude_kangbuk=True)
    prev_agg = aggregate_week(raw, prev_year, prev_week, exclude_kangbuk=True)
    curr_stores = aggregate_stores(raw, curr_year, curr_week)
    prev_stores = aggregate_stores(raw, prev_year, prev_week)

    # --- 4. Decompose ---
    decomposition = full_decomposition(prev_agg, curr_agg, prev_stores, curr_stores)
    decomposition["quality_warnings"] = [i.__dict__ for i in issues]

    # --- 5. Archive ---
    ARCHIVE.mkdir(exist_ok=True)
    (ARCHIVE / f"{curr_label}.json").write_text(
        json.dumps(decomposition, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    # --- 6. LLM narrative ---
    narrative = generate_narrative(
        decomposition,
        prompt_template=PROMPTS / "weekly_report.md",
        brand_context=PROMPTS / "brand_manager.md",
    )

    # --- 7. Publish ---
    post_to_slack(
        title=f"주간 수익률 리포트 · {curr_label}",
        body=narrative + warnings_note,
    )


def main() -> int:
    try:
        run()
        return 0
    except Exception as e:
        traceback.print_exc()
        try:
            post_failure_alert(e)
        except Exception:
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
