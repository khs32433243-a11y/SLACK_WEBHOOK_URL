"""Weekly profit margin report — orchestrator (Excel MVP version).

Reads data/latest.xlsx (committed weekly by the operator), runs the quality
gate, then the 4-level decomposition, then the LLM narrative, then posts to
Slack.

Env vars required:
    ANTHROPIC_API_KEY   — Claude API
    SLACK_WEBHOOK_URL   — Incoming Webhook
Optional:
    DATA_FILE           — path to xlsx (default: data/latest.xlsx)
"""

from __future__ import annotations
import json
import os
import sys
import traceback
from datetime import date
from pathlib import Path

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
DEFAULT_DATA = ROOT / "data" / "latest.xlsx"


def _pick_weeks(raw_df):
    """Pick the two most recent complete ISO weeks present in the file.

    Simpler than hardcoding to 'last week' — operator just drops the
    latest export and the pipeline figures out which weeks to report.
    """
    weeks = sorted({(int(y), int(w)) for y, w
                    in raw_df[['iso_year', 'iso_week']].values}, reverse=True)
    if len(weeks) < 2:
        raise ValueError(f"주차가 2개 미만: {weeks}. 전주 비교 불가.")
    return weeks[1], weeks[0]   # (prev, curr)


def run() -> None:
    data_path = Path(os.environ.get("DATA_FILE", DEFAULT_DATA))

    # --- 1. Load all weeks from the file (filter by recent only) ---
    full_df = fetch_raw_weeks(data_path, weeks=[])   # empty list = all weeks
    if full_df.empty:
        post_to_slack(
            title="⚠️ 데이터 없음",
            body=f"`{data_path}` 에서 유효한 지점 행을 찾을 수 없습니다.",
        )
        return

    (prev_year, prev_week), (curr_year, curr_week) = _pick_weeks(full_df)
    curr_label = f"{curr_year}-W{curr_week:02d}"
    prev_label = f"{prev_year}-W{prev_week:02d}"

    # --- 2. Quality gate (current week) ---
    issues = check_week_quality(full_df, curr_year, curr_week)
    if has_critical_issues(issues):
        post_to_slack(
            title=f"⚠️ {curr_label} 품질 게이트 실패 — 리포트 건너뜀",
            body=format_issues_for_slack(issues, curr_label),
        )
        ARCHIVE.mkdir(exist_ok=True)
        (ARCHIVE / f"{curr_label}_quality_fail.json").write_text(
            json.dumps([i.__dict__ for i in issues], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return

    # --- 3. Quality gate (prev week, for valid comparison) ---
    prev_issues = check_week_quality(full_df, prev_year, prev_week)
    if has_critical_issues(prev_issues):
        post_to_slack(
            title=f"⚠️ 전주({prev_label}) 품질 이슈 — {curr_label} 비교 불가",
            body=format_issues_for_slack(prev_issues, prev_label)
                 + "\n\n비교 기준이 되는 전주 데이터에 이슈가 있어 분해 불가.",
        )
        return

    warnings_note = ""
    if issues:
        warnings_note = (f"\n\n_데이터 품질 경고 {len(issues)}건. "
                        f"critical 아님 — 분해는 진행._")

    # --- 4. Aggregate + Decompose ---
    curr_agg = aggregate_week(full_df, curr_year, curr_week, exclude_kangbuk=True)
    prev_agg = aggregate_week(full_df, prev_year, prev_week, exclude_kangbuk=True)
    curr_stores = aggregate_stores(full_df, curr_year, curr_week)
    prev_stores = aggregate_stores(full_df, prev_year, prev_week)

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
