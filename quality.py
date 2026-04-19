"""Data quality gate.

Runs BEFORE decomposition. If critical issues are found, the pipeline aborts
and posts a quality-issue alert to Slack instead of generating a misleading
report.

Philosophy: one missing platform-cost day can shift the aggregate ratio by
several %p and trigger false conclusions. Better to skip a week than to
publish bad analysis.
"""

from __future__ import annotations
from dataclasses import dataclass
import pandas as pd


@dataclass
class QualityIssue:
    severity: str                  # "critical" | "warning"
    date: str | None
    store: str | None
    column: str | None
    message: str

    def format_line(self) -> str:
        parts = [self.date or "", self.store or "", self.column or "", self.message]
        return " · ".join(p for p in parts if p)


# Expected ratio ranges (fraction of 매출). Outside → likely data issue.
# Kangbuk is in BEP mode with wider bounds — checked separately.
RATIO_BOUNDS_NORMAL = {
    "총플랫폼비(광고비,쿠폰비포함)": (0.15, 0.50),
    "총원자재비(BOM기준)":           (0.20, 0.45),
    "총인건비":                      (0.10, 0.30),
}
RATIO_BOUNDS_KANGBUK = {
    "총플랫폼비(광고비,쿠폰비포함)": (0.15, 0.95),   # kangbuk runs higher
    "총원자재비(BOM기준)":           (0.20, 0.50),
    "총인건비":                      (0.08, 0.35),
}

EXPECTED_STORES = {'강남점','강동점','강북점','관악점','금천점',
                   '송파점','양천점','영등포점','은평점','중랑점'}


def check_week_quality(df: pd.DataFrame, iso_year: int, iso_week: int,
                       expected_days: int = 7) -> list[QualityIssue]:
    """Return list of issues for the given ISO week. Empty list = all clear.

    Assumes df has columns: 지점, 날짜, iso_year, iso_week, 총매출, plus the
    cost columns keyed in RATIO_BOUNDS_NORMAL.
    """
    issues: list[QualityIssue] = []

    mask = (df['iso_year'] == iso_year) & (df['iso_week'] == iso_week) & \
           (df['지점'] != '전체합계')
    wk = df[mask]

    # --- Check 1: data exists ---
    if len(wk) == 0:
        issues.append(QualityIssue("critical", None, None, None,
                                   f"W{iso_week} 데이터 없음"))
        return issues  # nothing more we can check

    # --- Check 2: row count (10 stores × 7 days = 70) ---
    expected = len(EXPECTED_STORES) * expected_days
    if len(wk) != expected:
        # Find which store × day combos are missing
        present = set(wk[['지점', '날짜']].apply(tuple, axis=1))
        all_dates = wk['날짜'].unique()
        missing = []
        for store in EXPECTED_STORES:
            for d in all_dates:
                if (store, d) not in present:
                    missing.append(f"{pd.Timestamp(d).strftime('%m/%d')} {store}")
        issues.append(QualityIssue(
            "warning", None, None, None,
            f"행 수 {len(wk)}/{expected}. 누락: {', '.join(missing[:5])}"
            + (f" (+{len(missing)-5}건 더)" if len(missing) > 5 else "")
        ))

    # --- Check 3: zero-revenue rows (almost always data error) ---
    zero_rev = wk[wk['총매출'] == 0]
    for _, row in zero_rev.iterrows():
        issues.append(QualityIssue(
            "critical",
            row['날짜'].strftime('%Y-%m-%d'),
            row['지점'], "총매출", "매출 0원"
        ))

    # --- Check 4: cost column anomalies (ratio out of bounds) ---
    for _, row in wk[wk['총매출'] > 0].iterrows():
        bounds = RATIO_BOUNDS_KANGBUK if row['지점'] == '강북점' else RATIO_BOUNDS_NORMAL
        for col, (lo, hi) in bounds.items():
            if col not in row.index:
                continue
            val = row[col]
            ratio = val / row['총매출']
            if val == 0:
                # Zero cost is almost always a data sync issue
                issues.append(QualityIssue(
                    "critical",
                    row['날짜'].strftime('%Y-%m-%d'),
                    row['지점'], col,
                    "0원 (정산 미반영 의심)"
                ))
            elif ratio < lo:
                issues.append(QualityIssue(
                    "warning",
                    row['날짜'].strftime('%Y-%m-%d'),
                    row['지점'], col,
                    f"{ratio*100:.1f}% (하한 {lo*100:.0f}% 미만, {val:,}원)"
                ))
            elif ratio > hi:
                issues.append(QualityIssue(
                    "warning",
                    row['날짜'].strftime('%Y-%m-%d'),
                    row['지점'], col,
                    f"{ratio*100:.1f}% (상한 {hi*100:.0f}% 초과, {val:,}원)"
                ))

    return issues


def format_issues_for_slack(issues: list[QualityIssue], week_label: str) -> str:
    """Build a Slack-friendly markdown summary of quality issues."""
    critical = [i for i in issues if i.severity == "critical"]
    warnings = [i for i in issues if i.severity == "warning"]

    lines = [f"*{week_label} 데이터 품질 게이트 실패*",
             f"critical {len(critical)}건, warning {len(warnings)}건", ""]

    if critical:
        lines.append("*Critical (분석 중단 사유)*")
        # Group by column for readability
        by_col: dict[str, list[QualityIssue]] = {}
        for issue in critical:
            by_col.setdefault(issue.column or "_", []).append(issue)
        for col, items in by_col.items():
            stores = sorted({f"{i.date} {i.store}" for i in items if i.store})
            lines.append(f"• `{col}` ({len(items)}건): {', '.join(stores[:4])}"
                        + (f" +{len(stores)-4}" if len(stores) > 4 else ""))
        lines.append("")

    if warnings and len(warnings) <= 10:
        lines.append(f"*Warning ({len(warnings)}건)*")
        for w in warnings[:10]:
            lines.append(f"• {w.format_line()}")
        lines.append("")
    elif warnings:
        lines.append(f"*Warning {len(warnings)}건* (상세는 archive/ 참조)")
        lines.append("")

    lines.append("→ 이번 주 분해 리포트 생성 *건너뜀*.")
    lines.append("→ 원인 파악 후 수동 재실행: Actions 탭에서 `Run workflow`")
    return "\n".join(lines)


def has_critical_issues(issues: list[QualityIssue]) -> bool:
    return any(i.severity == "critical" for i in issues)
