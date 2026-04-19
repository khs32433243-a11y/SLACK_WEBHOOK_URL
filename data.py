"""Data access layer — Excel version.

Reads from a weekly Excel export dropped into data/latest.xlsx.

This is the MVP path: no DB, no network, no secrets. You export the xlsx from
your dashboard each week, commit it to the repo, and push. GitHub Actions
picks up the push, runs the pipeline, and posts to Slack.

When you later want direct Postgres access: swap fetch_raw_weeks() for a
psycopg2 query that returns the same DataFrame shape. aggregate_week() and
aggregate_stores() are source-agnostic — they work on any DataFrame with the
expected columns.
"""

from __future__ import annotations
import re
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from decompose import WeekSnapshot, StoreSnapshot


EXPECTED_STORES = {'강남점', '강동점', '강북점', '관악점', '금천점',
                   '송파점', '양천점', '영등포점', '은평점', '중랑점'}

# Columns we need from the Excel export. If the dashboard export schema
# changes, update these to match.
REQUIRED_COLS = [
    '지점', '날짜', '총매출',
    '총플랫폼비(광고비,쿠폰비포함)',
    '총원자재비(BOM기준)', '총인건비',
    '총광고비', '총쿠폰비',
]


# ---------- helpers ----------

def _parse_won(v):
    """'1,234,567원' → 1234567. Handles NaN, empty, and already-numeric."""
    if pd.isna(v):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    digits = re.sub(r'[^\d-]', '', str(v))
    return int(digits) if digits else 0


def week_bounds(iso_year: int, iso_week: int) -> tuple[date, date]:
    jan4 = date(iso_year, 1, 4)
    mon = jan4 - timedelta(days=jan4.weekday())
    monday = mon + timedelta(weeks=iso_week - 1)
    return monday, monday + timedelta(days=6)


def previous_iso_week(reference: date, offset_weeks: int = 1) -> tuple[int, int]:
    y, w, _ = (reference - timedelta(weeks=offset_weeks)).isocalendar()
    return y, w


# ---------- raw fetch ----------

def fetch_raw_weeks(path: str | Path, weeks: list[tuple[int, int]]) -> pd.DataFrame:
    """Load Excel and return cleaned, week-filtered DataFrame.

    Expects sheet '시트1' with headers in row 1 and data starting row 2.
    Money columns are strings like '1,234,567원' — parsed to int.
    Drops 전체합계 and any other non-store rows.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"데이터 파일 없음: {path}")

    df = pd.read_excel(path, sheet_name='시트1', header=0)

    # Keep only valid store rows (drops 전체합계 and junk at bottom of file)
    df = df[df['지점'].isin(EXPECTED_STORES)].copy()

    # Validate required columns exist before parsing
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"엑셀에 필수 컬럼 누락: {missing}")

    df['날짜'] = pd.to_datetime(df['날짜'])
    for c in df.columns:
        if c not in ['지점', '날짜']:
            df[c] = df[c].apply(_parse_won)

    iso = df['날짜'].dt.isocalendar()
    df['iso_year'] = iso['year']
    df['iso_week'] = iso['week']

    # Filter to requested weeks only
    if weeks:
        wanted = {(int(y), int(w)) for y, w in weeks}
        mask = df.apply(lambda r: (int(r['iso_year']), int(r['iso_week'])) in wanted,
                        axis=1)
        df = df[mask].copy()

    return df


# ---------- aggregation ----------

def aggregate_week(df: pd.DataFrame, iso_year: int, iso_week: int,
                   exclude_kangbuk: bool = True) -> WeekSnapshot:
    mask = (df['iso_year'] == iso_year) & (df['iso_week'] == iso_week)
    if exclude_kangbuk:
        mask &= df['지점'] != '강북점'
    s = df[mask]
    rev = s['총매출'].sum()
    if rev == 0:
        raise ValueError(f"{iso_year}-W{iso_week:02d} 매출 합계 0")

    plat = s['총플랫폼비(광고비,쿠폰비포함)'].sum()
    mat = s['총원자재비(BOM기준)'].sum()
    labor = s['총인건비'].sum()
    ad = s['총광고비'].sum()
    cp = s['총쿠폰비'].sum()

    return WeekSnapshot(
        week=f"{iso_year}-W{iso_week:02d}",
        revenue=float(rev),
        platform_ratio=float(plat / rev * 100),
        material_ratio=float(mat / rev * 100),
        labor_ratio=float(labor / rev * 100),
        platform_fee_ratio=float((plat - ad - cp) / rev * 100),
        ad_ratio=float(ad / rev * 100),
        coupon_ratio=float(cp / rev * 100),
        op_ratio=float((rev - plat - mat - labor) / rev * 100),
    )


def aggregate_stores(df: pd.DataFrame, iso_year: int, iso_week: int) -> list[StoreSnapshot]:
    mask = (df['iso_year'] == iso_year) & (df['iso_week'] == iso_week)
    out: list[StoreSnapshot] = []
    for store, s in df[mask].groupby('지점'):
        rev = s['총매출'].sum()
        if rev == 0:
            continue
        plat = s['총플랫폼비(광고비,쿠폰비포함)'].sum()
        mat = s['총원자재비(BOM기준)'].sum()
        labor = s['총인건비'].sum()
        ad = s['총광고비'].sum()
        cp = s['총쿠폰비'].sum()
        out.append(StoreSnapshot(
            store=store,
            week=f"{iso_year}-W{iso_week:02d}",
            revenue=float(rev),
            platform_ratio=float(plat / rev * 100),
            material_ratio=float(mat / rev * 100),
            labor_ratio=float(labor / rev * 100),
            platform_fee_ratio=float((plat - ad - cp) / rev * 100),
            ad_ratio=float(ad / rev * 100),
            coupon_ratio=float(cp / rev * 100),
            op_ratio=float((rev - plat - mat - labor) / rev * 100),
        ))
    return out
