"""Data access layer.

Fetches raw daily × store rows from PostgreSQL. All aggregation happens in
Python so the quality gate can inspect the raw data first.

NOTE: column names below are placeholders. Inspect your
`public.daily_order_aggregate` and rename the SELECT list to match.
"""

from __future__ import annotations
from datetime import date, timedelta
import pandas as pd

from decompose import WeekSnapshot, StoreSnapshot


# ---------- helpers ----------

def week_bounds(iso_year: int, iso_week: int) -> tuple[date, date]:
    jan4 = date(iso_year, 1, 4)
    mon = jan4 - timedelta(days=jan4.weekday())
    monday = mon + timedelta(weeks=iso_week - 1)
    return monday, monday + timedelta(days=6)


def previous_iso_week(reference: date, offset_weeks: int = 1) -> tuple[int, int]:
    y, w, _ = (reference - timedelta(weeks=offset_weeks)).isocalendar()
    return y, w


# ---------- raw fetch ----------

_RAW_SQL = """
select
    order_date                as "날짜",
    store_name                as "지점",
    revenue                   as "총매출",
    platform_cost_total       as "총플랫폼비(광고비,쿠폰비포함)",
    material_cost_bom         as "총원자재비(BOM기준)",
    labor_cost                as "총인건비",
    ad_cost                   as "총광고비",
    coupon_cost               as "총쿠폰비"
from public.daily_order_aggregate
where order_date between %(start)s and %(end)s
"""


def fetch_raw_weeks(conn, weeks: list[tuple[int, int]]) -> pd.DataFrame:
    """Fetch raw daily rows for multiple ISO weeks in one query.
    Returns DataFrame with iso_year/iso_week columns pre-computed."""
    if not weeks:
        return pd.DataFrame()
    all_starts = [week_bounds(y, w)[0] for y, w in weeks]
    all_ends = [week_bounds(y, w)[1] for y, w in weeks]
    start, end = min(all_starts), max(all_ends)

    df = pd.read_sql(_RAW_SQL, conn, params={"start": start, "end": end})
    df['날짜'] = pd.to_datetime(df['날짜'])
    iso = df['날짜'].dt.isocalendar()
    df['iso_year'] = iso['year']
    df['iso_week'] = iso['week']
    return df


# ---------- aggregation (from raw DataFrame) ----------

def aggregate_week(df: pd.DataFrame, iso_year: int, iso_week: int,
                   exclude_kangbuk: bool = True) -> WeekSnapshot:
    mask = (df['iso_year'] == iso_year) & (df['iso_week'] == iso_week) & \
           (df['지점'] != '전체합계')
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
    fee = plat - ad - cp
    op = rev - plat - mat - labor
    return WeekSnapshot(
        week=f"{iso_year}-W{iso_week:02d}",
        revenue=float(rev),
        platform_ratio=float(plat / rev * 100),
        material_ratio=float(mat / rev * 100),
        labor_ratio=float(labor / rev * 100),
        platform_fee_ratio=float(fee / rev * 100),
        ad_ratio=float(ad / rev * 100),
        coupon_ratio=float(cp / rev * 100),
        op_ratio=float(op / rev * 100),
    )


def aggregate_stores(df: pd.DataFrame, iso_year: int, iso_week: int) -> list[StoreSnapshot]:
    mask = (df['iso_year'] == iso_year) & (df['iso_week'] == iso_week) & \
           (df['지점'] != '전체합계')
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
