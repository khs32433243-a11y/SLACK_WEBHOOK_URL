"""Profit margin decomposition engine.

Pure functions. Input: weekly snapshots. Output: structured decomposition that
can be JSON-serialized and handed to the LLM narrative layer.

Reused across T1 (weekly), T2 (daily), later T3/T4 — same math, different
cadence. Keep this module free of I/O.
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Iterable


# ----------------------------- data models ------------------------------

@dataclass
class WeekSnapshot:
    """Aggregate metrics for one ISO week. All ratios in percent (e.g. 36.9)."""
    week: str                       # "2026-W14"
    revenue: float                  # total KRW for the week
    platform_ratio: float           # 플랫폼비(CPC 포함)
    material_ratio: float           # 원자재비(BOM)
    labor_ratio: float              # 인건비
    platform_fee_ratio: float       # 중개 + 결제 + 배달 (광고/쿠폰 제외)
    ad_ratio: float
    coupon_ratio: float
    op_ratio: float                 # 영업이익률

    def cost(self, ratio: float) -> float:
        """ratio (%) → KRW amount."""
        return ratio / 100 * self.revenue


@dataclass
class StoreSnapshot:
    store: str
    week: str
    revenue: float
    platform_ratio: float
    material_ratio: float
    labor_ratio: float
    platform_fee_ratio: float
    ad_ratio: float
    coupon_ratio: float
    op_ratio: float


# ----------------------------- L1: category ------------------------------

@dataclass
class L1Result:
    d_op: float
    platform_contrib: float
    material_contrib: float
    labor_contrib: float
    residual: float


def decompose_l1(prev: WeekSnapshot, curr: WeekSnapshot) -> L1Result:
    """ΔOP% ≈ -ΔP - ΔR - ΔL. Residual is rounding + unclassified."""
    d_op = curr.op_ratio - prev.op_ratio
    d_p = curr.platform_ratio - prev.platform_ratio
    d_r = curr.material_ratio - prev.material_ratio
    d_l = curr.labor_ratio - prev.labor_ratio
    return L1Result(
        d_op=d_op,
        platform_contrib=-d_p,
        material_contrib=-d_r,
        labor_contrib=-d_l,
        residual=d_op - (-d_p - d_r - d_l),
    )


# ----------------------------- L2: platform internal ------------------------------

@dataclass
class L2Result:
    d_platform: float
    fee_delta: float        # 중개/결제/배달
    ad_delta: float
    coupon_delta: float


def decompose_l2(prev: WeekSnapshot, curr: WeekSnapshot) -> L2Result:
    return L2Result(
        d_platform=curr.platform_ratio - prev.platform_ratio,
        fee_delta=curr.platform_fee_ratio - prev.platform_fee_ratio,
        ad_delta=curr.ad_ratio - prev.ad_ratio,
        coupon_delta=curr.coupon_ratio - prev.coupon_ratio,
    )


# ----------------------------- L3: volume vs rate ------------------------------

@dataclass
class L3Result:
    category: str
    d_cost_won: float
    numerator_effect: float     # 비용 자체 변동이 비율에 준 영향 (%p)
    denominator_effect: float   # 매출 변동이 비율에 준 영향 (%p)
    total_ratio_change: float
    op_contribution: float      # = -total_ratio_change


def decompose_l3(prev: WeekSnapshot, curr: WeekSnapshot) -> list[L3Result]:
    """Volume (denominator) vs Rate (numerator) decomposition per category."""
    d_rev = curr.revenue - prev.revenue
    out: list[L3Result] = []
    for name, r1_attr, r2_attr in [
        ("플랫폼비", "platform_ratio", "platform_ratio"),
        ("원자재비", "material_ratio", "material_ratio"),
        ("인건비", "labor_ratio", "labor_ratio"),
    ]:
        r1 = getattr(prev, r1_attr)
        r2 = getattr(curr, r2_attr)
        c1, c2 = prev.cost(r1), curr.cost(r2)
        d_cost = c2 - c1
        num = d_cost / prev.revenue * 100
        den = -r1 * d_rev / prev.revenue
        total = num + den
        out.append(L3Result(
            category=name,
            d_cost_won=d_cost,
            numerator_effect=num,
            denominator_effect=den,
            total_ratio_change=total,
            op_contribution=-total,
        ))
    return out


# ----------------------------- L4: store mix vs performance ------------------------------

@dataclass
class L4Result:
    store: str
    prev_weight: float          # % of total
    curr_weight: float
    prev_op: float              # %
    curr_op: float
    d_op: float                 # %p
    mix_effect: float           # %p contribution to company OP%
    perf_effect: float


def decompose_l4(prev_stores: Iterable[StoreSnapshot],
                 curr_stores: Iterable[StoreSnapshot]) -> list[L4Result]:
    prev_map = {s.store: s for s in prev_stores}
    curr_map = {s.store: s for s in curr_stores}
    stores = sorted(set(prev_map) & set(curr_map))
    total_prev = sum(prev_map[s].revenue for s in stores)
    total_curr = sum(curr_map[s].revenue for s in stores)
    out: list[L4Result] = []
    for s in stores:
        p, c = prev_map[s], curr_map[s]
        w1 = p.revenue / total_prev * 100
        w2 = c.revenue / total_curr * 100
        d_op = c.op_ratio - p.op_ratio
        mix = p.op_ratio * (w2 - w1) / 100
        perf = (w1 / 100) * d_op
        out.append(L4Result(
            store=s, prev_weight=w1, curr_weight=w2,
            prev_op=p.op_ratio, curr_op=c.op_ratio, d_op=d_op,
            mix_effect=mix, perf_effect=perf,
        ))
    # sort by perf_effect ascending (biggest drag first)
    return sorted(out, key=lambda r: r.perf_effect)


# ----------------------------- top-level ------------------------------

def full_decomposition(prev: WeekSnapshot, curr: WeekSnapshot,
                       prev_stores: Iterable[StoreSnapshot],
                       curr_stores: Iterable[StoreSnapshot]) -> dict:
    """Run all four levels and return a JSON-serializable dict for LLM input."""
    return {
        "prev_week": prev.week,
        "curr_week": curr.week,
        "aggregate": {"prev": asdict(prev), "curr": asdict(curr)},
        "l1": asdict(decompose_l1(prev, curr)),
        "l2": asdict(decompose_l2(prev, curr)),
        "l3": [asdict(r) for r in decompose_l3(prev, curr)],
        "l4": [asdict(r) for r in decompose_l4(prev_stores, curr_stores)],
    }
