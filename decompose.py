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
    week: str
    revenue: float
    platform_ratio: float
    material_ratio: float
    labor_ratio: float
    platform_fee_ratio: float
    ad_ratio: float
    coupon_ratio: float
    op_ratio: float

    def cost(self, ratio: float) -> float:
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
    d_op = curr.op_ratio - prev.op_ratio
    d_p = curr.platform_ratio - prev.platform_ratio
    d_r = c
