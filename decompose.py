from typing import List, Dict
from pydantic import BaseModel

class StoreSnapshot(BaseModel):
    store_name: str
    platform: str
    total_sales: float
    platform_fee: float
    ad_cost: float
    coupon_cost: float
    net_profit: float

class WeekSnapshot(BaseModel):
    week_label: str
    stores: List[StoreSnapshot]

    @property
    def total_revenue(self) -> float:
        return sum(s.total_sales for s in self.stores)

    @property
    def total_profit(self) -> float:
        return sum(s.net_profit for s in self.stores)

    @property
    def profit_margin(self) -> float:
        if self.total_revenue == 0: return 0
        return (self.total_profit / self.total_revenue) * 100

def full_decomposition(current: WeekSnapshot, target_margin: float = 13.0) -> Dict:
    """
    수익률 분석 핵심 함수 (이름을 main.py와 맞춤)
    """
    total_rev = current.total_revenue
    actual_margin = current.profit_margin
    total_ads = sum(s.ad_cost for s in current.stores)
    total_fees = sum(s.platform_fee for s in current.stores)
    total_coupons = sum(s.coupon_cost for s in current.stores)

    store_contributions = []
    for store in current.stores:
        impact = (store.net_profit / total_rev * 100) - (store.total_sales / total_rev * target_margin) if total_rev > 0 else 0
        store_contributions.append({
            "name": store.store_name,
            "platform": store.platform,
            "impact_on_margin": round(impact, 2),
            "actual_margin": round((store.net_profit / store.total_sales * 100), 2) if store.total_sales > 0 else 0
        })
    store_contributions.sort(key=lambda x: x["impact_on_margin"])

    return {
        "summary": {"week": current.week_label, "actual_margin": round(actual_margin, 2), "target_margin": target_margin},
        "cost_structure": {
            "ad_ratio": round(total_ads / total_rev * 100, 2) if total_rev > 0 else 0,
            "fee_ratio": round(total_fees / total_rev * 100, 2) if total_rev > 0 else 0,
            "coupon_ratio": round(total_coupons / total_rev * 100, 2) if total_rev > 0 else 0
        },
        "worst_stores": store_contributions[:3]
    }
