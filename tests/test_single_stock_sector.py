"""
Regression test for the single-stock-sector z-score trap.

Sector-relative momentum is undefined for a sector with one member (std is NaN).
The old code returned a flat 0.0 → the lone stock always ranked ~neutral (p50),
burying its real momentum. The fix falls back to a UNIVERSE-relative z-score so
the signal still registers.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.signal_engine import MIN_HISTORY_DAYS, compute_cross_section


def _trading_days(n: int, end: str = "2024-12-31") -> pd.DatetimeIndex:
    return pd.bdate_range(end=end, periods=n)


def _ramp(n: int, drift: float, base: float = 100.0) -> np.ndarray:
    """Exponential price path; higher drift → higher 12-1 momentum."""
    return base * (1.0 + drift) ** np.arange(n)


def _universe() -> tuple[pd.DataFrame, dict[str, str]]:
    n = MIN_HISTORY_DAYS + 40
    idx = _trading_days(n)
    data = {
        "A": _ramp(n, 0.0010), "B": _ramp(n, 0.0015), "C": _ramp(n, 0.0020),
        "D": _ramp(n, 0.0025), "E": _ramp(n, 0.0030),   # multi-stock sector "Big"
        "LONE": _ramp(n, 0.0060),                        # single-stock sector, extreme momentum
    }
    prices = pd.DataFrame(data, index=idx)
    sector_map = {"A": "Big", "B": "Big", "C": "Big", "D": "Big", "E": "Big", "LONE": "Solo"}
    return prices, sector_map


def test_single_stock_sector_uses_universe_zscore_not_zero():
    prices, sector_map = _universe()
    cs = compute_cross_section(prices, sector_map=sector_map).set_index("ticker")

    lone_z = cs.loc["LONE", "sector_zscore"]
    assert lone_z != 0.0                       # NOT the old neutral fallback
    assert lone_z > 1.0                         # extreme momentum → high universe z
    assert cs.loc["LONE", "percentile_rank"] > 50.0   # ranks high, not the middle


def test_multi_stock_sector_still_standardized_within_sector():
    """Regression guard: the std>0 path is unchanged — within-sector mean ≈ 0."""
    prices, sector_map = _universe()
    cs = compute_cross_section(prices, sector_map=sector_map).set_index("ticker")

    big_z = cs.loc[["A", "B", "C", "D", "E"], "sector_zscore"]
    assert abs(float(big_z.mean())) < 1e-9
