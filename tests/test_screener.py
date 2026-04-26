"""Unit tests за screener metrics."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.screener import (
    DAYS_1Y,
    DAYS_3Y,
    DAYS_5Y,
    _max_drawdown,
    _size_bucket_from_weight,
    build_screener,
    compute_betas,
    compute_position_metrics,
    compute_returns,
    compute_risk_metrics,
)


def _bdate_range(n: int, end="2026-04-24") -> pd.DatetimeIndex:
    return pd.bdate_range(end=end, periods=n)


def test_returns_known_double():
    """Цена удвоена за 1Y → ret_1y = 100."""
    n = DAYS_1Y + 5
    prices = pd.Series([100.0] * 5 + [200.0] * DAYS_1Y, index=_bdate_range(n))
    ret = compute_returns(prices)
    assert ret["ret_1y"] == pytest.approx(100.0, abs=0.5)


def test_max_drawdown_constant_growth_is_zero():
    """Постоянно растяща цена → MaxDD = 0."""
    n = DAYS_1Y + 5
    prices = pd.Series(100.0 * (1.0005 ** np.arange(n)), index=_bdate_range(n))
    dd = _max_drawdown(prices, DAYS_1Y)
    assert dd == pytest.approx(0.0, abs=0.01)


def test_max_drawdown_known_crash():
    """Цена 100 → 200 → 50: MaxDD ≈ -75% (от 200 до 50)."""
    n = DAYS_1Y + 5
    prices = pd.Series(
        [100.0] * 5 + [200.0] * (DAYS_1Y // 2) + [50.0] * (DAYS_1Y - DAYS_1Y // 2),
        index=_bdate_range(n),
    )
    dd = _max_drawdown(prices, DAYS_1Y)
    assert dd == pytest.approx(-75.0, abs=1.0)


def test_position_metrics_at_high():
    """Цена в 52w high → dist_52w_high ~= 0, days_since_52w_high == 0."""
    n = DAYS_1Y + 5
    rng = np.random.default_rng(7)
    prices = pd.Series(100.0 + rng.normal(0, 0.5, n).cumsum(), index=_bdate_range(n))
    prices.iloc[-1] = prices.iloc[-DAYS_1Y:-1].max() + 1.0  # last strictly > prior max
    pos = compute_position_metrics(prices)
    assert pos["dist_52w_high"] == pytest.approx(0.0, abs=0.5)
    assert pos["days_since_52w_high"] == 0


def test_beta_self_is_one():
    """Когато всички ticker-и имат идентична price path, beta vs benchmark = 1."""
    n = DAYS_1Y + 5
    rng = np.random.default_rng(42)
    rets = rng.normal(0.0005, 0.01, n)
    prices = 100.0 * np.exp(rets.cumsum())
    df = pd.DataFrame({"A": prices, "B": prices, "C": prices}, index=_bdate_range(n))
    betas = compute_betas(df)
    for t in ["A", "B", "C"]:
        assert betas[t] == pytest.approx(1.0, abs=1e-6)


def test_beta_high_vol_is_higher():
    """High-vol stock vs equal-weight benchmark с low-vol peers → beta > 1."""
    n = DAYS_1Y + 5
    idx = _bdate_range(n)
    rng = np.random.default_rng(1)

    market_factor = rng.normal(0.0005, 0.01, n)
    high_vol = market_factor * 2.0
    low_vol = market_factor * 0.5

    df = pd.DataFrame(
        {
            "HIGH": 100.0 * np.exp(high_vol.cumsum()),
            "LOW1": 100.0 * np.exp(low_vol.cumsum()),
            "LOW2": 100.0 * np.exp((low_vol + rng.normal(0, 0.001, n)).cumsum()),
        },
        index=idx,
    )
    betas = compute_betas(df)
    assert betas["HIGH"] > betas["LOW1"]
    assert betas["HIGH"] > 1.0


def test_size_bucket_thresholds():
    # ETF weight % → size bucket
    assert _size_bucket_from_weight(2.5) == "Large"
    assert _size_bucket_from_weight(1.0) == "Large"
    assert _size_bucket_from_weight(0.5) == "Mid"
    assert _size_bucket_from_weight(0.3) == "Mid"
    assert _size_bucket_from_weight(0.1) == "Small"
    assert _size_bucket_from_weight(None) is None
    assert _size_bucket_from_weight(0) is None


def test_risk_metrics_handles_short_history():
    """Акция с по-малко от 252 дни история → metrics = None."""
    short_prices = pd.Series([100.0] * 100, index=_bdate_range(100))
    risk = compute_risk_metrics(short_prices)
    assert risk["vol_1y"] is None
    assert risk["maxdd_1y"] is None
    assert risk["sharpe_1y"] is None


def test_build_screener_full_pipeline():
    """Sanity: build_screener на 5 ticker-а връща DataFrame с очакваните колони."""
    n = DAYS_1Y + 50
    idx = _bdate_range(n)
    rng = np.random.default_rng(3)
    prices_df = pd.DataFrame(
        {
            f"T{i}": 100.0 * np.exp(rng.normal(0.0003, 0.015, n).cumsum())
            for i in range(5)
        },
        index=idx,
    )
    sector_map = {f"T{i}": ("Tech" if i < 3 else "Health") for i in range(5)}
    weights = {f"T{i}": (2.0 if i < 2 else 0.1) for i in range(5)}

    df = build_screener(
        prices_df,
        sector_map=sector_map,
        weights=weights,
    )
    assert len(df) == 5
    expected_cols = {
        "ticker", "sector", "etf_weight_pct", "size_bucket",
        "ret_1m", "ret_1y", "vol_1y", "sharpe_1y", "maxdd_1y",
        "dist_52w_high", "beta_1y",
    }
    assert expected_cols.issubset(set(df.columns))
    # Size bucket sanity
    assert (df[df["ticker"].isin(["T0", "T1"])]["size_bucket"] == "Large").all()
    assert (df[df["ticker"].isin(["T2", "T3", "T4"])]["size_bucket"] == "Small").all()


def test_ytd_return_positive():
    """YTD return за непрекъснато растящ price е положителен."""
    n = DAYS_1Y + 50
    prices = pd.Series(100.0 * (1.001 ** np.arange(n)), index=_bdate_range(n))
    ret = compute_returns(prices)
    assert ret["ret_ytd"] is not None
    assert ret["ret_ytd"] > 0
