"""Unit tests за signal_engine V2: pure 12-1 + sector z-score."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.signal_engine import (
    MIN_HISTORY_DAYS,
    MOM_12M_DAYS,
    SKIP_DAYS,
    compute_cross_section,
    compute_ticker_mom,
)


def _trading_days(n: int, end="2024-12-31") -> pd.DatetimeIndex:
    return pd.bdate_range(end=end, periods=n)


def _constant_growth(daily_pct: float, n: int) -> pd.Series:
    idx = _trading_days(n)
    base = 100.0 * (1.0 + daily_pct) ** np.arange(n)
    return pd.Series(base, index=idx)


def test_insufficient_history_returns_nan():
    short = _constant_growth(0.001, 100)
    assert np.isnan(compute_ticker_mom(short))


def test_skip_window_excludes_last_21_days():
    """
    Цена расте 12 месеца, после рязко пада последните 21 дни.
    12-1 momentum трябва да остане положителен (skip-ва падането).
    """
    n = MIN_HISTORY_DAYS + 5
    idx = _trading_days(n)
    prices = pd.Series(100.0 * (1.005 ** np.arange(n)), index=idx)
    prices.iloc[-SKIP_DAYS:] = prices.iloc[-SKIP_DAYS - 1] * 0.5

    mom = compute_ticker_mom(prices)
    assert mom > 0, "12-1 трябва да е положителен — skip пропуска crash-а"


def test_mom_periods_use_correct_lookbacks():
    """
    Цена = 100 до ден -253, скача на 110 от ден -252 нататък.
    12-1 чете start = price[-253] = 100, end = price[-22] = 110 → 10%.
    """
    n = MIN_HISTORY_DAYS + 5
    idx = _trading_days(n)
    prices = pd.Series(100.0, index=idx)
    prices.iloc[-MOM_12M_DAYS:] = 110.0

    mom = compute_ticker_mom(prices)
    assert mom == pytest.approx(0.10, abs=0.01)


def test_sector_zscore_normalizes_within_sector():
    """
    В Energy сектора: 3 акции с 12m моментум 30%, 25%, 20% (mean=25%).
    В Tech сектора: 3 акции с 12m моментум 50%, 45%, 40% (mean=45%).
    Top performer-ите в двата сектора трябва да получат еднаков ZSCORE
    (тъй като всеки е 1σ над неговия секторен mean), въпреки че абсолютните им returns са различни.
    """
    n = MIN_HISTORY_DAYS + 5
    idx = _trading_days(n)

    def make_constant_return(total_ret: float) -> pd.Series:
        # Прост price path: lock-step growth до day -22, без последния месец
        prices = pd.Series(100.0, index=idx)
        prices.iloc[-MOM_12M_DAYS:] = 100.0 * (1.0 + total_ret)
        return prices

    df = pd.DataFrame({
        "ENERGY1": make_constant_return(0.30),
        "ENERGY2": make_constant_return(0.25),
        "ENERGY3": make_constant_return(0.20),
        "TECH1": make_constant_return(0.50),
        "TECH2": make_constant_return(0.45),
        "TECH3": make_constant_return(0.40),
    })
    sector_map = {
        "ENERGY1": "Energy", "ENERGY2": "Energy", "ENERGY3": "Energy",
        "TECH1": "Tech", "TECH2": "Tech", "TECH3": "Tech",
    }

    cs = compute_cross_section(df, sector_map=sector_map).set_index("ticker")

    # Top in Energy и top in Tech трябва да имат СЪЩИЯ z-score (~+1.22)
    assert cs.loc["ENERGY1", "sector_zscore"] == pytest.approx(
        cs.loc["TECH1", "sector_zscore"], abs=0.001
    )
    # Bottom-ите също
    assert cs.loc["ENERGY3", "sector_zscore"] == pytest.approx(
        cs.loc["TECH3", "sector_zscore"], abs=0.001
    )
    # Top > middle > bottom вътре в сектора
    assert cs.loc["ENERGY1", "sector_zscore"] > cs.loc["ENERGY2", "sector_zscore"] > cs.loc["ENERGY3", "sector_zscore"]


def test_global_zscore_when_no_sector_map():
    """
    Без sector_map всички ticker-и са в "Universe" → global z-score.
    """
    n = MIN_HISTORY_DAYS + 5
    idx = _trading_days(n)
    prices = pd.Series(100.0 * (1.001 ** np.arange(n)), index=idx)

    df = pd.DataFrame({
        "A": prices,
        "B": prices * 0.95,
        "C": prices * 1.05,
    })

    cs = compute_cross_section(df).set_index("ticker")
    # Без sector_map → всички в един "Universe" сектор → z-score-ите трябва да сумират към 0
    assert cs["sector_zscore"].sum() == pytest.approx(0.0, abs=1e-9)


def test_percentile_ranks_are_normalized():
    n = MIN_HISTORY_DAYS + 5
    idx = _trading_days(n)
    rng = np.random.default_rng(7)
    n_tickers = 30

    df = pd.DataFrame(index=idx)
    for i in range(n_tickers):
        drift = rng.uniform(-0.001, 0.002)
        vol = rng.uniform(0.005, 0.03)
        rets = rng.normal(drift, vol, n)
        df[f"T{i}"] = 100.0 * np.exp(rets.cumsum())

    cs = compute_cross_section(df)
    valid = cs.dropna(subset=["percentile_rank"])
    assert (valid["percentile_rank"] >= 0).all()
    assert (valid["percentile_rank"] <= 100).all()
    assert valid["percentile_rank"].max() > 90
    assert valid["percentile_rank"].min() < 20


def test_cross_section_at_specific_date():
    n = MIN_HISTORY_DAYS + 60
    idx = _trading_days(n)
    prices = pd.Series(100.0 * (1.001 ** np.arange(n)), index=idx)
    df = pd.DataFrame({"A": prices, "B": prices * 0.99})

    mid_date = idx[-30]
    cs = compute_cross_section(df, as_of=mid_date)
    assert cs["date"].iloc[0] == mid_date


def test_unknown_sector_handled_gracefully():
    """
    Tickers без sector mapping → попадат в "Universe" сектор по default.
    """
    n = MIN_HISTORY_DAYS + 5
    idx = _trading_days(n)
    prices = pd.Series(100.0 * (1.001 ** np.arange(n)), index=idx)

    df = pd.DataFrame({"KNOWN": prices, "UNKNOWN": prices * 1.1})
    sector_map = {"KNOWN": "Tech"}  # UNKNOWN не е в map-а

    cs = compute_cross_section(df, sector_map=sector_map)
    sectors_present = set(cs["sector"])
    assert "Tech" in sectors_present
    assert "Universe" in sectors_present
