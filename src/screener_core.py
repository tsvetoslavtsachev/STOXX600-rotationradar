"""
screener_core — shared metric core for the rotation radar twins.

VENDORED: this exact file is byte-identical in SP500-rotationradar and
STOXX600-rotationradar (synced from tsachev-ops rotation_core/). Do NOT edit a
single repo's copy — edit the canon, run sync_rotation_core.py, commit both twins.
See rotation-twins-diff-journal.md.

Holds the price-only screener math the two dashboards share verbatim:
  Returns:    1M, 3M, 6M, YTD, 1Y, 3Y, 5Y
  Risk:       Vol 1Y/3Y, Sharpe 1Y/3Y, MaxDD 1Y/3Y/5Y, Calmar 3Y
  Position:   52w high distance, 52w low distance, days since 52w high
  Beta:       trailing 1Y vs equal-weight cross-section benchmark
  size_bucket(value, large, mid): three-tier size bucket — the thresholds are the
                                  US/EU parameter (see each repo's screener.py).

Everything that differs between the US and EU pipelines — the size-bucket source
and thresholds, the output record schema (market_cap vs country/etf_weight_pct) —
stays in each repo's screener.py and is passed in as a parameter.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

DAYS_1M = 21
DAYS_3M = 63
DAYS_6M = 126
DAYS_1Y = 252
DAYS_3Y = 756
DAYS_5Y = 1260


def _safe_total_return(prices: pd.Series, lookback: int) -> float | None:
    """Total return в процент за последните `lookback` търговски дни."""
    if len(prices) <= lookback:
        return None
    end = prices.iloc[-1]
    start = prices.iloc[-1 - lookback]
    if not np.isfinite(start) or not np.isfinite(end) or start <= 0:
        return None
    return float((end / start - 1.0) * 100.0)


def _ytd_return(prices: pd.Series) -> float | None:
    """Year-to-date total return."""
    if prices.empty:
        return None
    last_date = prices.index[-1]
    year_start = pd.Timestamp(year=last_date.year, month=1, day=1)
    sub = prices[prices.index >= year_start]
    if len(sub) < 2:
        return None
    start = sub.iloc[0]
    end = sub.iloc[-1]
    if not np.isfinite(start) or not np.isfinite(end) or start <= 0:
        return None
    return float((end / start - 1.0) * 100.0)


def compute_returns(prices: pd.Series) -> dict:
    return {
        "ret_1m": _safe_total_return(prices, DAYS_1M),
        "ret_3m": _safe_total_return(prices, DAYS_3M),
        "ret_6m": _safe_total_return(prices, DAYS_6M),
        "ret_ytd": _ytd_return(prices),
        "ret_1y": _safe_total_return(prices, DAYS_1Y),
        "ret_3y": _safe_total_return(prices, DAYS_3Y),
        "ret_5y": _safe_total_return(prices, DAYS_5Y),
    }


def _annualized_vol(prices: pd.Series, window: int) -> float | None:
    if len(prices) < window + 1:
        return None
    sub = prices.iloc[-(window + 1):]
    log_returns = np.log(sub / sub.shift(1)).dropna()
    if len(log_returns) < window // 2:
        return None
    vol = float(log_returns.std() * np.sqrt(252) * 100.0)
    return vol if vol > 0 else None


def _annualized_return(prices: pd.Series, window: int) -> float | None:
    """CAGR-style annualized return от последните `window` търговски дни."""
    if len(prices) <= window:
        return None
    end = prices.iloc[-1]
    start = prices.iloc[-1 - window]
    if not np.isfinite(start) or not np.isfinite(end) or start <= 0:
        return None
    years = window / 252.0
    return float(((end / start) ** (1.0 / years) - 1.0) * 100.0)


def _max_drawdown(prices: pd.Series, window: int) -> float | None:
    """Max drawdown в проценти за последните `window` дни. Връща отрицателно число."""
    if len(prices) < window + 1:
        return None
    sub = prices.iloc[-window:]
    cummax = sub.cummax()
    dd = (sub / cummax - 1.0) * 100.0
    return float(dd.min())


def compute_risk_metrics(prices: pd.Series) -> dict:
    vol_1y = _annualized_vol(prices, DAYS_1Y)
    vol_3y = _annualized_vol(prices, DAYS_3Y)
    ann_ret_1y = _annualized_return(prices, DAYS_1Y)
    ann_ret_3y = _annualized_return(prices, DAYS_3Y)

    sharpe_1y = ann_ret_1y / vol_1y if (ann_ret_1y is not None and vol_1y) else None
    sharpe_3y = ann_ret_3y / vol_3y if (ann_ret_3y is not None and vol_3y) else None

    maxdd_3y = _max_drawdown(prices, DAYS_3Y)
    calmar_3y = (
        ann_ret_3y / abs(maxdd_3y)
        if (ann_ret_3y is not None and maxdd_3y is not None and maxdd_3y < 0)
        else None
    )

    return {
        "vol_1y": vol_1y,
        "vol_3y": vol_3y,
        "sharpe_1y": sharpe_1y,
        "sharpe_3y": sharpe_3y,
        "maxdd_1y": _max_drawdown(prices, DAYS_1Y),
        "maxdd_3y": maxdd_3y,
        "maxdd_5y": _max_drawdown(prices, DAYS_5Y),
        "calmar_3y": calmar_3y,
    }


def compute_position_metrics(prices: pd.Series) -> dict:
    if len(prices) < DAYS_1Y:
        return {
            "high_52w": None,
            "low_52w": None,
            "dist_52w_high": None,
            "dist_52w_low": None,
            "days_since_52w_high": None,
        }
    sub = prices.iloc[-DAYS_1Y:]
    high = sub.max()
    low = sub.min()
    last = prices.iloc[-1]
    high_idx = sub.idxmax()
    days_since_high = int((prices.index[-1] - high_idx).days)
    return {
        "high_52w": float(high),
        "low_52w": float(low),
        "dist_52w_high": float((last / high - 1.0) * 100.0) if high > 0 else None,
        "dist_52w_low": float((last / low - 1.0) * 100.0) if low > 0 else None,
        "days_since_52w_high": days_since_high,
    }


def compute_betas(prices_df: pd.DataFrame, window: int = DAYS_1Y) -> pd.Series:
    """
    Trailing `window`-day beta vs equal-weight cross-section benchmark.

    Benchmark = средна daily simple return на всички ticker-и в cross-section.
    Beta = cov(stock_returns, bench_returns) / var(bench_returns).
    """
    daily_returns = prices_df.pct_change().iloc[-window:]
    if len(daily_returns) < window // 2:
        return pd.Series(dtype=float, name="beta_1y")

    benchmark = daily_returns.mean(axis=1)
    bench_var = float(benchmark.var())
    if bench_var <= 0:
        return pd.Series(dtype=float, name="beta_1y")

    betas = {}
    for ticker in daily_returns.columns:
        rets = daily_returns[ticker].dropna()
        bench_aligned = benchmark.reindex(rets.index).dropna()
        rets = rets.reindex(bench_aligned.index).dropna()
        if len(rets) < window // 2:
            continue
        cov = float(np.cov(rets.values, bench_aligned.values, ddof=1)[0, 1])
        betas[ticker] = cov / bench_var

    return pd.Series(betas, name="beta_1y")


def size_bucket(value: float | None, large_threshold: float, mid_threshold: float) -> str | None:
    """Three-tier size bucket from a numeric size proxy. The thresholds are the
    US/EU parameter passed by each repo's screener.py: S&P 500 buckets a stock by
    market cap (≥50e9 Large, ≥10e9 Mid), STOXX 600 by its iShares EXSA weight %
    (≥1.0 Large, ≥0.3 Mid). The bucketing logic itself is identical, so it lives here.
    The `build_screener` row assembly (which output schema, which size source) stays
    local in each repo's screener.py."""
    if value is None or not np.isfinite(value) or value <= 0:
        return None
    if value >= large_threshold:
        return "Large"
    if value >= mid_threshold:
        return "Mid"
    return "Small"
