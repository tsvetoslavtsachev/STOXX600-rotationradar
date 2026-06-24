"""
Universe Screener — пълни метрики за всички акции.

От 6 години дневни adj close цени изчислява:
  Returns:    1M, 3M, 6M, YTD, 1Y, 3Y, 5Y
  Risk:       Vol 1Y/3Y, Sharpe 1Y/3Y, MaxDD 1Y/3Y/5Y, Calmar 3Y
  Position:   52w high distance, 52w low distance, days since 52w high
  Beta:       trailing 1Y vs equal-weight STOXX 600 benchmark

За STOXX 600 — размерът се определя от ETF weight % (iShares EXSA holdings),
не от market cap (избягва multi-currency проблеми).

Метричните функции (returns/risk/position/beta + size_bucket) идват от ВЕНДОРНАТИЯ
shared core `screener_core.py` (байт-идентичен със SP500-rotationradar). Тук остава
само EU-специфичното: ETF-weight праговете + build_screener изходната схема.
"""

from __future__ import annotations

import pandas as pd

# DAYS_1Y/3Y/5Y и _max_drawdown се внасят и за tests/test_screener.py, който ги
# импортва от src.screener (re-export на споделеното ядро).
from src.screener_core import (
    DAYS_1M,
    DAYS_1Y,
    DAYS_3Y,
    DAYS_5Y,
    _max_drawdown,
    compute_betas,
    compute_position_metrics,
    compute_returns,
    compute_risk_metrics,
    size_bucket,
)

# ETF weight % thresholds — proxy за size buckets. Праговете са EU-специфични —
# SP500 ползва market-cap прагове (виж SP500 screener.py); bucket логиката е обща.
LARGE_WEIGHT_THRESHOLD = 1.0   # ≥ 1% от ETF
MID_WEIGHT_THRESHOLD = 0.3     # ≥ 0.3%


def _size_bucket_from_weight(weight_pct: float | None) -> str | None:
    """EU wrapper: ETF weight % → size bucket чрез споделения size_bucket."""
    return size_bucket(weight_pct, LARGE_WEIGHT_THRESHOLD, MID_WEIGHT_THRESHOLD)


def build_screener(
    prices_df: pd.DataFrame,
    sector_map: dict[str, str] | None = None,
    industry_map: dict[str, str] | None = None,
    name_map: dict[str, str] | None = None,
    weights: dict[str, float] | None = None,
    country_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    Изгражда screener DataFrame с по един ред на акция и пълни метрики.

    weights: ticker → ETF weight % (proxy за размер).
    country_map: ticker → country (Standort от iShares).
    """
    sector_map = sector_map or {}
    industry_map = industry_map or {}
    name_map = name_map or {}
    weights = weights or {}
    country_map = country_map or {}

    betas = compute_betas(prices_df)

    rows = []
    for ticker in prices_df.columns:
        prices = prices_df[ticker].dropna()
        if len(prices) < DAYS_1M + 1:
            continue
        weight = weights.get(ticker)
        row = {
            "ticker": ticker,
            "name": name_map.get(ticker),
            "sector": sector_map.get(ticker),
            "industry": industry_map.get(ticker),
            "country": country_map.get(ticker),
            "etf_weight_pct": weight,
            "size_bucket": _size_bucket_from_weight(weight),
            "beta_1y": float(betas[ticker]) if ticker in betas.index else None,
        }
        row.update(compute_returns(prices))
        row.update(compute_risk_metrics(prices))
        row.update(compute_position_metrics(prices))
        rows.append(row)

    return pd.DataFrame(rows)
