"""
Signal Engine V2 — pure 12-1 momentum, sector-relative z-score.

Промени спрямо V1 (която беше hand-calibrated multi-period + vol-normalization):
  - Премахнати: 6-1 и 3-1 momentum компоненти. Само Jegadeesh-Titman 12-1.
  - Премахната vol-нормализация (defensive bias).
  - Добавена sector-relative z-score нормализация: рангирането става
    спрямо sector peer-ите, не глобално.

Backtest показа (виж scripts/backtest_v2.py):
  Stable Winners (висока база, продължава да се качва) → +3.34% excess fwd_63d
  Quality Dip (висока база, временна слабост)         → +1.72% excess fwd_63d
  Faded Bounces (ниска база, скорошен скок)           → -0.44% excess fwd_63d
  Chronic Losers                                       → ~neutral (+0.41%)

Тоест "купи лидерите" е реалния сигнал, не "хвани падналите ангели".
"""

from __future__ import annotations

import numpy as np
import pandas as pd

SKIP_DAYS = 21
MOM_12M_DAYS = 252

MIN_HISTORY_DAYS = MOM_12M_DAYS + 1


def _period_return(prices: pd.Series, lookback: int, skip: int) -> float:
    """
    Return от ден[t-lookback] до ден[t-skip].
    Skip-ва последните `skip` дни (стандартен 12-1 momentum).
    """
    if len(prices) <= lookback:
        return np.nan
    end = prices.iloc[-1 - skip]
    start = prices.iloc[-1 - lookback]
    if not np.isfinite(start) or not np.isfinite(end) or start <= 0:
        return np.nan
    return float(end / start - 1.0)


def compute_ticker_mom(prices: pd.Series) -> float:
    """
    Връща pure 12-1 momentum return за един ticker.
    NaN ако историята е недостатъчна.
    """
    prices = prices.dropna()
    if len(prices) < MIN_HISTORY_DAYS:
        return np.nan
    return _period_return(prices, MOM_12M_DAYS, SKIP_DAYS)


def compute_cross_section(
    prices_df: pd.DataFrame,
    sector_map: dict[str, str] | None = None,
    as_of: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Изчислява sector-relative scores за cross-section в дадена дата.

    prices_df: DataFrame с adj close, columns=tickers, index=date (sorted).
    sector_map: dict ticker → sector. Ако None, всички ticker-и са третирани
                като един общ "Universe" sector (т.е. global z-score).
    as_of: дата на която да пресметне scores. Default = последна дата.

    Връща DataFrame: ticker, mom_12_1, sector, sector_zscore,
    raw_score (= sector_zscore), percentile_rank, unadj_percentile, date.
    """
    if as_of is None:
        sliced = prices_df
    else:
        sliced = prices_df.loc[:as_of]

    if len(sliced) < MIN_HISTORY_DAYS:
        return pd.DataFrame(
            columns=[
                "date", "ticker", "mom_12_1", "sector", "sector_zscore",
                "raw_score", "percentile_rank", "unadj_percentile",
            ]
        )

    rows = []
    for ticker in sliced.columns:
        mom = compute_ticker_mom(sliced[ticker])
        if not np.isfinite(mom):
            continue
        sector = (sector_map or {}).get(ticker, "Universe")
        rows.append({"ticker": ticker, "mom_12_1": mom, "sector": sector})

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Sector-relative z-score. Single-stock sectors → z = 0 (neutral).
    df["sector_zscore"] = df.groupby("sector")["mom_12_1"].transform(
        lambda s: (s - s.mean()) / s.std() if s.std() > 0 else 0.0
    )

    df["raw_score"] = df["sector_zscore"]
    df["percentile_rank"] = df["sector_zscore"].rank(pct=True) * 100.0
    df["unadj_percentile"] = df["mom_12_1"].rank(pct=True) * 100.0
    df["date"] = sliced.index[-1]

    return df[
        [
            "date", "ticker", "mom_12_1", "sector", "sector_zscore",
            "raw_score", "percentile_rank", "unadj_percentile",
        ]
    ].reset_index(drop=True)
