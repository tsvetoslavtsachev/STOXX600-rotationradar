"""
Render — генерира data.json за UI dashboard-а.

V2 architecture: главният сигнал е Stable Winners (лидери продължаващи да водят),
вторичен е Quality Dip (лидери временно отслабнали), а Faded Bounces (бившите Risers)
са показани с warning защото backtest показа отрицателен excess.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.rank_history import (  # noqa: E402
    compute_delta_metrics,
    get_faded_bounces,
    get_quality_dip,
    get_stable_winners,
    load_history,
)
from src.screener import build_screener  # noqa: E402
from src.sector_engine import (  # noqa: E402
    aggregate_by_sector,
    get_sector_dataframe,
)
from src.signal_engine import compute_ticker_mom  # noqa: E402

DATA_DIR = ROOT / "data"
DOCS_DIR = ROOT / "docs"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
PRICES_CACHE_PATH = DATA_DIR / "prices_cache.parquet"
SECTOR_CACHE_PATH = DATA_DIR / "sector_map.json"
UNIVERSE_CACHE_PATH = DATA_DIR / "universe.parquet"
OUTPUT_PATH = DOCS_DIR / "data.json"
TRAJECTORY_DAYS = 90


def load_universe_metadata(path: Path = UNIVERSE_CACHE_PATH) -> tuple[dict, dict, dict, dict, dict, str | None]:
    """
    Зарежда cached universe (от iShares CSV).
    Връща: weights, countries, exchanges, currencies, names, updated_iso.
    """
    if not path.exists():
        return {}, {}, {}, {}, {}, None
    df = pd.read_parquet(path)
    weights = dict(zip(df["ticker"], df["weight_pct"]))
    countries = dict(zip(df["ticker"], df["country"]))
    exchanges = dict(zip(df["ticker"], df["exchange"]))
    currencies = dict(zip(df["ticker"], df["currency"]))
    names = dict(zip(df["ticker"], df["name"]))
    # Use file mtime as proxy for update time
    import datetime as _dt
    updated = _dt.datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    return weights, countries, exchanges, currencies, names, updated


def build_screener_payload() -> tuple[list[dict], str | None]:
    """Изгражда screener секцията: списък с stock dicts + universe_updated timestamp."""
    if not PRICES_CACHE_PATH.exists():
        return [], None

    prices = pd.read_parquet(PRICES_CACHE_PATH)
    prices.index = pd.to_datetime(prices.index)

    sectors = get_sector_dataframe(SECTOR_CACHE_PATH)
    sector_map = dict(zip(sectors["ticker"], sectors["gics_sector"]))
    industry_map = dict(zip(sectors["ticker"], sectors["gics_sub_industry"]))
    name_map = dict(zip(sectors["ticker"], sectors["name"]))

    weights, countries, _, _, _, universe_updated = load_universe_metadata()

    df = build_screener(
        prices,
        sector_map=sector_map,
        industry_map=industry_map,
        name_map=name_map,
        weights=weights,
        country_map=countries,
    )

    def _safe(v, ndigits=2):
        if v is None or (isinstance(v, float) and not np.isfinite(v)):
            return None
        if isinstance(v, float):
            return round(v, ndigits)
        return v

    stocks = []
    for _, row in df.iterrows():
        record = {col: _safe(row[col]) for col in df.columns}
        stocks.append(record)

    return stocks, universe_updated


def compute_current_returns(prices_cache_path: Path = PRICES_CACHE_PATH) -> dict[str, float]:
    """
    Връща dict ticker → 12-1 momentum return (актуален). Пример: 0.27 = +27%.
    Зарежда prices_cache.parquet и за всеки ticker пресмята класическия 12-1.
    """
    if not prices_cache_path.exists():
        return {}
    prices = pd.read_parquet(prices_cache_path)
    prices.index = pd.to_datetime(prices.index)
    out = {}
    for ticker in prices.columns:
        mom = compute_ticker_mom(prices[ticker])
        if np.isfinite(mom):
            out[ticker] = float(mom)
    return out


def _safe_round(x, ndigits: int = 1):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return None
    return round(float(x), ndigits)


def _safe_str(x):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return None
    return str(x) if not pd.isna(x) else None


def _row_to_dict(
    row: pd.Series,
    sectors: pd.DataFrame,
    returns: dict[str, float] | None = None,
) -> dict:
    sector_info = sectors[sectors["ticker"] == row["ticker"]]
    if sector_info.empty:
        name = sector = sub = None
    else:
        s = sector_info.iloc[0]
        name = _safe_str(s.get("name"))
        sector = _safe_str(s.get("gics_sector"))
        sub = _safe_str(s.get("gics_sub_industry"))

    mom_return = (returns or {}).get(row["ticker"])
    mom_return_pct = round(mom_return * 100, 1) if mom_return is not None else None

    return {
        "ticker": row["ticker"],
        "name": name,
        "sector": sector,
        "sub_industry": sub,
        "current_rank": _safe_round(row.get("current_rank")),
        "abs_strength": _safe_round(row.get("abs_strength")),
        "mom_12_1_pct": mom_return_pct,
        "base_rank_6m": _safe_round(row.get("base_rank_6m")),
        "delta_1m": _safe_round(row.get("delta_1m")),
        "delta_3m": _safe_round(row.get("delta_3m")),
        "quadrant_1m": row.get("quadrant_1m"),
        "quadrant_3m": row.get("quadrant_3m"),
    }


def _trajectory(history: pd.DataFrame, ticker: str, days: int = TRAJECTORY_DAYS) -> list:
    sub = history[history["ticker"] == ticker].sort_values("date").tail(days)
    return [
        {"date": d.strftime("%Y-%m-%d"), "rank": _safe_round(r)}
        for d, r in zip(sub["date"], sub["percentile_rank"])
    ]


def render_dashboard_data(
    history_path: Path = HISTORY_PATH,
    sector_cache_path: Path = SECTOR_CACHE_PATH,
    output_path: Path = OUTPUT_PATH,
    limit: int = 25,
) -> dict:
    history = load_history(history_path)
    if history.empty:
        raise RuntimeError(f"History file is empty: {history_path}")

    sectors = get_sector_dataframe(sector_cache_path)
    deltas = compute_delta_metrics(history)
    if deltas.empty:
        raise RuntimeError("Could not compute delta metrics — insufficient history")

    as_of = deltas["as_of_date"].iloc[0]
    returns = compute_current_returns()

    stable_winners_1m = get_stable_winners(deltas, window="1m", limit=limit)
    stable_winners_3m = get_stable_winners(deltas, window="3m", limit=limit)
    quality_dip_1m = get_quality_dip(deltas, window="1m", limit=limit)
    quality_dip_3m = get_quality_dip(deltas, window="3m", limit=limit)
    faded_bounces_1m = get_faded_bounces(deltas, window="1m", limit=limit)

    # Current Strength leaderboard — топ 50 акции по абсолютна 12-1 momentum
    current_strength = (
        deltas.dropna(subset=["abs_strength"])
        .sort_values("abs_strength", ascending=False)
        .head(50)
    )

    sector_agg = aggregate_by_sector(deltas, sectors)

    screener_stocks, universe_updated = build_screener_payload()

    # Rank All Stocks — пълно подреждане по Score (sector-relative percentile).
    # Score е V2 еквивалент на старото "weighted_return / vol + 0.3*sharpe".
    rank_all_df = (
        deltas.dropna(subset=["current_rank"])
        .sort_values("current_rank", ascending=False)
        .reset_index(drop=True)
    )
    rank_all_df["rank_position"] = rank_all_df.index + 1

    quadrant_label = {
        "riser": "Faded Bounce",
        "decayer": "Quality Dip",
        "stable_winner": "Stable Winner",
        "chronic_loser": "Chronic Loser",
        "neutral": "Neutral",
        "unknown": "—",
    }

    rank_all_payload = []
    for _, row in rank_all_df.iterrows():
        ticker = row["ticker"]
        sector_info = sectors[sectors["ticker"] == ticker]
        if sector_info.empty:
            name = sector = sub = None
        else:
            s = sector_info.iloc[0]
            name = _safe_str(s.get("name"))
            sector = _safe_str(s.get("gics_sector"))
            sub = _safe_str(s.get("gics_sub_industry"))

        mom = returns.get(ticker)
        rank_all_payload.append({
            "rank_position": int(row["rank_position"]),
            "ticker": ticker,
            "name": name,
            "sector": sector,
            "sub_industry": sub,
            "score": _safe_round(row.get("current_rank")),
            "abs_strength": _safe_round(row.get("abs_strength")),
            "mom_12_1_pct": round(mom * 100, 1) if mom is not None else None,
            "base_rank_6m": _safe_round(row.get("base_rank_6m")),
            "delta_1m": _safe_round(row.get("delta_1m")),
            "delta_3m": _safe_round(row.get("delta_3m")),
            "quadrant_1m": quadrant_label.get(row.get("quadrant_1m"), "—"),
            "quadrant_3m": quadrant_label.get(row.get("quadrant_3m"), "—"),
        })

    def _with_trajectory(df: pd.DataFrame) -> list:
        if df.empty:
            return []
        out = []
        for _, row in df.iterrows():
            d = _row_to_dict(row, sectors, returns=returns)
            d["trajectory"] = _trajectory(history, row["ticker"])
            out.append(d)
        return out

    payload = {
        "metadata": {
            "as_of": as_of.strftime("%Y-%m-%d"),
            "total_universe": int(deltas["ticker"].nunique()),
            "history_start": history["date"].min().strftime("%Y-%m-%d"),
            "history_end": history["date"].max().strftime("%Y-%m-%d"),
            "thresholds": {
                "high_base": 80,
                "low_base": 20,
            },
            "scoring": "pure 12-1 momentum + GICS-sector-relative z-score",
            "universe_source": "iShares EXSA UCITS ETF holdings (daily)",
            "universe_updated": universe_updated,
        },
        "stable_winners_1m": _with_trajectory(stable_winners_1m),
        "stable_winners_3m": _with_trajectory(stable_winners_3m),
        "quality_dip_1m": _with_trajectory(quality_dip_1m),
        "quality_dip_3m": _with_trajectory(quality_dip_3m),
        "faded_bounces_1m": _with_trajectory(faded_bounces_1m),
        "current_strength": _with_trajectory(current_strength),
        "rank_all_stocks": rank_all_payload,
        "screener": {
            "as_of": as_of.strftime("%Y-%m-%d"),
            "universe_updated": universe_updated,
            "stocks": screener_stocks,
        },
        "sector_rotation": [
            {
                "sector": row["gics_sector"],
                "mean_delta_1m": _safe_round(row["mean_delta_1m"], 2),
                "mean_delta_3m": _safe_round(row["mean_delta_3m"], 2),
                "n_total": int(row["n_total"]),
                "n_risers": int(row["n_risers"]),
                "n_decayers": int(row["n_decayers"]),
            }
            for _, row in sector_agg.iterrows()
        ],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return payload


if __name__ == "__main__":
    payload = render_dashboard_data()
    print(f"Wrote {OUTPUT_PATH}")
    print(f"  As of: {payload['metadata']['as_of']}")
    print(f"  Stable Winners 1m: {len(payload['stable_winners_1m'])}")
    print(f"  Stable Winners 3m: {len(payload['stable_winners_3m'])}")
    print(f"  Quality Dip 1m: {len(payload['quality_dip_1m'])}")
    print(f"  Quality Dip 3m: {len(payload['quality_dip_3m'])}")
    print(f"  Faded Bounces (warning): {len(payload['faded_bounces_1m'])}")
    print(f"  Current Strength: {len(payload['current_strength'])}")
    print(f"  Rank All Stocks: {len(payload['rank_all_stocks'])}")
    print(f"  Screener stocks: {len(payload['screener']['stocks'])}")
    print(f"  Sectors: {len(payload['sector_rotation'])}")
