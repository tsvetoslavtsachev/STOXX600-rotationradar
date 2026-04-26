"""
Sector Context Engine — GICS Sector за всяка акция.

Source: iShares EXSA ETF holdings CSV via universe.fetch_current_constituents.
Sub-industry не идва от iShares CSV; имитирано със sector като fallback.
Cache: data/sector_map.json — обновяваме рядко.

Aggregation:
  - Sector heatmap: средна ΔRank по сектор
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.universe import fetch_current_constituents

DEFAULT_CACHE_TTL_DAYS = 90  # Refresh ~1×/тримесечие


def load_sector_map(cache_path: Path) -> dict:
    """Зарежда cached sector mapping. Връща dict с metadata."""
    if not cache_path.exists():
        return {"updated": None, "tickers": {}}
    with cache_path.open(encoding="utf-8") as f:
        return json.load(f)


def is_cache_fresh(cache: dict, ttl_days: int = DEFAULT_CACHE_TTL_DAYS) -> bool:
    if not cache.get("updated"):
        return False
    try:
        updated = datetime.fromisoformat(cache["updated"])
    except (ValueError, TypeError):
        return False
    return datetime.now() - updated < timedelta(days=ttl_days)


def refresh_sector_map(cache_path: Path) -> dict:
    """
    Изтегля fresh sector mapping от Wikipedia, презаписва cache-а.
    """
    df = fetch_current_constituents()
    mapping = {
        row["ticker"]: {
            "name": row["name"],
            "gics_sector": row["gics_sector"],
            "gics_sub_industry": row["gics_sub_industry"],
        }
        for _, row in df.iterrows()
    }
    cache = {
        "updated": datetime.now().isoformat(timespec="seconds"),
        "tickers": mapping,
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    return cache


def get_sector_dataframe(cache_path: Path, force_refresh: bool = False) -> pd.DataFrame:
    """Връща DataFrame: ticker, name, gics_sector, gics_sub_industry."""
    cache = load_sector_map(cache_path)
    if force_refresh or not is_cache_fresh(cache):
        cache = refresh_sector_map(cache_path)

    rows = [
        {"ticker": tk, **info}
        for tk, info in cache["tickers"].items()
    ]
    return pd.DataFrame(rows)


def aggregate_by_sector(
    deltas: pd.DataFrame,
    sectors: pd.DataFrame,
) -> pd.DataFrame:
    """
    Агрегира ΔRank по GICS Sector.
    Връща: gics_sector, mean_delta_1m, mean_delta_3m, n_risers, n_decayers, n_total.
    """
    merged = deltas.merge(sectors[["ticker", "gics_sector"]], on="ticker", how="left")
    merged = merged.dropna(subset=["gics_sector"])
    if merged.empty:
        return pd.DataFrame()

    grouped = merged.groupby("gics_sector").agg(
        mean_delta_1m=("delta_1m", "mean"),
        mean_delta_3m=("delta_3m", "mean"),
        n_total=("ticker", "count"),
        n_risers=("quadrant_1m", lambda s: (s == "riser").sum()),
        n_decayers=("quadrant_1m", lambda s: (s == "decayer").sum()),
    )
    return grouped.reset_index().sort_values("mean_delta_1m", ascending=False)


def aggregate_by_sub_industry(
    deltas: pd.DataFrame,
    sectors: pd.DataFrame,
    min_size: int = 3,
) -> pd.DataFrame:
    """
    Агрегира ΔRank по GICS Sub-Industry.
    Изключва sub-industries с < min_size компании (за да избегнем шум).
    """
    merged = deltas.merge(
        sectors[["ticker", "gics_sector", "gics_sub_industry"]],
        on="ticker",
        how="left",
    )
    merged = merged.dropna(subset=["gics_sub_industry"])
    if merged.empty:
        return pd.DataFrame()

    grouped = merged.groupby(["gics_sector", "gics_sub_industry"]).agg(
        mean_delta_1m=("delta_1m", "mean"),
        mean_delta_3m=("delta_3m", "mean"),
        n_total=("ticker", "count"),
    )
    grouped = grouped[grouped["n_total"] >= min_size]
    return grouped.reset_index().sort_values("mean_delta_1m", ascending=False)
