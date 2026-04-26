"""Sanity tests за ΔRank Engine + quadrant класификация."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.rank_history import (
    HIGH_BASE_THRESHOLD,
    LOW_BASE_THRESHOLD,
    _classify_quadrant,
    append_snapshot,
    compute_delta_metrics,
    get_sustained_risers,
    get_top_decayers,
    get_top_risers,
    load_history,
)


def _build_history(ticker_paths: dict, dates: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Построява изкуствена история от ticker → list[(date_idx, percentile)].
    Дава raw_score == percentile_rank за простота.
    """
    rows = []
    for ticker, daily_pct in ticker_paths.items():
        for dt, p in zip(dates, daily_pct):
            rows.append({
                "date": dt,
                "ticker": ticker,
                "raw_score": p,
                "percentile_rank": p,
                "unadj_percentile": p,
            })
    return pd.DataFrame(rows)


def test_classify_quadrant_riser():
    assert _classify_quadrant(base=20.0, delta=15.0) == "riser"


def test_classify_quadrant_decayer():
    assert _classify_quadrant(base=85.0, delta=-20.0) == "decayer"


def test_classify_quadrant_stable_winner():
    assert _classify_quadrant(base=85.0, delta=5.0) == "stable_winner"


def test_classify_quadrant_chronic_loser():
    assert _classify_quadrant(base=20.0, delta=-5.0) == "chronic_loser"


def test_classify_quadrant_neutral_middle():
    assert _classify_quadrant(base=50.0, delta=10.0) == "neutral"


def test_classify_quadrant_handles_nan():
    assert _classify_quadrant(base=np.nan, delta=10.0) == "unknown"
    assert _classify_quadrant(base=20.0, delta=np.nan) == "unknown"


def test_compute_delta_metrics_basic_riser():
    """
    RISER: имаше rank ~20 6 месеца, скочи до 80 в последния месец.
    DECAYER: имаше rank ~85 6 месеца, падна до 25 в последния месец.
    STABLE: винаги ~50.
    """
    dates = pd.bdate_range(end="2024-12-31", periods=200)
    n = len(dates)

    riser_path = [20.0] * (n - 21) + [80.0] * 21
    decayer_path = [85.0] * (n - 21) + [25.0] * 21
    stable_path = [50.0] * n

    hist = _build_history(
        {"RISER": riser_path, "DECAY": decayer_path, "STABLE": stable_path},
        dates,
    )

    deltas = compute_delta_metrics(hist).set_index("ticker")

    assert deltas.loc["RISER", "quadrant_1m"] == "riser"
    assert deltas.loc["DECAY", "quadrant_1m"] == "decayer"
    assert deltas.loc["STABLE", "quadrant_1m"] == "neutral"

    assert deltas.loc["RISER", "delta_1m"] > 50
    assert deltas.loc["DECAY", "delta_1m"] < -50


def test_top_risers_filters_only_riser_quadrant():
    dates = pd.bdate_range(end="2024-12-31", periods=200)
    n = len(dates)

    paths = {
        "TRUE_RISER": [15.0] * (n - 21) + [85.0] * 21,
        "STABLE_WINNER": [80.0] * (n - 21) + [95.0] * 21,
        "CHRONIC_LOSER": [10.0] * n,
    }
    hist = _build_history(paths, dates)
    deltas = compute_delta_metrics(hist)

    risers = get_top_risers(deltas, window="1m", limit=10)
    tickers = set(risers["ticker"])
    assert "TRUE_RISER" in tickers
    assert "STABLE_WINNER" not in tickers
    assert "CHRONIC_LOSER" not in tickers


def test_top_decayers_filters_only_decayer_quadrant():
    dates = pd.bdate_range(end="2024-12-31", periods=200)
    n = len(dates)

    paths = {
        "TRUE_DECAYER": [80.0] * (n - 21) + [15.0] * 21,
        "CHRONIC_LOSER_DROPPING": [25.0] * (n - 21) + [10.0] * 21,
        "STABLE_WINNER": [85.0] * n,
    }
    hist = _build_history(paths, dates)
    deltas = compute_delta_metrics(hist)

    decayers = get_top_decayers(deltas, window="1m", limit=10)
    tickers = set(decayers["ticker"])
    assert "TRUE_DECAYER" in tickers
    assert "CHRONIC_LOSER_DROPPING" not in tickers
    assert "STABLE_WINNER" not in tickers


def test_sustained_risers_requires_both_windows():
    """
    SUSTAINED: ниска база, ratchet up в двата windows → и двата quadrant-а са riser.
    RECENT_BUMP: средна база, скок само в 1m → quadrant-ите са neutral, не sustained.
    """
    dates = pd.bdate_range(end="2024-12-31", periods=300)
    n = len(dates)

    # 5 за повечето време, 25 в средата, 85 в края → base ниска (под p20), и двата delta положителни
    sustained = [5.0] * (n - 63) + [25.0] * 42 + [85.0] * 21
    # 50 за повечето време, 75 в края → base в средата, deltas положителни но quadrant=neutral
    recent_bump = [50.0] * (n - 21) + [75.0] * 21

    hist = _build_history(
        {"SUSTAINED": sustained, "RECENT_BUMP": recent_bump},
        dates,
    )
    deltas = compute_delta_metrics(hist)

    sus = get_sustained_risers(deltas, limit=10)
    tickers = set(sus["ticker"])
    assert "SUSTAINED" in tickers
    assert "RECENT_BUMP" not in tickers


def test_history_persistence_roundtrip(tmp_path: Path):
    history_file = tmp_path / "ranks.parquet"
    snap = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-15", "2024-01-15"]),
            "ticker": ["AAA", "BBB"],
            "raw_score": [0.5, -0.2],
            "percentile_rank": [80.0, 20.0],
            "unadj_percentile": [85.0, 15.0],
        }
    )
    append_snapshot(history_file, snap)
    loaded = load_history(history_file)
    assert len(loaded) == 2
    assert set(loaded["ticker"]) == {"AAA", "BBB"}


def test_history_persistence_idempotent(tmp_path: Path):
    """Append на същия (date, ticker) трябва да презаписва, не да дублира."""
    history_file = tmp_path / "ranks.parquet"
    snap_v1 = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-15"]),
            "ticker": ["AAA"],
            "raw_score": [0.5],
            "percentile_rank": [80.0],
            "unadj_percentile": [85.0],
        }
    )
    append_snapshot(history_file, snap_v1)

    snap_v2 = snap_v1.copy()
    snap_v2["percentile_rank"] = [90.0]
    append_snapshot(history_file, snap_v2)

    loaded = load_history(history_file)
    assert len(loaded) == 1
    assert loaded["percentile_rank"].iloc[0] == 90.0
