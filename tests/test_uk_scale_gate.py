"""
Scale gate over the PUBLISHED artefacts (docs/data.json, data/prices_cache.parquet).

Born 2026-09-11: 121 of 128 London ``.L`` names showed mom_12_1_pct between -98 and -99.5
because the price cache mixed pence (pre-P9 yfinance) and pounds (base reader). Ranks are
scale-invariant per series, so a unit seam inside one series is the only way to get there.

Three checks, each a hard fail:
  1. no ticker with mom_12_1_pct below MOM_FLOOR unless it is listed in EXTREME_LOSERS_ALLOWED
     with an explicit, dated reason;
  2. the share of .L names in the bottom BOTTOM_SHARE of the 12-1 momentum table is at most
     MAX_OVERREPRESENTATION times their share of the whole table;
  3. no .L series in the price cache carries a GBX/GBP seam.

The gate runs in CI (daily_update.yml, BEFORE the commit step) and in the test suite.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd
import pytest

from src.price_scale import find_minor_unit_seams, is_minor_unit_ticker

ROOT = Path(__file__).resolve().parents[1]
DATA_JSON = ROOT / "docs" / "data.json"
PRICES_CACHE = ROOT / "data" / "prices_cache.parquet"

MOM_FLOOR = -90.0           # percent; a 12-1 return below this needs an explicit reason
BOTTOM_SHARE = 0.15         # the bottom 15% of the momentum table
MAX_OVERREPRESENTATION = 2.0  # .L share in the bottom tail <= 2x their universe share

# ticker -> reason (dated, verifiable). Empty on purpose: an entry is a deliberate decision.
EXTREME_LOSERS_ALLOWED: dict[str, str] = {}


# ---- pure checks (unit-testable, no file access) -------------------------------------------

def unexplained_extreme_losers(rows: list[dict], allowed: dict[str, str] | None = None,
                               floor: float = MOM_FLOOR) -> list[tuple[str, float]]:
    """Tickers whose mom_12_1_pct is below ``floor`` and are not in ``allowed``."""
    allowed = allowed or {}
    out = []
    for r in rows:
        m = r.get("mom_12_1_pct")
        if m is None or not isinstance(m, (int, float)) or not math.isfinite(m):
            continue
        if m < floor and r.get("ticker") not in allowed:
            out.append((r["ticker"], float(m)))
    return sorted(out, key=lambda x: x[1])


def minor_unit_bottom_share(rows: list[dict], bottom_share: float = BOTTOM_SHARE) -> tuple[float, float, int]:
    """(share of .L in the bottom tail, share of .L in the whole table, tail size)."""
    scored = [r for r in rows
              if isinstance(r.get("mom_12_1_pct"), (int, float)) and math.isfinite(r["mom_12_1_pct"])]
    if not scored:
        return 0.0, 0.0, 0
    scored.sort(key=lambda r: r["mom_12_1_pct"])
    k = max(1, int(math.floor(len(scored) * bottom_share)))
    tail = scored[:k]
    share_tail = sum(is_minor_unit_ticker(r["ticker"]) for r in tail) / k
    share_all = sum(is_minor_unit_ticker(r["ticker"]) for r in scored) / len(scored)
    return share_tail, share_all, k


def cache_seams(prices: pd.DataFrame) -> dict[str, list]:
    return {str(c): find_minor_unit_seams(prices[c])
            for c in prices.columns if is_minor_unit_ticker(c) and find_minor_unit_seams(prices[c])}


# ---- the gate over the published files ----------------------------------------------------

def _rows() -> list[dict]:
    assert DATA_JSON.exists(), f"missing {DATA_JSON}"
    payload = json.loads(DATA_JSON.read_text(encoding="utf-8"))
    rows = payload.get("rank_all_stocks") or []
    assert rows, "rank_all_stocks is empty"
    return rows


def test_no_unexplained_extreme_losers():
    bad = unexplained_extreme_losers(_rows(), EXTREME_LOSERS_ALLOWED)
    assert not bad, (f"{len(bad)} tickers with mom_12_1_pct < {MOM_FLOOR} and no explicit reason "
                     f"(first 10): {bad[:10]}")


def test_minor_unit_names_not_overrepresented_in_bottom_tail():
    share_tail, share_all, k = minor_unit_bottom_share(_rows())
    assert share_tail <= MAX_OVERREPRESENTATION * share_all, (
        f".L names are {share_tail:.0%} of the bottom {k} by mom_12_1_pct vs {share_all:.0%} of the "
        f"universe (cap {MAX_OVERREPRESENTATION}x) -> a unit/scale problem, not a market move")


def test_price_cache_has_no_minor_unit_seams():
    assert PRICES_CACHE.exists(), f"missing {PRICES_CACHE}"
    prices = pd.read_parquet(PRICES_CACHE)
    prices.index = pd.to_datetime(prices.index)
    seams = cache_seams(prices)
    assert not seams, (f"{len(seams)} .L series mix GBX/GBP in the price cache (first 5): "
                       f"{dict(list(seams.items())[:5])}")


# ---- mutation proofs: the checks must FIRE on a broken table ------------------------------

def _healthy_rows(n: int = 100, n_uk: int = 20) -> list[dict]:
    rows = []
    for i in range(n):
        t = f"UK{i}.L" if i < n_uk else f"EU{i}.DE"
        rows.append({"ticker": t, "mom_12_1_pct": -40.0 + (i * 80.0 / n)})
    # spread the .L names evenly across the table
    for i in range(n_uk):
        rows[i]["mom_12_1_pct"] = -40.0 + (i * n / n_uk) * 80.0 / n
    return rows


def test_healthy_table_passes_both_checks():
    rows = _healthy_rows()
    assert unexplained_extreme_losers(rows) == []
    share_tail, share_all, _ = minor_unit_bottom_share(rows)
    assert share_tail <= MAX_OVERREPRESENTATION * share_all


def test_extreme_loser_check_fires_and_allowlist_silences_it():
    rows = _healthy_rows()
    rows[5]["mom_12_1_pct"] = -99.1
    assert unexplained_extreme_losers(rows) == [("UK5.L", -99.1)]
    assert unexplained_extreme_losers(rows, {"UK5.L": "delisted 2026-01-01 (verified)"}) == []


def test_bottom_share_check_fires_when_uk_collapses():
    rows = _healthy_rows()
    for r in rows:
        if is_minor_unit_ticker(r["ticker"]):
            r["mom_12_1_pct"] = -99.0  # the 2026-09 symptom
    share_tail, share_all, _ = minor_unit_bottom_share(rows)
    assert share_tail > MAX_OVERREPRESENTATION * share_all


def test_cache_seam_check_fires_on_mixed_units():
    idx = pd.bdate_range(end="2026-09-09", periods=300)
    px = pd.Series(800.0, index=idx)
    px.iloc[250:] = 8.0
    frame = pd.DataFrame({"AUTO.L": px, "SAP.DE": pd.Series(100.0, index=idx)})
    assert list(cache_seams(frame)) == ["AUTO.L"]
    frame["AUTO.L"] = 8.0
    assert cache_seams(frame) == {}


@pytest.mark.parametrize("bad", [float("nan"), None, "n/a"])
def test_missing_momentum_is_ignored_not_flagged(bad):
    rows = _healthy_rows()
    rows[0]["mom_12_1_pct"] = bad
    assert unexplained_extreme_losers(rows) == []
