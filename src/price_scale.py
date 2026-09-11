"""
Unit and basis seams in the incremental price cache.

The cache (data/prices_cache.parquet) is built incrementally: every run re-reads the last
few days and appends them to the stored history. Whatever basis the new rows come in,
the stored history keeps the old one, so a series can end up mixing:

  * pence and pounds: London ``.L`` names are quoted in pence (GBX) by yfinance, but the
    canonical base reader (collectors, P8c ``normalize_currency=True``) serves pounds.
    The cache built before P9 held pence; the first base-first run (2026-07-02) appended
    pounds from 2026-06-24 on. A 12-1 momentum spanning the seam is GBP_now / GBX_year_ago
    = about -99% (121 of 128 .L names on 2026-09-11).
  * pre- and post-split prices: a fetch after a split returns split-adjusted rows while
    the stored history stays unadjusted (SQN.SW 10:1 in May 2026 -> -92% 12-1).

Ranks are scale-invariant per series, so the only invariant that matters is: one series,
one basis. Three repairs, all idempotent (a repaired frame yields no report):

  1. ``repair_minor_unit_seams``: a one-day ratio within 2x of 1/100 (or 100) is a unit
     seam, never a price move; every segment is rescaled to the LOWEST level (pounds).
  2. ``repair_split_seams``: a one-day ratio outside [0.55, 1.8] whose date and size match
     a split in the ticker's split calendar (Yahoo by default, injectable) -> the history
     before the seam is divided by the split factor. Genuine crashes/spikes with no matching
     split are left alone (and the extreme-loser gate then asks for an explicit reason).
  3. ``rebase_history_to_overlap``: at merge time the re-read overlap days are compared with
     the cached values; a constant ratio (split, currency switch, dividend re-adjustment)
     is applied to the whole cached history BEFORE the overlap, so a future split never
     makes it into the cache as a seam in the first place.

Not vendored: this is a rotation-radar specific shim over the vendored src/prices.py.
"""

from __future__ import annotations

import math
from typing import Callable

import numpy as np
import pandas as pd

# Suffixes of exchanges that quote in a minor currency unit (London pence).
MINOR_UNIT_SUFFIXES: tuple[str, ...] = (".L",)
# Minor -> major unit factor (GBX -> GBP).
MINOR_UNIT_FACTOR = 100.0
# A one-day ratio within this multiplicative band of 1/factor (or factor) is a unit seam:
# |log(ratio * factor)| <= log(2), i.e. ratio in [0.005, 0.02]. An index member does not lose
# 98-99.5% in a day; a genuine -60% day (ratio 0.4) is far outside.
MINOR_UNIT_LOG_BAND = math.log(2.0)

# Split candidates: a one-day ratio below/above these is not an ordinary move.
SPLIT_CANDIDATE_LOW, SPLIT_CANDIDATE_HIGH = 0.55, 1.8
# A calendar split matches a candidate seam when the dates are within this many days and
# the observed ratio is within 25% of 1/split_factor.
SPLIT_DATE_WINDOW_DAYS = 10
SPLIT_LOG_BAND = math.log(1.25)

# Overlap rebase: a constant new/cached ratio beyond this (2%) over the overlap days means the
# stored history is on another basis. Dividends below 2% are left as drift (harmless).
OVERLAP_MIN_LOG_SHIFT = math.log(1.02)
# and the ratio must be constant across the overlap (max |log| spread) to count as a basis shift
OVERLAP_MAX_LOG_SPREAD = math.log(1.005)

SplitsLookup = Callable[[str], dict]


def is_minor_unit_ticker(ticker: str, suffixes: tuple[str, ...] = MINOR_UNIT_SUFFIXES) -> bool:
    return str(ticker).endswith(suffixes)


def _positive(series: pd.Series) -> pd.Series:
    s = series.dropna().astype(float)
    return s[s > 0]


# ---- 1. minor-unit (GBX/GBP) seams ----------------------------------------------------------

def find_minor_unit_seams(
    series: pd.Series,
    factor: float = MINOR_UNIT_FACTOR,
    log_band: float = MINOR_UNIT_LOG_BAND,
) -> list[pd.Timestamp]:
    """Dates on which the series steps by about ``factor`` or ``1/factor`` versus the
    previous observation. Empty list means the series is unit-consistent."""
    s = _positive(series)
    if len(s) < 2:
        return []
    log_ratio = np.log(s / s.shift(1))
    drop = (log_ratio + math.log(factor)).abs() <= log_band
    jump = (log_ratio - math.log(factor)).abs() <= log_band
    return list(s.index[(drop | jump).fillna(False)])


def repair_series(
    series: pd.Series,
    factor: float = MINOR_UNIT_FACTOR,
    log_band: float = MINOR_UNIT_LOG_BAND,
) -> tuple[pd.Series, list[pd.Timestamp]]:
    """Rescale every segment of ``series`` to the lowest unit level seen.

    Each seam moves the running level by one step (drop -> level-1, jump -> level+1);
    an observation at level k is divided by factor**(k - min_level). With a single
    pence->pounds seam this divides the pence history by 100 and leaves pounds alone.
    """
    seams = find_minor_unit_seams(series, factor, log_band)
    if not seams:
        return series, []
    s = _positive(series)
    ratio = s / s.shift(1)
    step = pd.Series(0, index=s.index, dtype=int)
    for d in seams:
        step.loc[d] = -1 if ratio.loc[d] < 1.0 else 1
    level = step.cumsum()
    divisor = np.power(factor, (level - level.min()).astype(float))
    repaired = series.astype(float).copy()
    repaired.loc[s.index] = s / divisor
    return repaired, seams


def repair_minor_unit_seams(
    prices: pd.DataFrame,
    suffixes: tuple[str, ...] = MINOR_UNIT_SUFFIXES,
    factor: float = MINOR_UNIT_FACTOR,
    log_band: float = MINOR_UNIT_LOG_BAND,
) -> tuple[pd.DataFrame, dict[str, list[pd.Timestamp]]]:
    """Repair every minor-unit column of a wide price frame (index = date, columns =
    tickers). Returns the repaired frame and ``{ticker: [seam dates]}`` for the
    columns that were touched (empty dict -> nothing changed, frame returned as-is)."""
    report: dict[str, list[pd.Timestamp]] = {}
    if prices is None or prices.empty:
        return prices, report
    out = prices.copy()
    for col in out.columns:
        if not is_minor_unit_ticker(col, suffixes):
            continue
        repaired, seams = repair_series(out[col], factor, log_band)
        if seams:
            out[col] = repaired
            report[str(col)] = seams
    return (out if report else prices), report


# ---- 2. split seams (calendar-confirmed) ----------------------------------------------------

def yahoo_splits(ticker: str) -> dict:
    """{date -> split factor} from Yahoo (10.0 = 10-for-1, 0.1 = 1-for-10). Empty on any
    failure: no evidence means no repair (the extreme-loser gate still watches)."""
    try:
        import yfinance as yf  # local import: tests inject a lookup and never hit the network
        s = yf.Ticker(ticker).splits
        return {pd.Timestamp(k).tz_localize(None).normalize(): float(v) for k, v in s.items() if v > 0}
    except Exception:  # noqa: BLE001 -- network/parse failure: no evidence, no repair
        return {}


def find_split_candidates(series: pd.Series,
                          low: float = SPLIT_CANDIDATE_LOW,
                          high: float = SPLIT_CANDIDATE_HIGH) -> list[tuple[pd.Timestamp, float]]:
    """(date, one-day ratio) for every step outside [low, high]."""
    s = _positive(series)
    if len(s) < 2:
        return []
    ratio = (s / s.shift(1)).dropna()
    hits = ratio[(ratio < low) | (ratio > high)]
    return [(d, float(v)) for d, v in hits.items()]


def match_split(seam_date: pd.Timestamp, ratio: float, splits: dict,
                window_days: int = SPLIT_DATE_WINDOW_DAYS,
                log_band: float = SPLIT_LOG_BAND) -> float | None:
    """The calendar split factor that explains an observed step (ratio ~ 1/factor within
    ``log_band``, dated within ``window_days``), else None."""
    best = None
    for d, factor in splits.items():
        if factor <= 0 or abs((pd.Timestamp(d) - seam_date).days) > window_days:
            continue
        if abs(math.log(ratio * factor)) <= log_band:
            best = factor if best is None else best
    return best


def repair_split_seams(
    prices: pd.DataFrame,
    splits_lookup: SplitsLookup = yahoo_splits,
) -> tuple[pd.DataFrame, dict[str, list[tuple[pd.Timestamp, float]]]]:
    """For every column with a candidate step, ask the split calendar; a confirmed split
    divides the history BEFORE the seam by the split factor. Returns the repaired frame and
    ``{ticker: [(seam date, factor)]}`` (empty dict -> frame returned as-is)."""
    report: dict[str, list[tuple[pd.Timestamp, float]]] = {}
    if prices is None or prices.empty:
        return prices, report
    out = prices.copy()
    for col in out.columns:
        candidates = find_split_candidates(out[col])
        if not candidates:
            continue
        splits = splits_lookup(str(col))
        if not splits:
            continue
        fixed = out[col].astype(float).copy()
        applied = []
        for seam_date, ratio in candidates:
            factor = match_split(seam_date, ratio, splits)
            if factor is None:
                continue
            fixed.loc[fixed.index < seam_date] = fixed.loc[fixed.index < seam_date] / factor
            applied.append((seam_date, factor))
        if applied:
            out[col] = fixed
            report[str(col)] = applied
    return (out if report else prices), report


# ---- 3. overlap rebase at merge time --------------------------------------------------------

def rebase_history_to_overlap(
    cached: pd.DataFrame,
    new: pd.DataFrame,
    min_log_shift: float = OVERLAP_MIN_LOG_SHIFT,
    max_log_spread: float = OVERLAP_MAX_LOG_SPREAD,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Compare the overlap days (present in both frames) column by column. When new/cached
    is a CONSTANT ratio k with |log k| >= ``min_log_shift`` the cached history before the
    overlap is on another basis (split, unit switch, dividend re-adjustment): multiply the
    cached rows dated before the overlap by k. Returns (rebased cached, {ticker: k})."""
    report: dict[str, float] = {}
    if cached is None or cached.empty or new is None or new.empty:
        return cached, report
    overlap = cached.index.intersection(new.index)
    if len(overlap) == 0:
        return cached, report
    out = cached.copy()
    first_overlap = overlap.min()
    for col in new.columns:
        if col not in out.columns:
            continue
        a = out.loc[overlap, col].astype(float)
        b = new.loc[overlap, col].astype(float)
        mask = a.notna() & b.notna() & (a > 0) & (b > 0)
        if mask.sum() == 0:
            continue
        log_k = np.log(b[mask] / a[mask])
        if (log_k.max() - log_k.min()) > max_log_spread:
            continue  # not a constant basis shift (revisions, different sources)
        k = float(np.exp(log_k.median()))
        if abs(math.log(k)) < min_log_shift:
            continue
        before = out.index < first_overlap
        out.loc[before, col] = out.loc[before, col].astype(float) * k
        report[str(col)] = k
    return (out if report else cached), report


# ---- reporting -------------------------------------------------------------------------------

def earliest_seam(report: dict) -> pd.Timestamp | None:
    dates = []
    for seams in report.values():
        for item in seams:
            dates.append(pd.Timestamp(item[0] if isinstance(item, tuple) else item))
    return min(dates) if dates else None


def describe_report(report: dict) -> str:
    if not report:
        return "no seams"
    dates = sorted({pd.Timestamp(i[0] if isinstance(i, tuple) else i).date()
                    for seams in report.values() for i in seams})
    return f"{len(report)} columns repaired; seam dates: {[str(d) for d in dates]}"
