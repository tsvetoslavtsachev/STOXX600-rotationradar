"""Unit tests for the seam repairs (src/price_scale.py): GBX/GBP units, splits, overlap rebase."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.price_scale import (
    earliest_seam,
    find_minor_unit_seams,
    find_split_candidates,
    match_split,
    rebase_history_to_overlap,
    repair_minor_unit_seams,
    repair_series,
    repair_split_seams,
)
from src.signal_engine import compute_ticker_mom


def _dates(n: int, end: str = "2026-09-09") -> pd.DatetimeIndex:
    return pd.bdate_range(end=end, periods=n)


def _pence_then_pounds(n: int = 400, seam_at: int = 350, level: float = 800.0) -> pd.Series:
    """A pence series (about 800 GBX) whose last rows are in pounds (8.00)."""
    rng = np.random.default_rng(7)
    px = level * np.cumprod(1 + rng.normal(0, 0.01, n))
    px[seam_at:] = px[seam_at:] / 100.0
    return pd.Series(px, index=_dates(n))


# ---- 1. minor-unit seams --------------------------------------------------------------------

def test_seam_detected_on_the_switch_day():
    s = _pence_then_pounds()
    assert find_minor_unit_seams(s) == [s.index[350]]


def test_seam_detected_even_with_a_17pct_move_on_the_switch_day():
    # SGRO.L 2026-06-24: 742.0 GBX -> 8.71 GBP (ratio 0.0117) must still count as a unit seam
    s = pd.Series([740.0, 742.0, 8.71, 8.8, 8.8], index=_dates(5))
    assert find_minor_unit_seams(s) == [s.index[2]]


def test_repair_makes_series_unit_consistent_and_idempotent():
    s = _pence_then_pounds()
    repaired, seams = repair_series(s)
    assert len(seams) == 1
    assert find_minor_unit_seams(repaired) == []
    assert np.isclose(repaired.iloc[0], s.iloc[0] / 100.0)   # pence history -> pounds
    assert np.isclose(repaired.iloc[-1], s.iloc[-1])         # pounds tail untouched
    again, seams2 = repair_series(repaired)
    assert seams2 == []
    pd.testing.assert_series_equal(again, repaired)


def test_repair_handles_pounds_history_with_pence_tail():
    s = _pence_then_pounds()
    flipped = s.copy()
    flipped.iloc[:350] = s.iloc[:350] / 100.0  # pounds history
    flipped.iloc[350:] = s.iloc[350:] * 100.0  # pence tail (a raw yfinance fallback)
    repaired, seams = repair_series(flipped)
    assert len(seams) == 1
    assert find_minor_unit_seams(repaired) == []
    assert np.isclose(repaired.iloc[-1], flipped.iloc[-1] / 100.0)
    assert np.isclose(repaired.iloc[0], flipped.iloc[0])


def test_momentum_is_sane_after_repair_and_broken_before():
    s = _pence_then_pounds()
    assert compute_ticker_mom(s) < -0.9  # the bug: GBP now / GBX a year ago
    repaired, _ = repair_series(s)
    assert -0.5 < compute_ticker_mom(repaired) < 0.5


def test_frame_repair_touches_only_minor_unit_columns():
    s = _pence_then_pounds()
    frame = pd.DataFrame({"AUTO.L": s, "SAP.DE": s.copy()})
    frame["IHG.L"] = np.linspace(100, 120, len(s))  # a .L name already in pounds, no seam
    repaired, report = repair_minor_unit_seams(frame)
    assert set(report) == {"AUTO.L"}
    assert earliest_seam(report) == s.index[350]
    pd.testing.assert_series_equal(repaired["SAP.DE"], frame["SAP.DE"])  # not a .L: untouched
    pd.testing.assert_series_equal(repaired["IHG.L"], frame["IHG.L"])    # no seam: untouched
    assert find_minor_unit_seams(repaired["AUTO.L"]) == []


def test_frame_without_seams_is_returned_unchanged():
    frame = pd.DataFrame({"AUTO.L": np.linspace(5, 6, 300), "SAP.DE": np.linspace(100, 90, 300)},
                         index=_dates(300))
    repaired, report = repair_minor_unit_seams(frame)
    assert report == {}
    assert repaired is frame


def test_genuine_large_move_is_not_a_unit_seam():
    s = pd.Series([100.0, 100.0, 40.0, 41.0, 42.0], index=_dates(5))   # a -60% day is a crash
    assert find_minor_unit_seams(s) == []
    s2 = pd.Series([100.0, 100.0, 1.0, 1.01, 1.02], index=_dates(5))   # a -99% day is a unit switch
    assert find_minor_unit_seams(s2) == [s2.index[2]]


# ---- 2. split seams --------------------------------------------------------------------------

def _split_series(n: int = 300, seam_at: int = 250, factor: float = 10.0) -> pd.Series:
    px = np.full(n, 400.0)
    px[seam_at:] = 400.0 / factor
    return pd.Series(px, index=_dates(n))


def test_split_candidate_and_calendar_match():
    s = _split_series()
    cands = find_split_candidates(s)
    assert [d for d, _ in cands] == [s.index[250]]
    seam, ratio = cands[0]
    assert match_split(seam, ratio, {seam + pd.Timedelta(days=7): 10.0}) == 10.0
    assert match_split(seam, ratio, {seam + pd.Timedelta(days=40): 10.0}) is None  # too far
    assert match_split(seam, ratio, {seam: 2.0}) is None                          # wrong size


def test_split_repair_divides_history_only_with_calendar_evidence():
    frame = pd.DataFrame({"SQN.SW": _split_series(), "ABVX.PA": _split_series(factor=1 / 6.0)})
    seam = frame.index[250]
    calendar = {"SQN.SW": {seam + pd.Timedelta(days=7): 10.0}}   # ABVX.PA: no split -> a real spike
    repaired, report = repair_split_seams(frame, splits_lookup=lambda t: calendar.get(t, {}))
    assert list(report) == ["SQN.SW"] and report["SQN.SW"] == [(seam, 10.0)]
    assert np.isclose(repaired["SQN.SW"].iloc[0], 40.0)          # pre-split history / 10
    assert np.isclose(repaired["SQN.SW"].iloc[-1], 40.0)         # post-split untouched
    pd.testing.assert_series_equal(repaired["ABVX.PA"], frame["ABVX.PA"])
    again, report2 = repair_split_seams(repaired, splits_lookup=lambda t: calendar.get(t, {}))
    assert report2 == {}                                        # idempotent


def test_split_repair_without_calendar_changes_nothing():
    frame = pd.DataFrame({"SQN.SW": _split_series()})
    repaired, report = repair_split_seams(frame, splits_lookup=lambda t: {})
    assert report == {} and repaired is frame


# ---- 3. overlap rebase -----------------------------------------------------------------------

def test_overlap_rebase_applies_constant_ratio_to_pre_overlap_history():
    idx = _dates(30)
    cached = pd.DataFrame({"SQN.SW": np.full(30, 400.0), "SAP.DE": np.full(30, 100.0)}, index=idx)
    new_idx = idx[-5:].append(pd.bdate_range(start=idx[-1] + pd.Timedelta(days=1), periods=3))
    new = pd.DataFrame({"SQN.SW": np.full(8, 40.0), "SAP.DE": np.full(8, 100.5)}, index=new_idx)
    rebased, report = rebase_history_to_overlap(cached, new)
    assert set(report) == {"SQN.SW"} and np.isclose(report["SQN.SW"], 0.1)
    assert np.isclose(rebased["SQN.SW"].iloc[0], 40.0)           # history moved to the new basis
    pd.testing.assert_series_equal(rebased["SAP.DE"], cached["SAP.DE"])  # 0.5%: below threshold


def test_overlap_rebase_ignores_non_constant_differences():
    idx = _dates(30)
    cached = pd.DataFrame({"X.DE": np.full(30, 100.0)}, index=idx)
    new = pd.DataFrame({"X.DE": [90.0, 100.0, 110.0, 95.0, 105.0]}, index=idx[-5:])  # revisions, not a basis
    rebased, report = rebase_history_to_overlap(cached, new)
    assert report == {} and rebased is cached
