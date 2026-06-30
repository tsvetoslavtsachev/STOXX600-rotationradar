"""
Daily orchestrator — изпълнява се от GitHub Actions всеки delnik след EU close.

Стъпки:
  1. Зареди STOXX 600 universe от iShares (cache в data/universe.parquet)
  2. Update prices cache (incremental)
  3. Пресметни today's cross-section (sector-relative z-score)
  4. Append към ranks_history.parquet (idempotent)
  5. Render data.json за dashboard
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.prices import download_prices  # noqa: E402
# INIT-22 P9 strangler: read canonical daily Close FROM the price-archive (base) first, keeping the
# OLD yfinance download_prices as a CLOSED fallback so production never stops. ONE canonical reader
# (collectors/price/consumer.py) shared by every consumer. If collectors/data-core are not
# importable (a bare local run, no archive checkout) we degrade to the pure fetch. The CI guard
# (scripts/assert_base_sourced.py), gated on the read PATs, fails RED on a silent mass-fallback.
# STOXX600 has 126 GBX-quoted .L London names -> normalize_currency=True (/100 GBX->GBP).
try:
    from collectors.price.consumer import load_ohlcv_base_first  # noqa: E402
    _HAVE_BASE = True
except ImportError:
    _HAVE_BASE = False
from src.rank_history import HISTORY_COLUMNS, append_snapshot  # noqa: E402
from src.render import render_dashboard_data  # noqa: E402
from src.sector_engine import get_sector_dataframe  # noqa: E402
from src.signal_engine import compute_cross_section  # noqa: E402
from src.universe import fetch_full_universe  # noqa: E402

DATA_DIR = ROOT / "data"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
PRICES_CACHE_PATH = DATA_DIR / "prices_cache.parquet"
SECTOR_CACHE_PATH = DATA_DIR / "sector_map.json"
UNIVERSE_CACHE_PATH = DATA_DIR / "universe.parquet"

# 5 години история за screener
LOOKBACK_DAYS_FOR_SCORING = 1500

# INIT-22 P9 strangler helpers ------------------------------------------------
PRICE_SOURCE_PATH = DATA_DIR / "price_source.json"


def _clean_close(df: pd.DataFrame, requested) -> pd.DataFrame:
    """Normalize a download_prices result to flat ticker-string columns, keeping only requested
    names. The VENDORED download_prices single-ticker path can return a MultiIndex/tuple column
    (current yfinance) which would leak a junk column into the price frame AND escape the consumer's
    provenance stamping. We can't edit the vendored src/prices.py, so the shim sanitizes here
    (flatten tuple -> ticker, drop anything not requested)."""
    if df is None or getattr(df, "empty", True):
        return pd.DataFrame()
    req = {str(x).upper(): x for x in requested}
    out = {}
    for col in df.columns:
        key = col[-1] if isinstance(col, tuple) else col
        canon = req.get(str(key).upper())
        if canon is not None and canon not in out:
            out[canon] = df[col]
    return pd.DataFrame(out) if out else pd.DataFrame()


def _base_first_close(tickers: list[str], start, end, source_acc: dict) -> pd.DataFrame:
    """Base-first flat adjusted-Close read mirroring download_prices' shape (DatetimeIndex x
    tickers), with the OLD download_prices as a CLOSED fallback (P9 strangler -- production NEVER
    stops: it degrades to the old fetch when the base reader is unimportable AND on ANY base-read
    exception). Accumulates {ticker -> 'base'|'fetch'|'missing'} into ``source_acc`` across every
    call. Every REQUESTED ticker is recorded -- one the archive cannot serve AND the fallback cannot
    fetch is stamped 'missing' (the consumer pops it from its own map, so without this it would
    evaporate and the guard's base fraction would not see it). STOXX600 has 126 GBX-quoted .L London
    names -> normalize_currency=True (/100 GBX->GBP, applied post-merge & uniform; only the .L names
    move and ranks are scale-invariant; Volume is never divided).
    """
    def _pure_fetch() -> pd.DataFrame:
        # The OLD path: pull the whole requested set from yfinance (the CLOSED fallback). Used when
        # the base reader is unimportable (no archive checkout) AND when a base read raises -- the
        # strangler must degrade, never hard-stop.
        df = _clean_close(download_prices(tickers, start=start, end=end), tickers)
        served = set(df.columns)
        for t in tickers:
            source_acc[t] = "fetch" if t in served else "missing"
        return df

    if not _HAVE_BASE:
        return _pure_fetch()

    def _fallback(missing, period=None):
        # download_prices has no ``period`` kwarg; reuse the window from the enclosing request so
        # the fallback path does not TypeError exactly when the archive is unavailable. Sanitize the
        # columns (single-ticker yfinance can return tuple columns) before handing back to the merge.
        return {"Close": _clean_close(download_prices(list(missing), start=start, end=end), missing)}

    try:
        ohlcv, source_map = load_ohlcv_base_first(
            tickers, fetch_fallback=_fallback, start=start, end=end, normalize_currency=True)
    except Exception as e:  # noqa: BLE001 -- strangler: ANY base failure degrades to the old fetch
        print(f"  WARN: base read raised ({e!r}); degrading to yfinance for this call (strangler).")
        return _pure_fetch()

    source_acc.update(source_map)
    for t in tickers:
        source_acc.setdefault(t, "missing")  # requested but neither base nor fetch served it
    return ohlcv.get("Close", pd.DataFrame())


def _write_price_source(source_acc: dict, expected: int) -> None:
    """Write per-symbol price provenance for scripts/assert_base_sourced.py (P9 strangler, mirror of
    the P6 ETF-rr guard). ``expected`` = requested universe size."""
    n_base = sum(1 for v in source_acc.values() if v == "base")
    payload = {
        "by_symbol": dict(sorted(source_acc.items())),
        "summary": {"expected": expected, "covered": len(source_acc),
                    "base": n_base, "fetch": len(source_acc) - n_base},
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PRICE_SOURCE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_sector_map_for_scoring(cache_path: Path) -> dict[str, str]:
    """GICS sector mapping за sector-relative z-score scoring."""
    df = get_sector_dataframe(cache_path)
    return dict(zip(df["ticker"], df["gics_sector"]))


def refresh_universe_cache() -> pd.DataFrame:
    """Изтегля свежо universe от iShares и пише cache.
    При промяна в universe инвалидира sector_map.json кеша."""
    print("  Refreshing universe from iShares EXSA holdings CSV...")
    universe = fetch_full_universe()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Сравни новия universe с кешираната версия — ако ticker set се е променил,
    # инвалидираме sector_map.json (може да има нови ticker-и без metadata).
    if UNIVERSE_CACHE_PATH.exists():
        try:
            old = pd.read_parquet(UNIVERSE_CACHE_PATH)
            if set(old["ticker"]) != set(universe["ticker"]):
                print(f"  Universe ticker set changed → invalidating sector_map.json")
                if SECTOR_CACHE_PATH.exists():
                    SECTOR_CACHE_PATH.unlink()
        except Exception:
            pass

    universe.to_parquet(UNIVERSE_CACHE_PATH)
    return universe


def update_prices_cache(tickers: list[str], source_acc: dict) -> pd.DataFrame:
    """Incremental update на price cache (base-first canonical; yfinance CLOSED fallback)."""
    end = pd.Timestamp.today().normalize()

    if PRICES_CACHE_PATH.exists():
        cached = pd.read_parquet(PRICES_CACHE_PATH)
        cached.index = pd.to_datetime(cached.index)
        last_date = cached.index.max()

        if last_date >= end - pd.tseries.offsets.BusinessDay(1):
            print(f"  Prices cache up to date ({last_date.date()}).")
            return cached.tail(LOOKBACK_DAYS_FOR_SCORING + 30)

        start = last_date - pd.Timedelta(days=5)
        print(f"  Incremental download {start.date()} → {end.date()}")
        new_prices = _base_first_close(tickers, start, end, source_acc)

        if new_prices.empty:
            return cached.tail(LOOKBACK_DAYS_FOR_SCORING + 30)

        combined = pd.concat([
            cached[~cached.index.isin(new_prices.index)],
            new_prices,
        ]).sort_index()
        combined = combined.dropna(axis=1, how="all")

        cutoff = end - pd.DateOffset(years=6)
        trimmed = combined[combined.index >= cutoff]
        trimmed.to_parquet(PRICES_CACHE_PATH)
        return trimmed.tail(LOOKBACK_DAYS_FOR_SCORING + 30)

    # No cache — full download
    start = end - pd.Timedelta(days=int(LOOKBACK_DAYS_FOR_SCORING * 1.6))
    print(f"  Full download {start.date()} → {end.date()}")
    prices = _base_first_close(tickers, start, end, source_acc)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    prices.to_parquet(PRICES_CACHE_PATH)
    return prices


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("[1/5] Fetching STOXX 600 universe (iShares EXSA holdings)...")
    universe = refresh_universe_cache()
    tickers = universe[universe["is_current"]]["ticker"].tolist()
    print(f"      {len(tickers)} current tickers")

    # INIT-22 P9: per-symbol price provenance (base vs fetch), written for assert_base_sourced.py.
    price_source: dict[str, str] = {}

    print("[2/5] Updating prices cache (base-first canonical; yfinance CLOSED fallback)...")
    prices = update_prices_cache(tickers, price_source)
    print(f"      {len(prices.columns)} tickers × {len(prices)} days")
    if price_source:
        n_base = sum(1 for v in price_source.values() if v == "base")
        print(f"      Price source: {n_base} base / {len(price_source) - n_base} fetch")
    _write_price_source(price_source, expected=len(tickers))

    print("[3/5] Computing today's cross-section (sector-relative z-score)...")
    sector_map = load_sector_map_for_scoring(SECTOR_CACHE_PATH)
    print(f"      Loaded {len(sector_map)} sector mappings")
    cs = compute_cross_section(prices, sector_map=sector_map)
    cs = cs.dropna(subset=["raw_score"])
    print(f"      {len(cs)} valid scores for {cs['date'].iloc[0].date()}")

    print("[4/5] Appending snapshot to history...")
    append_snapshot(HISTORY_PATH, cs[HISTORY_COLUMNS])
    size_mb = HISTORY_PATH.stat().st_size / 1e6
    print(f"      History now {size_mb:.1f} MB")

    print("[5/5] Rendering data.json...")
    payload = render_dashboard_data()
    print(f"      Rendered: as of {payload['metadata']['as_of']}")
    print(
        f"      Stable Winners 1m: {len(payload['stable_winners_1m'])} | "
        f"Quality Dip 1m: {len(payload['quality_dip_1m'])} | "
        f"Faded Bounces: {len(payload['faded_bounces_1m'])}"
    )
    print(
        f"      Current Strength: {len(payload['current_strength'])} | "
        f"Screener: {len(payload['screener']['stocks'])} | "
        f"Sectors: {len(payload['sector_rotation'])}"
    )

    print("\nDaily update complete.")


if __name__ == "__main__":
    main()
