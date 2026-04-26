"""
ЕДНОКРАТЕН retrospective backfill — построява 5-годишна rank history за STOXX 600.

Стъпки:
  1. Изтегля STOXX 600 universe от iShares EXSA holdings CSV
  2. Изтегля 5y price history за всички ticker-и (yfinance)
  3. Зарежда sector_map от universe (за sector-relative scoring)
  4. За всяка business day в [start+252d, today]: пресмята cross-section ranks
  5. Записва в data/ranks_history.parquet

Използване:
  python scripts/backfill_history.py
  python scripts/backfill_history.py --years 5 --sample-every 1
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.prices import download_prices  # noqa: E402
from src.rank_history import HISTORY_COLUMNS, build_history_from_prices  # noqa: E402
from src.universe import fetch_full_universe  # noqa: E402

DATA_DIR = ROOT / "data"
HISTORY_PATH = DATA_DIR / "ranks_history.parquet"
PRICES_CACHE_PATH = DATA_DIR / "prices_cache.parquet"
UNIVERSE_CACHE_PATH = DATA_DIR / "universe.parquet"


def run_backfill(
    years: int = 5,
    sample_every: int = 1,
    use_cache: bool = True,
) -> Path:
    """
    sample_every: 1 = всеки търговски ден; 5 = веднъж седмично; полезно за бърз dev cycle.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[1/4] Fetching STOXX 600 universe (iShares EXSA holdings CSV)...")
    universe = fetch_full_universe(years_back=years)
    universe.to_parquet(UNIVERSE_CACHE_PATH)
    tickers = universe["ticker"].tolist()
    print(f"      {len(tickers)} current tickers (STOXX 600 ETF holdings)")
    # Invalidate sector_map.json — universe може да има нови ticker-и
    # (особено след промяна в ticker mapping logic-а)
    sector_map_path = DATA_DIR / "sector_map.json"
    if sector_map_path.exists():
        sector_map_path.unlink()
        print("      Invalidated sector_map.json (will be regenerated)")

    end = pd.Timestamp.today().normalize()
    start = end - pd.DateOffset(years=years + 1)  # +1y буфер за 12m lookback

    if use_cache and PRICES_CACHE_PATH.exists():
        print(f"[2/4] Loading cached prices from {PRICES_CACHE_PATH.name}...")
        prices = pd.read_parquet(PRICES_CACHE_PATH)
        prices.index = pd.to_datetime(prices.index)
    else:
        print(f"[2/4] Downloading {len(tickers)} tickers prices ({start.date()} → {end.date()})...")
        prices = download_prices(tickers, start=start, end=end)
        prices.to_parquet(PRICES_CACHE_PATH)
        print(f"      Cached to {PRICES_CACHE_PATH.name}")

    print(f"      Got {len(prices.columns)} tickers × {len(prices)} days")

    print(f"[3/4] Computing daily cross-sections (sample_every={sample_every})...")
    cutoff = prices.index[252] if len(prices) > 252 else prices.index[0]
    sample_dates = prices.index[prices.index >= cutoff][::sample_every]
    print(f"      {len(sample_dates)} sample dates from {sample_dates[0].date()} to {sample_dates[-1].date()}")

    # Sector mapping за sector-relative z-score scoring
    sector_map = dict(zip(universe["ticker"], universe["gics_sector"]))
    print(f"      Loaded {len(sector_map)} sector mappings ({len(set(sector_map.values()))} unique sectors)")

    history = build_history_from_prices(prices, sample_dates, sector_map=sector_map)
    print(f"      Built {len(history)} rank records")

    print(f"[4/4] Writing to {HISTORY_PATH.name}...")
    history[HISTORY_COLUMNS].to_parquet(HISTORY_PATH, index=False)
    size_mb = HISTORY_PATH.stat().st_size / 1e6
    print(f"      Done. {size_mb:.1f} MB")

    return HISTORY_PATH


def run_validation() -> None:
    """Sanity report за backfill historiya."""
    if not HISTORY_PATH.exists():
        print("ERROR: History file does not exist. Run backfill first.")
        sys.exit(1)

    history = pd.read_parquet(HISTORY_PATH)
    history["date"] = pd.to_datetime(history["date"])

    print(f"\nHistory spans: {history['date'].min().date()} → {history['date'].max().date()}")
    print(f"Total records: {len(history):,}")
    print(f"Unique tickers: {history['ticker'].nunique()}")

    last_date = history["date"].max()
    last_snap = history[history["date"] == last_date]
    print(f"\nLast snapshot ({last_date.date()}): {len(last_snap)} valid scores")
    print("Top 5 by sector_zscore percentile:")
    print(last_snap.nlargest(5, "percentile_rank")[["ticker", "raw_score", "percentile_rank"]].to_string(index=False))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=int, default=5)
    p.add_argument("--sample-every", type=int, default=1, help="Sample stride in business days")
    p.add_argument("--no-cache", action="store_true", help="Skip prices cache")
    p.add_argument("--validate", action="store_true", help="Run sanity checks")
    args = p.parse_args()

    if args.validate:
        run_validation()
        return

    run_backfill(
        years=args.years,
        sample_every=args.sample_every,
        use_cache=not args.no_cache,
    )
    run_validation()


if __name__ == "__main__":
    main()
