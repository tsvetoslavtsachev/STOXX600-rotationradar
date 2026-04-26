"""
Batch price download от yfinance.

Използваме adjusted close (auto_adjust=True) за коректно third-party
splits/dividends handling. Yfinance е безплатен, но има rate limits, затова
batch-ваме на групи от ~50 ticker-а с retry.
"""

from __future__ import annotations

import time

import pandas as pd
import yfinance as yf

DEFAULT_BATCH = 50
DEFAULT_PAUSE = 1.0


def download_prices(
    tickers: list[str],
    start: str | pd.Timestamp,
    end: str | pd.Timestamp | None = None,
    batch_size: int = DEFAULT_BATCH,
    pause: float = DEFAULT_PAUSE,
) -> pd.DataFrame:
    """
    Изтегля adjusted close prices за списък ticker-и.

    Връща DataFrame: index = date, columns = tickers, values = adjusted close.
    Ticker-и без данни биват пропуснати (warning се изписва).
    """
    if end is None:
        end = pd.Timestamp.today().normalize()

    frames: list[pd.DataFrame] = []
    failed: list[str] = []

    for i in range(0, len(tickers), batch_size):
        chunk = tickers[i : i + batch_size]
        try:
            data = yf.download(
                chunk,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                threads=True,
                group_by="column",
            )
        except Exception as e:
            print(f"[batch {i}] download failed: {e}")
            failed.extend(chunk)
            continue

        if data.empty:
            failed.extend(chunk)
            continue

        # При single ticker, yfinance връща DataFrame без MultiIndex.
        if len(chunk) == 1:
            close = data[["Close"]].rename(columns={"Close": chunk[0]})
        else:
            if isinstance(data.columns, pd.MultiIndex):
                close = data["Close"]
            else:
                close = data

        close = close.dropna(how="all")
        frames.append(close)

        if i + batch_size < len(tickers):
            time.sleep(pause)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, axis=1).sort_index()
    # Махам колони, които са изцяло NaN
    combined = combined.dropna(axis=1, how="all")

    received = set(combined.columns)
    missing = [t for t in tickers if t not in received]
    if missing:
        print(f"Missing data for {len(missing)} tickers (first 10): {missing[:10]}")

    return combined
