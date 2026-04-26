# STOXX 600 Rotation Radar

> **"Купи лидерите, особено когато временно отслабнат."** Не "хвани падналите ангели".

V2 на rotation radar архитектурата приложена върху **STOXX Europe 600**. Identify Stable Winners (лидери, които продължават да водят) и Quality Dip (лидери, които временно отслабват) в европейския пазар.

Sister repo на [SP500-rotationradar](https://github.com/tsvetoslavtsachev/SP500-rotationradar). Същият движок, същите tabs, същата логика — но за европейския universe.

## Какво е различно от SP500 версията

| | SP500 | STOXX 600 |
|---|---|---|
| Universe source | Wikipedia | **iShares EXSA UCITS ETF holdings CSV** |
| Universe size | 503 акции | ~544 (от 603, след yfinance fetch errors) |
| Размер proxy | Market cap (USD) | **ETF weight %** (избягва multi-currency проблеми) |
| Sub-Industry | GICS Sub-Industry | Не е достъпно (не идва от iShares CSV) |
| Sector | GICS via Wikipedia | GICS via iShares (German labels преведени) |
| Country/Exchange | Само US | 17 европейски страни, 16 борси |

## Universe source — iShares EXSA

Списъкът на 600-те STOXX Europe 600 constituent-и идва от **iShares Core EXSA UCITS ETF (DE)** holdings CSV — daily-обновяван официален source. Този подход е проверен от съществуващия stoxx600-momentumrank repo и избягва нестабилностите на Wikipedia за европейски индекси.

CSV дава за всеки ticker: GICS sector, country, exchange, currency, **ETF weight %**, и local price. Няма нужда от market cap fetch (избягваме multi-currency kerfuffle).

## Архитектура (същата като SP500)

### Слой 1 — Signal Engine V2
Pure 12-1 momentum (Jegadeesh-Titman 1993), нормализиран като **sector-relative z-score**:
```
mom_12_1 = price[t-21] / price[t-252] - 1
sector_zscore = (mom_12_1 - sector_mean) / sector_std
```

### Слой 2 — ΔRank Engine
- `base_rank_6m` = средна percentile_rank за 6 месеца назад (excluding последния)
- `delta_1m`, `delta_3m` = промяна в ранга
- 4-quadrant класификация (прагове p20/p80)

### Слой 3 — Sector Context
GICS Sector от iShares CSV (немски лейбъли, преведени на английски).
Sub-industry липсва (не е в CSV-то).

## UI Tabs

1. **🎯 Stable Winners (1m)** — primary watchlist
2. **🎯 Stable Winners (3m)** — стабилно тестваните
3. **💎 Quality Dip (1m)** — Nike-style buy points
4. **💎 Quality Dip (3m)** — по-сериозни pullbacks
5. **⚠ Faded Bounces** — contrarian warning
6. **📈 Current Strength** — топ 50 по абсолютен 12-1 momentum
7. **📋 Rank All** — пълно подреждане по Score
8. **📊 Universe Screener** — пълна таблица с всички акции
9. **🌡 Sector Heatmap**

## Setup

```bash
pip install -r requirements.txt

# Еднократен 5y backfill (~10-15 минути)
python scripts/backfill_history.py

# Daily incremental update (за GitHub Actions)
python scripts/daily_update.py

# Tests
pytest tests/ -v
```

## Caveats

- **Survivorship bias** — iShares CSV съдържа само текущи constituent-и, не historic. Backtest резултатите ще са оптимистично-биасирани.
- **yfinance coverage** — за някои small-cap европейски акции yfinance няма пълна 5-годишна история. Тези ticker-и пропадат и не се класират.
- **Multi-currency** — цените в CSV-то са в local currency. ΔRank работи на ratio-based metrics, не зависи от FX. Но абсолютни return сравнения между UK (GBP/GBp) и Eurozone (EUR) акции трябва да се правят с разбиране.
- **Sector translations** — German GICS лейбъли са преведени ръчно. Ако iShares въведе нов сектор (рядко), ще трябва обновяване в `src/universe.py`.

## Свързани материали

- [SP500-rotationradar](https://github.com/tsvetoslavtsachev/SP500-rotationradar) — US версията
- [stoxx600-momentumrank](https://github.com/tsvetoslavtsachev/stoxx600-momentumrank) — старият "current strength view" (запазва се)
- iShares EXSA ETF: https://www.ishares.com/de/privatanleger/de/produkte/251931/
