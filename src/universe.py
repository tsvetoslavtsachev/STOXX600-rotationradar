"""
STOXX 600 universe — извлича от iShares EXSA ETF holdings CSV.

Wikipedia не е надежден източник за STOXX 600 (фрагментирани change history).
iShares EXSA UCITS ETF е sister-фонд на STOXX Europe 600 индекса; неговото
daily-обновявано CSV holdings файл е canonical източник.

Replikира методологията от съществуващия stoxx600-momentumrank repo.

CSV формат (German):
  Emittententicker, Name, Sektor, Anlageklasse, Marktwert, Gewichtung (%),
  Nominalwert, Nominale, Kurs, Standort, Börse, Marktwährung
"""

from __future__ import annotations

import io
import re

import pandas as pd
import requests

ISHARES_URL = (
    "https://www.ishares.com/de/privatanleger/de/produkte/251931/"
    "ishares-stoxx-europe-600-ucits-etf-de-fund/1478358465952.ajax"
    "?fileType=csv&fileName=EXSA_holdings&dataType=fund"
)

# German sector names → English (GICS-style)
SECTOR_TRANSLATIONS = {
    "IT": "Information Technology",
    "Informationstechnologie": "Information Technology",
    "Financials": "Financials",
    "Finanzen": "Financials",
    "Gesundheitsversorgung": "Health Care",
    "Healthcare": "Health Care",
    "Nichtzyklische Konsumgüter": "Consumer Staples",
    "Basiskonsumgüter": "Consumer Staples",
    "Zyklische Konsumgüter": "Consumer Discretionary",
    "Konsumgüter": "Consumer Discretionary",
    "Energie": "Energy",
    "Industrie": "Industrials",
    "Versorger": "Utilities",
    "Telekommunikation": "Communication Services",
    "Telekommunikationsdienste": "Communication Services",
    "Kommunikationsdienste": "Communication Services",
    "Kommunikation": "Communication Services",
    "Grundstoffe": "Materials",
    "Werkstoffe": "Materials",
    "Materialien": "Materials",
    "Immobilien": "Real Estate",
    "Cash und/oder Derivate": "Cash",
}

# Exchange code → Yahoo Finance suffix
EXCHANGE_SUFFIX = {
    "London Stock Exchange": ".L",
    "Xetra": ".DE",
    "Deutsche Boerse Xetra": ".DE",
    "Frankfurt Stock Exchange": ".F",
    "Nyse Euronext - Euronext Paris": ".PA",
    "Euronext Paris": ".PA",
    "Euronext Amsterdam": ".AS",
    "Nyse Euronext - Euronext Amsterdam": ".AS",
    "Euronext Brussels": ".BR",
    "Nyse Euronext - Euronext Brussels": ".BR",
    "Euronext Lisbon": ".LS",
    "Nyse Euronext - Euronext Lisbon": ".LS",
    "Euronext Dublin": ".IR",
    "Irish Stock Exchange - All Market": ".IR",
    "Borsa Italiana": ".MI",
    "Bolsa De Madrid": ".MC",
    "SIX Swiss Exchange": ".SW",
    "Swiss Exchange": ".SW",
    "Stockholm Stock Exchange": ".ST",
    "Nasdaq Stockholm": ".ST",
    "Nasdaq Omx Nordic": ".ST",
    "Nasdaq Helsinki": ".HE",
    "Nasdaq Omx Helsinki Ltd.": ".HE",
    "Nasdaq Copenhagen": ".CO",
    "Omx Nordic Exchange Copenhagen A/S": ".CO",
    "Oslo Stock Exchange": ".OL",
    "Oslo Bors": ".OL",
    "Oslo Bors Asa": ".OL",
    "Wiener Boerse": ".VI",
    "Wiener Boerse Ag": ".VI",
    "Vienna Stock Exchange": ".VI",
    "Athens Exchange": ".AT",
    "Warsaw Stock Exchange": ".WA",
    "Warsaw Stock Exchange/Equities/Main Market": ".WA",
}

# Country (Standort) → fallback Yahoo suffix when exchange is unknown
COUNTRY_FALLBACK_SUFFIX = {
    "Vereinigtes Königreich": ".L",
    "Großbritannien": ".L",
    "Deutschland": ".DE",
    "Frankreich": ".PA",
    "Niederlande": ".AS",
    "Belgien": ".BR",
    "Schweiz": ".SW",
    "Italien": ".MI",
    "Spanien": ".MC",
    "Schweden": ".ST",
    "Finnland": ".HE",
    "Dänemark": ".CO",
    "Norwegen": ".OL",
    "Österreich": ".VI",
    "Irland": ".IR",
    "Portugal": ".LS",
    "Polen": ".WA",
    "Griechenland": ".AT",
}

# Manual ticker overrides per exchange suffix.
# Replikира TICKER_OVERRIDES_BY_EXCHANGE от stoxx600-momentumrank repo-то.
TICKER_OVERRIDES_BY_EXCHANGE: dict[str, dict[str, str]] = {
    ".SW": {
        "ROP": "ROG",  # Roche — iShares ticker несъвпада с Yahoo
    },
}

# Регекс за detect-ване на dual-class share class designation в name полето.
# Покрива форми: "CLASS A", "CLASS B", "CL B", "CLASS B SHS", "B SHS", "SERIES B".
_CLASS_LETTER_RE = re.compile(
    r"\b(?:CLASS|CL|SERIES|CLASSE)?\s*([A-C])\s*(?:SHS|SHARES|CLASS)?\s*$",
    re.IGNORECASE,
)


def _parse_german_number(s) -> float | None:
    """Конвертира German formatted number ('1.234,56') → 1234.56."""
    if pd.isna(s) or s == "":
        return None
    txt = str(s).strip().replace(".", "").replace(",", ".")
    try:
        return float(txt)
    except (ValueError, TypeError):
        return None


def _detect_class_letter(name: str) -> str | None:
    """
    От name на акцията (напр. "VOLVO CLASS B") извлича share class буквата.
    Връща 'A'/'B'/'C' или None.
    """
    if not name:
        return None
    name_upper = name.upper().strip()
    # Прекратяваме при думи "PLC", "AG", "SA" etc — те не са class designation
    name_upper = re.sub(
        r"\b(PLC|AG|SA|NV|N\.V\.|SE|SPA|S\.A\.|HOLDING[S]?|GROUP|N|PS|PAR)\b",
        " ",
        name_upper,
    ).strip()
    m = re.search(r"\b(?:CLASS|CL|SERIES)\s+([A-C])\b", name_upper)
    if m:
        return m.group(1)
    # "VOLVO B", "ASSA ABLOY B" — последна samostoyatelna буква A/B/C
    m = re.search(r"\b([A-C])\s*$", name_upper)
    if m:
        return m.group(1)
    return None


def _ticker_to_yahoo(ticker: str, exchange: str, country: str, name: str = "") -> str | None:
    """Конвертира iShares ticker + борса/страна към Yahoo Finance ticker."""
    if not ticker:
        return None

    # Normalize: spaces, slashes, internal dots → dashes; strip trailing dots
    base = ticker.strip().upper()
    base = base.replace(" ", "-").replace("/", "-")
    base = base.rstrip(".")
    base = base.replace(".", "-")

    # Override (per exchange suffix) — checked BEFORE class-letter logic
    suffix = EXCHANGE_SUFFIX.get(exchange) or COUNTRY_FALLBACK_SUFFIX.get(country)
    if not suffix:
        return None

    overrides = TICKER_OVERRIDES_BY_EXCHANGE.get(suffix, {})
    if base in overrides:
        base = overrides[base]
        return f"{base}{suffix}"

    # Nordic dual-class fix: iShares "VOLVB" → Yahoo "VOLV-B"
    # Detect когато name индикира class A/B/C и ticker завършва със същата буква.
    class_letter = _detect_class_letter(name)
    if class_letter and len(base) >= 3 and base.endswith(class_letter) and "-" not in base:
        # Не вмъкваме dash ако вече е там, и ако ticker-ът е напр. "AB" (твърде къс)
        new_base = base[:-1] + "-" + class_letter
        base = new_base

    return f"{base}{suffix}"


def fetch_ishares_csv() -> str:
    """Изтегля surovия CSV от iShares."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0"
        ),
        "Accept": "text/csv,application/csv,text/plain,*/*",
    }
    resp = requests.get(ISHARES_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_constituents(csv_text: str) -> pd.DataFrame:
    """
    Парсира iShares CSV-то.
    Връща DataFrame: yahoo_ticker, ishares_ticker, name, gics_sector,
                     country, exchange, currency, weight_pct, local_price.
    """
    lines = csv_text.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Emittententicker"):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Не намерих 'Emittententicker' header в CSV-то.")

    data_csv = "\n".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(data_csv), dtype=str)

    df = df[df["Anlageklasse"].str.strip() == "Aktien"].copy()

    df.rename(
        columns={
            "Emittententicker": "ishares_ticker",
            "Name": "name",
            "Sektor": "sector_de",
            "Standort": "country",
            "Börse": "exchange",
            "Marktwährung": "currency",
            "Gewichtung (%)": "weight_pct_str",
            "Kurs": "local_price_str",
        },
        inplace=True,
    )

    df["weight_pct"] = df["weight_pct_str"].apply(_parse_german_number)
    df["local_price"] = df["local_price_str"].apply(_parse_german_number)
    # Trim NBSP и whitespace преди sector lookup (iShares CSV понякога има \xa0)
    df["sector_de"] = df["sector_de"].astype(str).str.replace("\xa0", " ", regex=False).str.strip()
    df["exchange"] = df["exchange"].astype(str).str.replace("\xa0", " ", regex=False).str.strip()
    df["country"] = df["country"].astype(str).str.replace("\xa0", " ", regex=False).str.strip()
    df["gics_sector"] = df["sector_de"].map(SECTOR_TRANSLATIONS).fillna(df["sector_de"])
    df["yahoo_ticker"] = df.apply(
        lambda r: _ticker_to_yahoo(
            r["ishares_ticker"], r["exchange"], r["country"], r.get("name", "") or ""
        ),
        axis=1,
    )

    out = df[
        [
            "yahoo_ticker", "ishares_ticker", "name", "gics_sector",
            "country", "exchange", "currency", "weight_pct", "local_price",
        ]
    ].copy()
    out = out.dropna(subset=["yahoo_ticker"])
    out = out[out["yahoo_ticker"] != ""]

    # Dedup yahoo_ticker — iShares понякога има същия ticker двукратно
    # (различни share classes които се мап-ват към един Yahoo ticker).
    # Запазваме реда с по-голям weight_pct.
    out = out.sort_values("weight_pct", ascending=False).drop_duplicates(
        subset=["yahoo_ticker"], keep="first"
    )
    return out.reset_index(drop=True)


def fetch_constituents() -> pd.DataFrame:
    """High-level wrapper: fetch + parse iShares CSV."""
    csv_text = fetch_ishares_csv()
    return parse_constituents(csv_text)


# Compatibility shims за SP500-style API (за да не пренаписвам всичко в downstream-а):
def fetch_current_constituents() -> pd.DataFrame:
    """
    Връща DataFrame: ticker (= yahoo_ticker), name, gics_sector, gics_sub_industry.
    Sub-industry не идва от iShares CSV — пълним със sector като fallback.
    """
    df = fetch_constituents()
    out = pd.DataFrame({
        "ticker": df["yahoo_ticker"],
        "name": df["name"],
        "gics_sector": df["gics_sector"],
        "gics_sub_industry": df["gics_sector"],
    })
    return out.reset_index(drop=True)


def fetch_full_universe(years_back: int = 5) -> pd.DataFrame:
    """
    Връща current constituents + ETF weight за size proxy.
    is_current винаги True (iShares няма removed-tickers history тук).
    """
    df = fetch_constituents()
    out = pd.DataFrame({
        "ticker": df["yahoo_ticker"],
        "name": df["name"],
        "gics_sector": df["gics_sector"],
        "gics_sub_industry": df["gics_sector"],
        "country": df["country"],
        "exchange": df["exchange"],
        "currency": df["currency"],
        "weight_pct": df["weight_pct"],
        "is_current": True,
        "removed_date": pd.NaT,
    })
    return out.reset_index(drop=True)


if __name__ == "__main__":
    universe = fetch_constituents()
    print(f"Total constituents: {len(universe)}")
    print(f"Sectors: {sorted(universe['gics_sector'].dropna().unique())}")
    print(f"Exchanges: {sorted(universe['exchange'].dropna().unique())}")
    print()
    print("Top 10 by weight:")
    top = universe.nlargest(10, "weight_pct")[["yahoo_ticker", "name", "gics_sector", "country", "weight_pct"]]
    print(top.to_string(index=False))
