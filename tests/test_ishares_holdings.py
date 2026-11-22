"""Regression: iShares EXSA holdings CSV format (new site, from 2026-09-23).

The fixture is the real latest-holdings.csv downloaded 2026-10-10 (as of 08.Okt.2026).
"""
from pathlib import Path

import pytest

from src.ishares_holdings import IsharesHoldingsError, decode_bytes, parse_holdings_csv

FIXTURE = Path(__file__).parent / "fixtures" / "exsa_holdings_2026-10-08.csv"


def _sample() -> str:
    return decode_bytes(FIXTURE.read_bytes())


def test_new_format_parses_all_equities():
    df = parse_holdings_csv(_sample())
    assert len(df) == 602
    assert set(df["Anlageklasse"]) == {"Aktien"}
    assert df.loc[0, "Emittententicker"] == "ASML"
    assert df.loc[0, "Börse"] == "Euronext Amsterdam"
    assert "Vereinigtes Königreich" in set(df["Standort"])


def test_bom_quoted_header_and_semicolons():
    text = _sample()
    lines = text.splitlines()
    i = next(n for n, ln in enumerate(lines) if ln.startswith("Emittententicker"))
    lines[i] = ",".join(f'"{c}"' for c in lines[i].split(","))
    df = parse_holdings_csv("\ufeff" + "\n".join(lines))
    assert len(df) == 602
    semi = "\n".join(ln.replace('","', '";"') for ln in lines[i + 1:])
    df2 = parse_holdings_csv(lines[i].replace(",", ";") + "\n" + semi)
    assert len(df2) == 602


def test_english_headers_are_aliased():
    text = _sample()
    de = "Emittententicker,Name,Sektor,Anlageklasse,Marktwert,Gewichtung (%),Nominalwert,Nominale,Kurs,Standort,Börse,Marktwährung"
    en = "Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Shares,Price,Location,Exchange,Market Currency"
    assert de in text
    df = parse_holdings_csv(text.replace(de, en))
    assert len(df) == 602 and "Emittententicker" in df.columns


def test_html_page_gives_clear_error():
    html = "<!doctype html>\n<html><head><title>iShares STOXX Europe 600 UCITS ETF (DE) | EXSA | iShares</title>"
    with pytest.raises(IsharesHoldingsError, match=r"HTML page titled 'iShares STOXX Europe 600"):
        parse_holdings_csv(html, "text/html; charset=utf-8")


def test_truncated_file_is_rejected():
    head = "\n".join(_sample().splitlines()[:50])
    with pytest.raises(IsharesHoldingsError, match="only 47 equity rows"):
        parse_holdings_csv(head)


def test_cp1252_bytes_decode():
    assert decode_bytes("Börse".encode("cp1252")) == "Börse"
    assert decode_bytes("\ufeffBörse".encode("utf-8")) == "Börse"


def test_every_sector_in_sample_is_translated():
    """Guard: an iShares sector label missing from SECTOR_TRANSLATIONS used to
    pass through in German silently ('Finanzwesen' until 2026-10-10)."""
    from src.universe import SECTOR_TRANSLATIONS, parse_constituents

    df = parse_holdings_csv(_sample())
    assert set(df["Sektor"]) <= set(SECTOR_TRANSLATIONS)
    sectors = set(parse_constituents(_sample())["gics_sector"])
    assert "Financials" in sectors and "Finanzwesen" not in sectors


CH_FIXTURE = Path(__file__).parent / "fixtures" / "exsa_holdings_ch_en_2026-11-19.csv"
XTR_FIXTURE = Path(__file__).parent / "fixtures" / "xtrackers_stoxx600_2026-11-22.xlsx"


def test_swiss_english_file_gives_german_values():
    """The CH professional site (first URL since 22.11.2026) has English
    labels and '1’288.20' numbers; callers must see the DE-site form."""
    from src.universe import COUNTRY_TRANSLATIONS, SECTOR_TRANSLATIONS, parse_constituents

    df = parse_holdings_csv(decode_bytes(CH_FIXTURE.read_bytes()))
    assert len(df) == 601
    asml = df[df["Emittententicker"] == "ASML"].iloc[0]
    assert asml["Gewichtung (%)"] == "3,75" and asml["Kurs"] == "1288,20"
    assert set(df["Sektor"]) <= set(SECTOR_TRANSLATIONS)
    assert set(df["Standort"]) <= set(COUNTRY_TRANSLATIONS) | {"Portugal"}
    cons = parse_constituents(decode_bytes(CH_FIXTURE.read_bytes()))
    a = cons[cons["ishares_ticker"] == "ASML"].iloc[0]
    assert abs(a["weight_pct"] - 3.75) < 1e-9 and abs(a["local_price"] - 1288.20) < 1e-9


def test_german_numbers_unchanged_in_value():
    df = parse_holdings_csv(_sample())
    assert df["Gewichtung (%)"].iloc[0] == "4,84" and df["Kurs"].iloc[0] == "1630,60"


def test_crosscheck_with_xtrackers_agrees():
    import pandas as pd
    from src.ishares_holdings import crosscheck

    ish = parse_holdings_csv(decode_bytes(CH_FIXTURE.read_bytes()))
    raw = pd.read_excel(XTR_FIXTURE, header=3)
    res = crosscheck(ish, raw)
    assert res["ok"], res
    assert res["matched_weight_pct"] >= 95


def test_crosscheck_flags_a_truncated_file():
    import pandas as pd
    from src.ishares_holdings import crosscheck

    ish = parse_holdings_csv(decode_bytes(CH_FIXTURE.read_bytes()))
    raw = pd.read_excel(XTR_FIXTURE, header=3)
    assert not crosscheck(ish.iloc[:450], raw)["ok"]
