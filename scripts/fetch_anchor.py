#!/usr/bin/env python3
"""
fetch_anchor.py — Israeli defense industry EDGAR 6-K pipeline for HARPY

Source: SEC EDGAR Form 6-K filings from Israeli defense contractors.
        Elbit Systems (CIK 1027664) files a 6-K for every material contract win,
        typically within 24-48h of announcement. No authentication required.

Each signal = one contract announcement. Title and value extracted from the
press release exhibit. Buyer country extracted where disclosed.

No API key required. Rate limit: 10 req/s per SEC EDGAR fair-use guidelines.

Usage:
  python fetch_anchor.py             # last 45 days
  python fetch_anchor.py --backfill  # last 365 days
"""

import argparse
import html
import json
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone

# Literal dollar-sign character for regex patterns — r"\$" behaves as end-anchor
# in Python 3.9's re module; use [$] (character class) instead.
_USD = "[$]"
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_ARCHIVE     = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/"

LOOKBACK_DAYS         = 45
LOOKBACK_DAYS_BACKFILL = 365
REQUEST_DELAY         = 0.15   # stay under SEC 10 req/s guideline

# Israeli defense contractors with SEC filings
COMPANIES = [
    {"name": "Elbit Systems",    "cik": "0001027664", "iso": "IL"},
]

REPO_ROOT    = Path(__file__).parent.parent
SIGNALS_PATH = REPO_ROOT / "data" / "anchor_signals.json"
IL_PROFILE   = REPO_ROOT / "data" / "profiles" / "IL.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_il_score():
    if IL_PROFILE.exists():
        try:
            return float(json.loads(IL_PROFILE.read_text()).get("structural_interest_score", 8))
        except Exception:
            pass
    return 8.0


def load_existing(path: Path):
    if path.exists():
        try:
            data = json.loads(path.read_text())
            signals = data.get("signals", [])
            seen = {s["accession"] for s in signals if "accession" in s}
            return signals, seen
        except Exception:
            pass
    return [], set()


def http_get(url: str, retries: int = 3) -> bytes:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "harpy research@harpy.io",
                "Accept": "application/json, text/html, */*",
            })
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            raise
    raise RuntimeError(f"Failed after {retries} attempts: {url}")


def get_filing_docs(cik_num: str, accession: str) -> list:
    """Return list of document filenames in this filing's EDGAR directory."""
    acc_nodash = accession.replace("-", "")
    url = EDGAR_ARCHIVE.format(cik=cik_num, acc_nodash=acc_nodash)
    try:
        html = http_get(url).decode("utf-8", errors="ignore")
        return re.findall(r'href="/Archives/edgar/data/\d+/\d+/([^"]+\.htm)"', html)
    except Exception:
        return []


def fetch_exhibit(cik_num: str, accession: str, docs: list) -> str:
    """Fetch the press release exhibit from the filing. Returns plain text."""
    acc_nodash = accession.replace("-", "")
    # Prefer files named exhibit*, ex99*, ex-99*, zk* (Elbit pattern)
    priority = sorted(docs, key=lambda d: (
        0 if re.match(r"exhibit[_-]?1", d, re.I) else
        1 if re.match(r"ex.?99", d, re.I) else
        2 if d.lower().startswith("zk") else
        3
    ))
    for doc in priority:
        # Skip the cover 6-K itself
        if doc.lower().startswith("cover"):
            continue
        url = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_nodash}/{doc}"
        try:
            time.sleep(REQUEST_DELAY)
            raw = http_get(url).decode("utf-8", errors="ignore")
            text = re.sub(r"<[^>]+>", " ", raw)
            text = html.unescape(text)           # decode &#160; &#8217; etc.
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) > 200:
                return text
        except Exception:
            continue
    return ""


def extract_value_usd(text: str):
    """Pull the contract dollar value from the press release text.
    Searches near award/contract language first to avoid earnings data."""
    # Look for the value in award context first (most accurate)
    award_patterns = [
        rf"(?:awarded?|contract[s]?)[^.{{0,200}}]{_USD}\s*([\d,]+(?:\.\d+)?)\s*(billion|million)",
        rf"aggregate[d]?\s+value[^.{{0,100}}]{_USD}\s*([\d,]+(?:\.\d+)?)\s*(million|billion)",
        rf"approximately\s+{_USD}\s*([\d,]+(?:\.\d+)?)\s*(billion|million)",
        rf"{_USD}\s*([\d,]+(?:\.\d+)?)\s*(billion|million)",
    ]
    for pat in award_patterns:
        for m in re.finditer(pat, text, re.I):
            raw = m.group(1).replace(",", "")
            val = float(raw)
            unit = m.group(2).lower()
            val *= 1e9 if unit == "billion" else 1e6
            if val >= 1e6:  # skip values under $1M
                return val
    return None


def extract_title(text: str, company_name: str) -> str:
    """Pull the headline from the press release (first bold/header line)."""
    # Look for the announcement headline — typically right after the boilerplate
    m = re.search(
        rf"{re.escape(company_name)}\s+(?:Awarded|Wins?|Secures?|Announces?|Selected|Signs?)[^\n]{{10,200}}",
        text, re.I
    )
    if m:
        title = re.sub(r"\s+", " ", m.group(0)).strip()
        # Strip city/date boilerplate: "… Haifa, Israel, …" or "… Tel Aviv, …"
        title = re.sub(r"\s+(?:Haifa|Tel Aviv|Jerusalem|Herzliya)[,.].*", "", title, flags=re.I)
        return title[:150]
    # Fallback: first sentence mentioning a dollar amount
    m2 = re.search(rf"[A-Z][^.{{20,150}}]{_USD}[^.]+[.]", text)
    if m2:
        return re.sub(r"\s+", " ", m2.group(0)).strip()[:150]
    return f"{company_name} contract announcement"


def extract_buyer_iso(text: str) -> str:
    """Try to identify buyer country ISO2 from the press release text."""
    # Named countries
    country_map = {
        r"\bIsrael(?:i)?\b": "IL",
        r"\bUnited States\b|\bU\.S\.\s": "US",
        r"\bGermany\b|\bGerman\b": "DE",
        r"\bIndia\b|\bIndian\b": "IN",
        r"\bAustralia\b|\bAustralian\b": "AU",
        r"\bUkraine\b|\bUkrainian\b": "UA",
        r"\bBrazil\b|\bBrazilian\b": "BR",
        r"\bPhilippines\b|\bFilipino\b": "PH",
        r"\bSingapore\b": "SG",
        r"\bNATO\b": "ZZ",  # NATO collective
        r"\bIsrael Defense Forces\b|\bIDF\b": "IL",
    }
    for pattern, iso in country_map.items():
        if re.search(pattern, text):
            return iso
    return "IL"  # default: Israeli company, probably Israeli or undisclosed buyer


def make_description(text: str, accession: str) -> str:
    parts = []
    # First substantive sentence — skip city/date lines and boilerplate headers
    sentences = re.split(r"(?<=[.!?])\s+", text)
    for s in sentences[1:6]:
        s = s.strip()
        if (len(s) > 60
                and not re.match(r"(?:Haifa|Tel Aviv|Jerusalem|Herzliya|EX-|EXHIBIT)", s, re.I)
                and not re.match(r"[A-Z]{2,}\s+[A-Z]{2,}", s)):  # skip ALL-CAPS header lines
            parts.append(s[:300])
            break
    parts.append(f"EDGAR: {accession}")
    return " | ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()

    lookback = LOOKBACK_DAYS_BACKFILL if args.backfill else LOOKBACK_DAYS
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback)).strftime("%Y-%m-%d")
    print(f"[anchor] lookback={lookback}d  cutoff={cutoff}")

    il_score = load_il_score()
    existing_signals, seen_ids = load_existing(SIGNALS_PATH)
    print(f"[anchor] existing={len(existing_signals)}  seen={len(seen_ids)}")

    new_signals = []

    for company in COMPANIES:
        name  = company["name"]
        cik   = company["cik"]
        iso   = company["iso"]
        cik_num = cik.lstrip("0")

        print(f"[anchor] fetching {name} (CIK {cik})")
        try:
            sub_url = EDGAR_SUBMISSIONS.format(cik=cik)
            sub_data = json.loads(http_get(sub_url))
        except Exception as e:
            print(f"[anchor] ERROR fetching submissions for {name}: {e}", file=sys.stderr)
            continue

        filings = sub_data["filings"]["recent"]
        forms       = filings["form"]
        dates       = filings["filingDate"]
        accessions  = filings["accessionNumber"]

        for i, form in enumerate(forms):
            if form != "6-K":
                continue
            filing_date = dates[i]
            if filing_date < cutoff:
                break  # submissions are newest-first; stop once past window

            accession = accessions[i]
            if accession in seen_ids:
                continue

            time.sleep(REQUEST_DELAY)
            docs = get_filing_docs(cik_num, accession)
            if not docs:
                continue

            time.sleep(REQUEST_DELAY)
            text = fetch_exhibit(cik_num, accession, docs)
            if not text or len(text) < 100:
                continue

            # Only include filings where the headline is a contract award
            # (excludes earnings releases, annual reports, prospectuses, etc.)
            headline_pat = rf"{re.escape(name)}\s+(?:Awarded|Wins?|Secures?|Signs?)[^\n]{{10,200}}"
            if not re.search(headline_pat, text, re.I):
                continue

            value_usd = extract_value_usd(text)
            title     = f"IL — {extract_title(text, name)}"
            buyer_iso = extract_buyer_iso(text)
            desc      = make_description(text, accession)

            sig = {
                "iso":         buyer_iso if buyer_iso != "IL" else iso,
                "source":      "anchor_budget",
                "signal_date": filing_date,
                "title":       title,
                "value_usd":   value_usd,
                "description": desc,
                "raw_score":   il_score,
                "weight":      1.0,
                "accession":   accession,
            }
            new_signals.append(sig)
            seen_ids.add(accession)
            print(f"[anchor]   {filing_date}  {value_usd and f'${value_usd/1e6:.0f}M' or '?'}  {title[:70]}")

    print(f"[anchor] new signals: {len(new_signals)}")
    if not new_signals:
        print("[anchor] nothing new — exiting")
        return

    all_signals = existing_signals + new_signals
    SIGNALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SIGNALS_PATH.write_text(json.dumps({"signals": all_signals}, indent=2, ensure_ascii=False))
    print(f"[anchor] wrote {len(all_signals)} total to {SIGNALS_PATH}")


if __name__ == "__main__":
    main()
