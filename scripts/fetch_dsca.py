#!/usr/bin/env python3
"""
fetch_dsca.py — DSCA Major Arms Sales congressional notification scraper

Source: https://www.dsca.mil/Press-Media/Major-Arms-Sales/Major-Arms-Sales-Library
Pagination: ?igpage=N (last page discovered at runtime from "LAST" link)
Strategy: scrape every page, collect all records, filter by target country client-side.

Usage:
  python fetch_dsca.py --probe              # dump page 1 HTML + pagination info, exit
  python fetch_dsca.py                      # scrape all pages, write data/dsca_notifications.json
  python fetch_dsca.py --backtest           # filter saved data to 2021-02-24→2022-02-24, print
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DSCA_LIBRARY_URL = "https://www.dsca.mil/Press-Media/Major-Arms-Sales/Major-Arms-Sales-Library"
REQUEST_DELAY = 1.5  # seconds between page requests

# Comprehensive country name → ISO alpha-2 map.
# Covers all countries that commonly appear in DSCA arms sale notifications.
# Keys are uppercase as they appear in DSCA filenames.
DSCA_COUNTRY_MAP = {
    "AFGHANISTAN": "AF", "ALBANIA": "AL", "ALGERIA": "DZ", "ANGOLA": "AO",
    "ARGENTINA": "AR", "ARMENIA": "AM", "AUSTRALIA": "AU", "AUSTRIA": "AT",
    "AZERBAIJAN": "AZ", "BAHRAIN": "BH", "BANGLADESH": "BD", "BELGIUM": "BE",
    "BELIZE": "BZ", "BENIN": "BJ", "BOTSWANA": "BW", "BRAZIL": "BR",
    "BULGARIA": "BG", "BURKINA FASO": "BF", "CAMBODIA": "KH", "CAMEROON": "CM",
    "CANADA": "CA", "CHAD": "TD", "CHILE": "CL", "COLOMBIA": "CO",
    "CROATIA": "HR", "CZECH REPUBLIC": "CZ", "DENMARK": "DK", "DJIBOUTI": "DJ",
    "DOMINICAN REPUBLIC": "DO", "ECUADOR": "EC", "EGYPT": "EG",
    "EL SALVADOR": "SV", "ERITREA": "ER", "ESTONIA": "EE", "ETHIOPIA": "ET",
    "FINLAND": "FI", "FRANCE": "FR", "GEORGIA": "GE", "GERMANY": "DE",
    "GHANA": "GH", "GREECE": "GR", "GUATEMALA": "GT", "HONDURAS": "HN",
    "HUNGARY": "HU", "INDIA": "IN", "INDONESIA": "ID", "IRAQ": "IQ",
    "IRELAND": "IE", "ISRAEL": "IL", "ITALY": "IT", "JAMAICA": "JM",
    "JAPAN": "JP", "JORDAN": "JO", "KAZAKHSTAN": "KZ", "KENYA": "KE",
    "KOSOVO": "XK", "KUWAIT": "KW", "KYRGYZSTAN": "KG", "LATVIA": "LV",
    "LEBANON": "LB", "LIBERIA": "LR", "LIBYA": "LY", "LITHUANIA": "LT",
    "LUXEMBOURG": "LU", "MALAYSIA": "MY", "MALI": "ML", "MAURITANIA": "MR",
    "MEXICO": "MX", "MOLDOVA": "MD", "MONGOLIA": "MN", "MONTENEGRO": "ME",
    "MOROCCO": "MA", "MOZAMBIQUE": "MZ", "NAMIBIA": "NA", "NETHERLANDS": "NL",
    "NEW ZEALAND": "NZ", "NIGER": "NE", "NIGERIA": "NG", "NORTH MACEDONIA": "MK",
    "NORWAY": "NO", "OMAN": "OM", "PAKISTAN": "PK", "PANAMA": "PA",
    "PAPUA NEW GUINEA": "PG", "PERU": "PE", "PHILIPPINES": "PH", "POLAND": "PL",
    "PORTUGAL": "PT", "QATAR": "QA", "ROMANIA": "RO", "RWANDA": "RW",
    "SAUDI ARABIA": "SA", "SENEGAL": "SN", "SERBIA": "RS", "SINGAPORE": "SG",
    "SLOVAKIA": "SK", "SLOVENIA": "SI", "SOMALIA": "SO", "SOUTH AFRICA": "ZA",
    "SOUTH KOREA": "KR", "SOUTH SUDAN": "SS", "SPAIN": "ES", "SRI LANKA": "LK",
    "SWEDEN": "SE", "SWITZERLAND": "CH", "TAIWAN": "TW", "TAJIKISTAN": "TJ",
    "TANZANIA": "TZ", "THAILAND": "TH", "TIMOR-LESTE": "TL", "TOGO": "TG",
    "TRINIDAD AND TOBAGO": "TT", "TUNISIA": "TN", "TURKEY": "TR",
    "TURKMENISTAN": "TM", "UGANDA": "UG", "UKRAINE": "UA",
    "UNITED ARAB EMIRATES": "AE", "UAE": "AE", "UNITED KINGDOM": "GB",
    "URUGUAY": "UY", "UZBEKISTAN": "UZ", "VIETNAM": "VN", "YEMEN": "YE",
    "ZAMBIA": "ZM", "ZIMBABWE": "ZW",
    # DSCA-specific abbreviations and alternate forms
    "REPUBLIC OF KOREA": "KR", "ROK": "KR",
    "REPUBLIC OF THE PHILIPPINES": "PH",
    "CZECH": "CZ",
    "KINGDOM OF SAUDI ARABIA": "SA",
    "KINGDOM OF BAHRAIN": "BH",
    "HASHEMITE KINGDOM OF JORDAN": "JO",
    "NSPA": "XN",   # NATO Support and Procurement Agency — no ISO, use XN
    "NATO": "XN",
}

# Sorted longest-first so multi-word names match before their substrings
_DSCA_NAMES_SORTED = sorted(DSCA_COUNTRY_MAP.keys(), key=len, reverse=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

BACKTEST_START = "2021-02-24"
BACKTEST_END   = "2022-02-24"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def parse_date_from_url(url):
    """Extract date from media.defense.gov URL: /YYYY/Mon/DD/"""
    m = re.search(r"/(\d{4})/([A-Za-z]{3})/(\d{2})/", url)
    if m:
        try:
            return datetime.strptime(
                f"{m.group(1)}/{m.group(2)}/{m.group(3)}", "%Y/%b/%d"
            ).date().isoformat()
        except ValueError:
            pass
    return None


def parse_cn_from_text(text):
    """Extract CN number like '21-45' from a filename or title."""
    m = re.search(r"\b(\d{2,4}-\d{1,3})\b", text)
    return m.group(1) if m else None


def country_from_filename(filename):
    """
    Extract country name and ISO alpha-2 from a DSCA filename.

    Filename patterns observed:
      'PRESS RELEASE - UKRAINE 25-105 CN.PDF'
      'PRESS RELEASE - SAUDI ARABIA 25-103 CN.PDF'
      'GEORGIA_17-59.PDF'

    Strategy:
      1. Strip 'PRESS RELEASE - ' prefix and file extension.
      2. Extract the text before the first CN number (NN-NNN pattern).
      3. Look up the resulting name in DSCA_COUNTRY_MAP (longest match first).

    Returns (country_name, iso2) or (None, None).
    """
    text = filename.upper()
    text = re.sub(r"\.PDF$", "", text).strip()
    text = re.sub(r"^PRESS\s+RELEASE\s*[-–]\s*", "", text).strip()
    # Replace underscores (older format: GEORGIA_17-59)
    text = text.replace("_", " ").strip()
    # Extract text before the CN number
    m = re.match(r"^(.+?)\s+\d{2,4}-\d{1,3}", text)
    if not m:
        candidate = text
    else:
        candidate = m.group(1).strip()

    # Longest-match lookup
    for name in _DSCA_NAMES_SORTED:
        if candidate == name or candidate.startswith(name + " "):
            return name.title(), DSCA_COUNTRY_MAP[name]

    return None, None


def parse_page(html):
    """
    Parse one library page. Returns:
      records  — list of notification dicts for target countries
      last_page — int, highest page number found in pagination (or None)
    """
    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Find last page from pagination
    last_page = None
    for a in soup.find_all("a", href=True):
        if "igpage=" in a["href"]:
            try:
                n = int(re.search(r"igpage=(\d+)", a["href"]).group(1))
                if last_page is None or n > last_page:
                    last_page = n
            except (AttributeError, ValueError):
                pass

    # Collect all media.defense.gov PDF links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "media.defense.gov" not in href:
            continue
        if not href.lower().endswith(".pdf"):
            continue

        title = a.get_text(strip=True) or href.split("/")[-1]
        filename = href.split("/")[-1]

        country_name, iso2 = country_from_filename(filename)
        if not iso2:
            country_name, iso2 = country_from_filename(title)
        if not iso2:
            continue  # couldn't identify country — skip

        date_str = parse_date_from_url(href)
        cn_number = parse_cn_from_text(filename) or parse_cn_from_text(title)

        records.append({
            "cn_number": cn_number,
            "country": country_name,
            "country_iso2": iso2,
            "date": date_str,
            "pdf_url": href,
            "title": title,
        })

    return records, last_page


# ---------------------------------------------------------------------------
# Probe mode
# ---------------------------------------------------------------------------

def probe():
    """Fetch page 1, print pagination summary and first 10 target-country records."""
    session = get_session()
    print(f"[probe] GET {DSCA_LIBRARY_URL}", file=sys.stderr)
    resp = session.get(DSCA_LIBRARY_URL, timeout=30)
    print(f"[probe] HTTP {resp.status_code}", file=sys.stderr)

    records, last_page = parse_page(resp.text)
    print(f"[probe] Last page: {last_page}", file=sys.stderr)
    print(f"[probe] Target-country records on page 1: {len(records)}", file=sys.stderr)

    print(json.dumps({"last_page": last_page, "page_1_records": records}, indent=2))


# ---------------------------------------------------------------------------
# Full scrape
# ---------------------------------------------------------------------------

def scrape(output_path):
    """Paginate all pages, collect target-country records, write JSON."""
    session = get_session()
    all_records = []
    seen_urls = set()

    # Page 1 — also discover last_page
    print(f"[scrape] Fetching page 1...")
    resp = session.get(DSCA_LIBRARY_URL, timeout=30)
    if resp.status_code != 200:
        print(f"[ERROR] Page 1 returned HTTP {resp.status_code}", file=sys.stderr)
        sys.exit(1)

    records, last_page = parse_page(resp.text)
    if last_page is None:
        print(f"[ERROR] Could not determine last page from pagination", file=sys.stderr)
        sys.exit(1)

    for r in records:
        if r["pdf_url"] not in seen_urls:
            seen_urls.add(r["pdf_url"])
            all_records.append(r)

    print(f"[scrape] Page 1/{last_page} — {len(records)} target records, {last_page} pages total")

    # Pages 2..last_page
    for page_num in range(2, last_page + 1):
        time.sleep(REQUEST_DELAY)
        url = f"{DSCA_LIBRARY_URL}?igpage={page_num}"

        # Retry up to 2 times on timeout/error
        resp = None
        for attempt in range(3):
            try:
                resp = session.get(url, timeout=60)
                break
            except requests.exceptions.Timeout:
                wait = 5 * (attempt + 1)
                print(f"[scrape] Page {page_num} timeout (attempt {attempt+1}/3) — waiting {wait}s")
                time.sleep(wait)
            except requests.exceptions.RequestException as e:
                print(f"[scrape] Page {page_num} error: {e} — skipping")
                break

        if resp is None or resp.status_code != 200:
            status = resp.status_code if resp is not None else "timeout"
            print(f"[scrape] Page {page_num} HTTP {status} — skipping")
            continue

        records, _ = parse_page(resp.text)
        added = 0
        for r in records:
            if r["pdf_url"] not in seen_urls:
                seen_urls.add(r["pdf_url"])
                all_records.append(r)
                added += 1

        print(f"[scrape] Page {page_num}/{last_page} — {added} new target records ({len(all_records)} total)")

        # Save incrementally so a crash doesn't lose progress
        if added > 0:
            _write(all_records, output_path)

    _write(all_records, output_path)
    print(f"[done] {len(all_records)} notifications written to {output_path}")
    signals_path = output_path.parent / "dsca_signals.json"
    write_signals(output_path, signals_path)




def _write(records, output_path):
    sorted_records = sorted(records, key=lambda r: r.get("date") or "9999-99-99")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(sorted_records, indent=2))


def write_signals(notifications_path, signals_path):
    """
    Transform dsca_notifications.json → dsca_signals.json (and dsca_nato.json).

    NATO/NSPA collective notifications (iso2 == "XN") are written to
    dsca_nato.json using the same schema. All other records go to dsca_signals.json.
    """
    records = json.loads(notifications_path.read_text())
    generated_at = datetime.now(timezone.utc).isoformat()

    signals = []
    nato = []

    for r in records:
        iso2 = r.get("country_iso2")
        if not iso2:
            continue
        entry = {
            "iso":         iso2,
            "source":      "dsca",
            "signal_date": r.get("date"),
            "title":       r.get("title"),
            "value_usd":   None,
            "description": None,
            "raw_score":   None,
            "cn_number":   r.get("cn_number"),
            "pdf_url":     r.get("pdf_url"),
        }
        if iso2 == "XN":
            nato.append(entry)
        else:
            signals.append(entry)

    def _bundle(entries):
        return {
            "generated_at": generated_at,
            "sources":      ["dsca"],
            "signals":      entries,
        }

    signals_path.parent.mkdir(parents=True, exist_ok=True)
    signals_path.write_text(json.dumps(_bundle(signals), indent=2))
    print(f"[signals] {len(signals)} signals → {signals_path}")

    nato_path = signals_path.parent / "dsca_nato.json"
    nato_path.write_text(json.dumps(_bundle(nato), indent=2))
    print(f"[signals] {len(nato)} NATO/collective signals → {nato_path}")


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------

def backtest(data_path, start=BACKTEST_START, end=BACKTEST_END):
    """Filter dsca_notifications.json to a date window and print chronologically."""
    if not data_path.exists():
        print(f"[ERROR] {data_path} not found — run without flags first.", file=sys.stderr)
        sys.exit(1)

    records = json.loads(data_path.read_text())
    in_window = [r for r in records if r.get("date") and start <= r["date"] <= end]
    in_window.sort(key=lambda r: r["date"])

    print(f"[backtest] {start} → {end}  ({len(in_window)} of {len(records)} total)\n")
    for r in in_window:
        cn = r.get("cn_number") or "—"
        print(f"{r['date']}  {r['country']:<12}  CN {cn:<8}  {r['title']}")
        print(f"           {r['pdf_url']}")
        print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DSCA Major Arms Sales scraper")
    parser.add_argument("--probe", action="store_true",
                        help="Dump page 1 records and pagination info, exit")
    parser.add_argument("--backtest", action="store_true",
                        help=f"Print {BACKTEST_START}→{BACKTEST_END} records from saved data")
    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent
    output_path = repo_root / "data" / "dsca_notifications.json"

    if args.probe:
        probe()
        sys.exit(0)

    if args.backtest:
        backtest(output_path)
        sys.exit(0)

    signals_path = repo_root / "data" / "dsca_signals.json"
    scrape(output_path)
    # write_signals is called inside scrape, but also handle the case
    # where notifications already exist and signals need to be regenerated
    if not signals_path.exists():
        write_signals(output_path, signals_path)


if __name__ == "__main__":
    main()
