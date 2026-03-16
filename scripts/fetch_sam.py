#!/usr/bin/env python3
"""
fetch_sam.py — SAM.gov procurement signal pipeline for CREST

Usage:
  python fetch_sam.py --probe                            # dump raw API response, exit
  python fetch_sam.py                                    # full pipeline, write data/signals.json
  python fetch_sam.py --backtest AF 2021-08-15           # validate: 90 days before event date
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SAM_API_BASE = "https://api.sam.gov/opportunities/v2/search"
LOOKBACK_DAYS = 30
BACKTEST_DAYS = 365
PAGE_LIMIT_CANDIDATES = [10, 25, 50, 100]  # ascending — stop at first success

AGENCY_FRAGMENTS = [
    # Department of Defense — SAM.gov uses various abbreviations
    "department of defense",
    "dept of defense",
    "dept of the army",
    "dept of the navy",
    "dept of the air force",
    "defense logistics agency",
    "defense intelligence agency",
    "national geospatial-intelligence agency",
    "u.s. special operations command",
    "united states special operations command",
    "socom",
    "nga",
    "dia",
    # Department of State
    "department of state",
    "dept of state",
    "state, department of",
    # USAID
    "agency for international development",
    "usaid",
]

# Weights: language pairs = 3, logistics/construction = 2, intel support = 3
# Keyword matching is on title only — description is a URL in SAM.gov v2
KEYWORDS = {
    # Language pairs
    "Pashto":             3,
    "Farsi":              3,
    "Dari":               3,
    "Arabic":             3,
    "Amharic":            3,
    "Somali":             3,
    "Hausa":              3,
    "Burmese":            3,
    "Tigrinya":           3,
    "Uzbek":              3,
    "Tajik":              3,
    # Logistics
    "forward operating":  2,
    "pre-positioned":     2,
    "airlift":            2,
    "sealift":            2,
    "base operations":    2,
    "force protection":   2,
    # Construction
    "expeditionary":      2,
    "hardened facility":  2,
    "perimeter security": 2,
    # Intelligence support
    "geospatial":         3,
    "HUMINT":             3,
    "ISR":                3,
}

# Geographic title keywords for backtest country matching.
# placeOfPerformance.country is null on ~93% of SAM.gov records,
# so we match geography from the solicitation title instead.
GEO_KEYWORDS = {
    "UA": [
        "ukraine", "ukrainian", "kyiv", "kiev", "lviv",
        "odessa", "kharkiv", "eastern europe", "black sea",
        "ukrainian language", "russian language",
        "poland", "polish", "romania", "romanian",
        "estonia", "latvia", "lithuania", "baltic",
        "finland", "finnish", "georgia",
    ],
    "AF": [
        "afghanistan", "afghan", "kabul", "kandahar", "helmand",
        "kunduz", "mazar-i-sharif", "herat", "jalalabad",
    ],
    "IQ": [
        "iraq", "iraqi", "baghdad", "mosul", "basra", "erbil", "kirkuk",
    ],
    "SY": [
        "syria", "syrian", "damascus", "aleppo", "deir ez-zor",
    ],
    "YE": [
        "yemen", "yemeni", "sanaa", "aden", "hodeidah",
    ],
    "SO": [
        "somalia", "somali", "mogadishu",
    ],
    "LY": [
        "libya", "libyan", "tripoli", "benghazi",
    ],
    "ML": [
        "mali", "malian", "bamako", "sahel",
    ],
    "SS": [
        "south sudan", "juba",
    ],
    "ET": [
        "ethiopia", "ethiopian", "addis ababa", "tigray",
    ],
}

# Recency decay: (max_age_days, multiplier)
DECAY_THRESHOLDS = [
    (7,  1.0),
    (14, 0.7),
    (21, 0.5),
    (30, 0.3),
]

# ---------------------------------------------------------------------------
# ISO alpha-3 → alpha-2 conversion
# Confirmed: SAM.gov placeOfPerformance.country.code returns alpha-3 (e.g. "USA")
# ---------------------------------------------------------------------------

ALPHA3_TO_ALPHA2 = {
    "AFG": "AF", "ALB": "AL", "DZA": "DZ", "AGO": "AO", "ARM": "AM",
    "AUS": "AU", "AUT": "AT", "AZE": "AZ", "BHR": "BH", "BGD": "BD",
    "BLR": "BY", "BEL": "BE", "BLZ": "BZ", "BEN": "BJ", "BIH": "BA",
    "BWA": "BW", "BRA": "BR", "BGR": "BG", "BFA": "BF", "BDI": "BI",
    "KHM": "KH", "CMR": "CM", "CAN": "CA", "CAF": "CF", "TCD": "TD",
    "CHL": "CL", "CHN": "CN", "COL": "CO", "COM": "KM", "COG": "CG",
    "COD": "CD", "CRI": "CR", "HRV": "HR", "CUB": "CU", "CYP": "CY",
    "CZE": "CZ", "DNK": "DK", "DJI": "DJ", "DOM": "DO", "ECU": "EC",
    "EGY": "EG", "SLV": "SV", "ERI": "ER", "EST": "EE", "ETH": "ET",
    "FIN": "FI", "FRA": "FR", "GAB": "GA", "GMB": "GM", "GEO": "GE",
    "DEU": "DE", "GHA": "GH", "GRC": "GR", "GTM": "GT", "GIN": "GN",
    "GNB": "GW", "HTI": "HT", "HND": "HN", "HUN": "HU", "IND": "IN",
    "IDN": "ID", "IRN": "IR", "IRQ": "IQ", "IRL": "IE", "ISR": "IL",
    "ITA": "IT", "CIV": "CI", "JAM": "JM", "JPN": "JP", "JOR": "JO",
    "KAZ": "KZ", "KEN": "KE", "XKX": "XK", "KWT": "KW", "KGZ": "KG",
    "LAO": "LA", "LVA": "LV", "LBN": "LB", "LBR": "LR", "LBY": "LY",
    "LTU": "LT", "MDG": "MG", "MWI": "MW", "MYS": "MY", "MDV": "MV",
    "MLI": "ML", "MRT": "MR", "MEX": "MX", "MDA": "MD", "MNG": "MN",
    "MNE": "ME", "MAR": "MA", "MOZ": "MZ", "MMR": "MM", "NAM": "NA",
    "NPL": "NP", "NLD": "NL", "NIC": "NI", "NER": "NE", "NGA": "NG",
    "PRK": "KP", "MKD": "MK", "NOR": "NO", "OMN": "OM", "PAK": "PK",
    "PAN": "PA", "PNG": "PG", "PRY": "PY", "PER": "PE", "PHL": "PH",
    "POL": "PL", "PRT": "PT", "QAT": "QA", "ROU": "RO", "RUS": "RU",
    "RWA": "RW", "SAU": "SA", "SEN": "SN", "SRB": "RS", "SLE": "SL",
    "SVK": "SK", "SVN": "SI", "SOM": "SO", "ZAF": "ZA", "KOR": "KR",
    "SSD": "SS", "ESP": "ES", "LKA": "LK", "SDN": "SD", "SWE": "SE",
    "CHE": "CH", "SYR": "SY", "TWN": "TW", "TJK": "TJ", "TZA": "TZ",
    "THA": "TH", "TLS": "TL", "TGO": "TG", "TUN": "TN", "TUR": "TR",
    "TKM": "TM", "UGA": "UG", "UKR": "UA", "ARE": "AE", "GBR": "GB",
    "USA": "US", "URY": "UY", "UZB": "UZ", "VEN": "VE", "VNM": "VN",
    "PSE": "PS", "YEM": "YE", "ZMB": "ZM", "ZWE": "ZW", "SWZ": "SZ",
    "MUS": "MU", "CPV": "CV", "GNQ": "GQ", "STP": "ST", "SYC": "SC",
    "DJI": "DJ", "LSO": "LS", "MKD": "MK", "ALG": "DZ", "KOS": "XK",
}

# Country name → ISO alpha-2 (fallback: parse from title text only)
COUNTRY_NAME_TO_ISO2 = {
    "afghanistan": "AF", "albania": "AL", "algeria": "DZ", "angola": "AO",
    "armenia": "AM", "australia": "AU", "austria": "AT", "azerbaijan": "AZ",
    "bahrain": "BH", "bangladesh": "BD", "belgium": "BE", "belize": "BZ",
    "benin": "BJ", "bosnia": "BA", "bosnia and herzegovina": "BA",
    "botswana": "BW", "brazil": "BR", "bulgaria": "BG", "burkina faso": "BF",
    "burma": "MM", "burundi": "BI", "cambodia": "KH", "cameroon": "CM",
    "central african republic": "CF", "chad": "TD", "colombia": "CO",
    "comoros": "KM", "congo": "CG", "democratic republic of congo": "CD",
    "democratic republic of the congo": "CD", "drc": "CD", "costa rica": "CR",
    "croatia": "HR", "cuba": "CU", "cyprus": "CY", "czech republic": "CZ",
    "denmark": "DK", "djibouti": "DJ", "dominican republic": "DO",
    "ecuador": "EC", "egypt": "EG", "el salvador": "SV", "eritrea": "ER",
    "estonia": "EE", "ethiopia": "ET", "finland": "FI", "france": "FR",
    "gabon": "GA", "gambia": "GM", "georgia": "GE", "germany": "DE",
    "ghana": "GH", "greece": "GR", "guatemala": "GT", "guinea": "GN",
    "guinea-bissau": "GW", "haiti": "HT", "honduras": "HN", "hungary": "HU",
    "india": "IN", "indonesia": "ID", "iran": "IR", "iraq": "IQ",
    "israel": "IL", "italy": "IT", "ivory coast": "CI", "cote d'ivoire": "CI",
    "jamaica": "JM", "japan": "JP", "jordan": "JO", "kazakhstan": "KZ",
    "kenya": "KE", "kosovo": "XK", "kuwait": "KW", "kyrgyzstan": "KG",
    "laos": "LA", "latvia": "LV", "lebanon": "LB", "liberia": "LR",
    "libya": "LY", "lithuania": "LT", "madagascar": "MG", "malawi": "MW",
    "malaysia": "MY", "mali": "ML", "mauritania": "MR", "mexico": "MX",
    "moldova": "MD", "mongolia": "MN", "montenegro": "ME", "morocco": "MA",
    "mozambique": "MZ", "myanmar": "MM", "namibia": "NA", "nepal": "NP",
    "netherlands": "NL", "nicaragua": "NI", "niger": "NE", "nigeria": "NG",
    "north korea": "KP", "north macedonia": "MK", "norway": "NO",
    "oman": "OM", "pakistan": "PK", "panama": "PA", "papua new guinea": "PG",
    "paraguay": "PY", "peru": "PE", "philippines": "PH", "poland": "PL",
    "portugal": "PT", "qatar": "QA", "romania": "RO", "russia": "RU",
    "russian federation": "RU", "rwanda": "RW", "saudi arabia": "SA",
    "senegal": "SN", "serbia": "RS", "sierra leone": "SL", "slovakia": "SK",
    "slovenia": "SI", "somalia": "SO", "south africa": "ZA",
    "south korea": "KR", "south sudan": "SS", "spain": "ES",
    "sri lanka": "LK", "sudan": "SD", "sweden": "SE", "switzerland": "CH",
    "syria": "SY", "taiwan": "TW", "tajikistan": "TJ", "tanzania": "TZ",
    "thailand": "TH", "timor-leste": "TL", "east timor": "TL", "togo": "TG",
    "tunisia": "TN", "turkey": "TR", "turkmenistan": "TM", "uganda": "UG",
    "ukraine": "UA", "united arab emirates": "AE", "uae": "AE",
    "united kingdom": "GB", "united states": "US", "usa": "US",
    "uruguay": "UY", "uzbekistan": "UZ", "venezuela": "VE", "vietnam": "VN",
    "viet nam": "VN", "west bank": "PS", "gaza": "PS", "palestine": "PS",
    "yemen": "YE", "zambia": "ZM", "zimbabwe": "ZW",
}

# Longest-match-first for title text scanning
_COUNTRY_NAMES_SORTED = sorted(COUNTRY_NAME_TO_ISO2.keys(), key=len, reverse=True)
_COUNTRY_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(n) for n in _COUNTRY_NAMES_SORTED) + r")\b",
    re.IGNORECASE,
)

# Word-boundary keyword patterns
_KEYWORD_PATTERNS = {
    kw: re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
    for kw in KEYWORDS
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def date_str(dt):
    return dt.strftime("%m/%d/%Y")


def recency_decay(posted_date_str, reference_dt=None):
    """Decay multiplier based on age of solicitation relative to reference_dt."""
    if reference_dt is None:
        reference_dt = datetime.now(timezone.utc)
    try:
        posted = datetime.fromisoformat(posted_date_str.rstrip("Z"))
        if posted.tzinfo is None:
            posted = posted.replace(tzinfo=timezone.utc)
        age_days = (reference_dt - posted).days
    except (ValueError, AttributeError):
        return 0.3

    for threshold, multiplier in DECAY_THRESHOLDS:
        if age_days <= threshold:
            return multiplier
    return 0.3


def match_keywords(title):
    """Return {keyword: weight} for all keywords found in title."""
    matched = {}
    for kw, weight in KEYWORDS.items():
        if _KEYWORD_PATTERNS[kw].search(title):
            matched[kw] = weight
    return matched


def geo_matches(title, country_code):
    """
    Return list of matched geographic keywords for country_code found in title.
    Case-insensitive. Any match = True. Returns [] if no match or unknown code.
    """
    terms = GEO_KEYWORDS.get(country_code.upper(), [])
    title_lower = title.lower()
    return [t for t in terms if t in title_lower]


def extract_country(opportunity):
    """
    Extract ISO alpha-2 country code.
    Primary:  placeOfPerformance.country.code (alpha-3) → convert to alpha-2
    Fallback: scan title for country names
    """
    pop = opportunity.get("placeOfPerformance") or {}
    country_obj = pop.get("country") or {}
    code = (country_obj.get("code") or "").strip().upper()

    if code:
        # Alpha-3 → alpha-2
        if len(code) == 3:
            return ALPHA3_TO_ALPHA2.get(code)
        # Already alpha-2 (unlikely but handle it)
        if len(code) == 2:
            return code

    # Fallback: title text only (description is a URL in SAM.gov v2)
    title = opportunity.get("title") or ""
    m = _COUNTRY_PATTERN.search(title)
    if m:
        return COUNTRY_NAME_TO_ISO2.get(m.group(1).lower())

    return None


def agency_matches(opportunity):
    """True if the opportunity's agency path contains a target fragment."""
    path = (opportunity.get("fullParentPathName") or "").lower()
    for fragment in AGENCY_FRAGMENTS:
        if fragment in path:
            return True
    return False


def contract_value(opportunity):
    """Best-effort dollar value extraction."""
    award = opportunity.get("award") or {}
    try:
        return float(award.get("amount") or 0)
    except (ValueError, TypeError):
        return 0.0


def write_error_state(reason, output_path):
    """Write error state to sam_signals.json. Never fake data."""
    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": ["sam_gov"],
        "error": reason,
        "signals": [],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(doc, indent=2))
    print(f"[ERROR] {reason}", file=sys.stderr)


# ---------------------------------------------------------------------------
# API fetch (paginated)
# ---------------------------------------------------------------------------

def find_working_limit(api_key):
    """
    Test PAGE_LIMIT_CANDIDATES in ascending order against the current 30-day
    window. Stop at first success. Each call counts against the daily budget.
    Returns the working limit value only — does not reuse the response.
    """
    today = datetime.now(timezone.utc)
    from_str = date_str(today - timedelta(days=30))
    to_str = date_str(today)

    for limit in PAGE_LIMIT_CANDIDATES:
        params = {
            "api_key": api_key,
            "postedFrom": from_str,
            "postedTo": to_str,
            "limit": limit,
            "offset": 0,
        }
        req = requests.Request("GET", SAM_API_BASE, params=params).prepare()
        safe_url = req.url.replace(api_key, "***")
        print(f"[limit-test] limit={limit} — {safe_url}")
        resp = requests.get(SAM_API_BASE, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            total = data.get("totalRecords", 0)
            print(f"[limit-test] limit={limit} OK — totalRecords={total} in current 30-day window")
            return limit
        print(f"[limit-test] limit={limit} → HTTP {resp.status_code}")

    raise RuntimeError("No working limit found — all candidates failed")


def fetch_opportunities(api_key, posted_from_dt, posted_to_dt, limit):
    """
    Paginate through all records in the date window using the given limit.
    Prints a budget estimate (pages needed) after the first call so the caller
    can abort if cost is too high. Raises on HTTP/network errors.
    """
    from_str = date_str(posted_from_dt)
    to_str = date_str(posted_to_dt)
    all_opps = []
    offset = 0

    print(f"[fetch] {from_str} → {to_str}  limit={limit}")

    while True:
        params = {
            "api_key": api_key,
            "postedFrom": from_str,
            "postedTo": to_str,
            "limit": limit,
            "offset": offset,
        }
        if offset == 0:
            req = requests.Request("GET", SAM_API_BASE, params=params).prepare()
            print(f"[fetch] URL: {req.url.replace(api_key, '***')}")

        resp = requests.get(SAM_API_BASE, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        page_opps = data.get("opportunitiesData") or []
        total = data.get("totalRecords", 0)

        if offset == 0:
            pages_needed = -(-total // limit)  # ceiling division
            print(f"[fetch] totalRecords={total} — {pages_needed} pages at limit={limit}")

        print(f"[fetch] offset={offset} got={len(page_opps)}")

        if not page_opps:
            break

        all_opps.extend(page_opps)
        offset += len(page_opps)

        if offset >= total:
            break

    print(f"[fetch] Done — retrieved {len(all_opps)} of {total}")
    return all_opps


# ---------------------------------------------------------------------------
# Probe mode
# ---------------------------------------------------------------------------

def probe(api_key):
    """Hit SAM.gov with limit=5, dump raw response to stdout, exit."""
    today = datetime.now(timezone.utc)
    params = {
        "api_key": api_key,
        "postedFrom": date_str(today - timedelta(days=LOOKBACK_DAYS)),
        "postedTo": date_str(today),
        "limit": 5,
        "offset": 0,
    }

    print(f"[probe] GET {SAM_API_BASE}", file=sys.stderr)
    print(f"[probe] params (redacted key): {dict(params, api_key='***')}", file=sys.stderr)

    resp = requests.get(SAM_API_BASE, params=params, timeout=30)
    print(f"[probe] HTTP {resp.status_code}", file=sys.stderr)
    print(f"[probe] Content-Type: {resp.headers.get('Content-Type')}", file=sys.stderr)

    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print("[probe] Response is not JSON:", file=sys.stderr)
        print(resp.text)


# ---------------------------------------------------------------------------
# Debug-filter mode
# ---------------------------------------------------------------------------

def _raw_count(api_key, from_str, to_str, extra_params=None):
    """
    Fire a single limit=1 request and return (totalRecords, full_url, raw_data).
    Used only for diagnostic probing — not for real fetches.
    """
    params = {
        "api_key": api_key,
        "postedFrom": from_str,
        "postedTo": to_str,
        "limit": 10,  # safe floor; limit=1 may also 404
        "offset": 0,
    }
    if extra_params:
        params.update(extra_params)
    req = requests.Request("GET", SAM_API_BASE, params=params).prepare()
    safe_url = req.url.replace(api_key, "***")
    resp = requests.get(SAM_API_BASE, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("totalRecords", 0), safe_url, data


def debug_filter(api_key, country_code, event_date_str):
    """
    Diagnostic mode: fires three progressively-minimal API calls against the
    90-day window and reports totalRecords for each. Identifies whether the
    constraint is ptype, the date range, or something else upstream of our
    filters. No file writes.
    """
    try:
        event_dt = datetime.strptime(event_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"[ERROR] EVENT_DATE must be YYYY-MM-DD, got: {event_date_str}", file=sys.stderr)
        sys.exit(1)

    target = country_code.upper()
    window_end = event_dt
    window_start = event_dt - timedelta(days=BACKTEST_DAYS)
    from_str = date_str(window_start)
    to_str = date_str(window_end)

    print(f"[debug-filter] Window: {from_str} → {to_str}  Target country: {target}")
    print(f"[debug-filter] Firing 3 minimal calls to isolate the constraint...\n")

    # Call 1: bare minimum — date range only, no ptype
    try:
        total_bare, url_bare, _ = _raw_count(api_key, from_str, to_str)
        print(f"Call 1 — date range only (no ptype):")
        print(f"  URL          : {url_bare}")
        print(f"  totalRecords : {total_bare}\n")
    except Exception as e:
        print(f"Call 1 failed: {e}\n")
        total_bare = 0

    # Call 2: date range + ptype=o,p (what the pipeline currently sends)
    try:
        total_ptype, url_ptype, _ = _raw_count(api_key, from_str, to_str, {"ptype": "o,p"})
        print(f"Call 2 — date range + ptype=o,p (current pipeline):")
        print(f"  URL          : {url_ptype}")
        print(f"  totalRecords : {total_ptype}\n")
    except Exception as e:
        print(f"Call 2 failed: {e}\n")
        total_ptype = 0

    # Call 3: date range + ptype=o (solicitations only, no comma)
    try:
        total_o, url_o, _ = _raw_count(api_key, from_str, to_str, {"ptype": "o"})
        print(f"Call 3 — date range + ptype=o (solicitations only):")
        print(f"  URL          : {url_o}")
        print(f"  totalRecords : {total_o}\n")
    except Exception as e:
        print(f"Call 3 failed: {e}\n")
        total_o = 0

    # Diagnosis
    print("--- Diagnosis ---")
    if total_bare < 100:
        print("  DATE RANGE is the constraint — bare call returns < 100 records.")
        print("  Possible causes: SAM.gov caps historical queries, key has limited access,")
        print("  or postedFrom/postedTo format is being rejected silently.")
    elif total_ptype < total_bare * 0.5:
        print(f"  PTYPE is the constraint — bare={total_bare} vs ptype=o,p={total_ptype}.")
        print("  ptype=o,p may not be valid as a comma-separated value.")
        print("  Fix: remove ptype from the query or use a single value.")
    else:
        print(f"  Query returns {total_bare} records without filters.")
        print("  Date range and ptype are not the issue — problem is downstream (filters).")

    # If we have a usable dataset, fetch it and run the filter stages
    best_total = max(total_bare, total_ptype, total_o)
    if best_total < 100:
        print(f"\n  Cannot run filter-stage analysis — API only returned {best_total} total records.")
        print("  Fix the query first, then rerun --debug-filter.")
        return

    # Fetch using bare params (no ptype) to get the full dataset
    print(f"\n[debug-filter] Fetching full window with no ptype (bare call, expected ~{total_bare} records)...")
    try:
        all_opps = []
        offset = 0
        while True:
            params = {
                "api_key": api_key,
                "postedFrom": from_str,
                "postedTo": to_str,
                "limit": PAGE_LIMIT_CANDIDATES[-1],  # highest candidate
                "offset": offset,
            }
            resp = requests.get(SAM_API_BASE, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            page = data.get("opportunitiesData") or []
            total_r = data.get("totalRecords", 0)
            print(f"  offset={offset} got={len(page)} total={total_r}")
            if not page:
                break
            all_opps.extend(page)
            offset += len(page)
            if offset >= total_r:
                break
    except Exception as e:
        print(f"  Fetch failed: {e}")
        return

    from collections import Counter

    after_agency = [o for o in all_opps if agency_matches(o)]
    after_country = [o for o in all_opps if extract_country(o) == target]
    after_both = [o for o in after_agency if extract_country(o) == target]

    print(f"\n--- Filter stages (bare fetch, no ptype) ---")
    print(f"  Total fetched          : {len(all_opps)}")
    print(f"  After agency filter    : {len(after_agency)}")
    print(f"  After country ({target})   : {len(after_country)}  (no agency prereq)")
    print(f"  After agency + country : {len(after_both)}")

    agency_counts = Counter(
        (o.get("fullParentPathName") or "").split(".")[0].strip()
        for o in all_opps
    )
    print(f"\n--- Top 30 agency roots in raw data ---")
    for name, count in agency_counts.most_common(30):
        marker = "  [MATCH]" if agency_matches({"fullParentPathName": name}) else ""
        print(f"  {count:5d}  {name}{marker}")

    country_counts = Counter(
        extract_country(o) for o in all_opps if extract_country(o)
    )
    print(f"\n--- Top 20 countries in raw data ---")
    for code, count in country_counts.most_common(20):
        marker = "  <-- TARGET" if code == target else ""
        print(f"  {count:5d}  {code}{marker}")


# ---------------------------------------------------------------------------
# Backtest mode
# ---------------------------------------------------------------------------

def backtest(api_key, country_code, event_date_str):
    """
    Query 90 days before event_date for a specific country.
    Print matching solicitations chronologically. No file writes.
    """
    try:
        event_dt = datetime.strptime(event_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"[ERROR] EVENT_DATE must be YYYY-MM-DD, got: {event_date_str}", file=sys.stderr)
        sys.exit(1)

    target = country_code.upper()
    window_end = event_dt
    window_start = event_dt - timedelta(days=BACKTEST_DAYS)

    print(f"[backtest] Country: {target}  Window: {date_str(window_start)} → {date_str(window_end)}")

    limit = find_working_limit(api_key)

    try:
        opportunities = fetch_opportunities(api_key, window_start, window_end, limit)
    except requests.HTTPError as e:
        print(f"[ERROR] SAM.gov HTTP error: {e}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"[ERROR] SAM.gov request failed: {e}", file=sys.stderr)
        sys.exit(1)

    total = len(opportunities)
    n_agency = 0
    n_geo = 0
    matches = []

    if target not in GEO_KEYWORDS:
        print(f"[WARN] No GEO_KEYWORDS defined for {target} — title-based geo matching will return nothing.", file=sys.stderr)

    for opp in opportunities:
        if not agency_matches(opp):
            continue
        n_agency += 1

        title = opp.get("title") or ""
        matched_geo = geo_matches(title, target)
        if not matched_geo:
            continue
        n_geo += 1

        matched_signal = match_keywords(title)

        # Place of performance — print whatever fields are populated
        pop = opp.get("placeOfPerformance") or {}
        pop_parts = []
        for field in ("streetAddress", "city", "state", "country"):
            val = pop.get(field)
            if isinstance(val, dict):
                val = val.get("name") or val.get("code")
            if val:
                pop_parts.append(str(val))
        pop_str = ", ".join(pop_parts) if pop_parts else "not specified"

        matches.append({
            "posted": opp.get("postedDate") or "",
            "title": title,
            "agency": opp.get("fullParentPathName") or "",
            "geo_matched": matched_geo,
            "signal_matched": list(matched_signal.keys()),
            "pop": pop_str,
            "value": contract_value(opp),
        })

    print(
        f"[filter] {total} fetched"
        f" → {n_agency} after agency"
        f" → {n_geo} after geo title match ({target})"
        f" → {len(matches)} total (all geo matches printed)",
        file=sys.stderr,
    )

    # Sort chronologically
    matches.sort(key=lambda x: x["posted"])

    if not matches:
        print(f"[backtest] No matching solicitations found for {target} in window.")
        return

    print(f"\n[backtest] {len(matches)} solicitations matching {target} geo keywords:\n")
    for m in matches:
        geo_str = ", ".join(m["geo_matched"])
        sig_str = (", ".join(m["signal_matched"])) if m["signal_matched"] else "—"
        val_str = f"${m['value']:,.0f}" if m["value"] else "undisclosed"
        title = m["title"][:80] + ("…" if len(m["title"]) > 80 else "")
        agency = m["agency"].split(".")[-1].strip() if m["agency"] else ""
        print(f"{m['posted']} | {title}")
        print(f"           agency: {agency}")
        print(f"           place:  {m['pop']}")
        print(f"           geo:    {geo_str}")
        print(f"           signal: {sig_str}  |  {val_str}")
        print()


# ---------------------------------------------------------------------------
# Scoring and normalization
# ---------------------------------------------------------------------------

def score_opportunities(opportunities):
    """
    Filter by agency + keywords, score by recency, group by country.
    Returns raw country data dict.
    """
    total = len(opportunities)
    n_agency = 0
    n_keyword = 0
    n_country = 0
    countries = {}

    for opp in opportunities:
        if not agency_matches(opp):
            continue
        n_agency += 1

        title = opp.get("title") or ""
        matched = match_keywords(title)
        if not matched:
            continue
        n_keyword += 1

        country = extract_country(opp)
        if not country:
            continue
        n_country += 1

        posted = opp.get("postedDate") or ""
        decay = recency_decay(posted)
        score = (1 + sum(matched.values())) * decay

        entry = {
            "title": title,
            "agency": opp.get("fullParentPathName") or "",
            "posted": posted,
            "value": contract_value(opp),
            "keywords_matched": list(matched.keys()),
            "raw_score": score,
        }

        if country not in countries:
            countries[country] = {"raw_score": 0.0, "raw_count": 0, "contracts": []}

        countries[country]["raw_score"] += score
        countries[country]["raw_count"] += 1
        countries[country]["contracts"].append(entry)

    print(
        f"[filter] {total} fetched"
        f" → {n_agency} after agency"
        f" → {n_keyword} after keyword"
        f" → {n_country} after country"
    )
    return countries


def normalize(countries):
    """Normalize raw_score to 0–100 across all countries."""
    if not countries:
        return {}
    max_score = max(v["raw_score"] for v in countries.values()) or 1
    return {
        iso2: {
            "score": round((data["raw_score"] / max_score) * 100, 1),
            "raw_count": data["raw_count"],
            "contracts": data["contracts"],
        }
        for iso2, data in countries.items()
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="CREST SAM.gov signal pipeline")
    parser.add_argument("--probe", action="store_true",
                        help="Dump raw SAM.gov response (limit=5) and exit")
    parser.add_argument("--backtest", nargs=2, metavar=("COUNTRY_CODE", "EVENT_DATE"),
                        help="Print solicitations for COUNTRY_CODE in 90 days before EVENT_DATE")
    parser.add_argument("--debug-filter", nargs=2, metavar=("COUNTRY_CODE", "EVENT_DATE"),
                        help="Show record counts at each filter stage for a backtest window")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print a sample of the output schema without calling the API")
    args = parser.parse_args()

    if args.dry_run:
        sample = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": ["sam_gov"],
            "signals": [
                {
                    "iso": "UA",
                    "source": "sam_gov",
                    "signal_date": "2026-03-01",
                    "title": "Interpreter Services — Ukrainian Language Support",
                    "value_usd": 250000.0,
                    "description": None,
                    "raw_score": 2.1,
                },
                {
                    "iso": "IQ",
                    "source": "sam_gov",
                    "signal_date": "2026-03-10",
                    "title": "Forward Operating Base Logistics Support",
                    "value_usd": None,
                    "description": None,
                    "raw_score": 1.4,
                },
            ],
        }
        print(json.dumps(sample, indent=2))
        sys.exit(0)

    api_key = os.environ.get("SAM_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] SAM_API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)

    if args.probe:
        probe(api_key)
        sys.exit(0)

    if args.debug_filter:
        debug_filter(api_key, args.debug_filter[0], args.debug_filter[1])
        sys.exit(0)

    if args.backtest:
        backtest(api_key, args.backtest[0], args.backtest[1])
        sys.exit(0)

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------
    repo_root = Path(__file__).parent.parent
    output_path = repo_root / "data" / "sam_signals.json"

    today = datetime.now(timezone.utc)
    window_start = today - timedelta(days=LOOKBACK_DAYS)

    try:
        limit = find_working_limit(api_key)
        opportunities = fetch_opportunities(api_key, window_start, today, limit)
    except requests.HTTPError as e:
        write_error_state(f"SAM.gov HTTP error: {e}", output_path)
        sys.exit(1)
    except requests.RequestException as e:
        write_error_state(f"SAM.gov request failed: {e}", output_path)
        sys.exit(1)

    if not opportunities:
        write_error_state("SAM.gov returned 0 opportunities — unexpected", output_path)
        sys.exit(1)

    raw_countries = score_opportunities(opportunities)
    print(f"[score] Countries with signal: {len(raw_countries)}")
    for iso2, data in sorted(raw_countries.items(), key=lambda x: -x[1]["raw_score"]):
        print(f"  {iso2}: count={data['raw_count']} raw_score={data['raw_score']:.2f}")

    signals = []
    for iso2, data in raw_countries.items():
        for contract in data["contracts"]:
            signals.append({
                "iso": iso2,
                "source": "sam_gov",
                "signal_date": contract["posted"][:10] if contract["posted"] else None,
                "title": contract["title"],
                "value_usd": contract["value"] if contract["value"] else None,
                "description": None,
                "raw_score": contract["raw_score"],
            })

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": ["sam_gov"],
        "signals": signals,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2))
    print(f"[done] Wrote {output_path} ({len(signals)} signals across {len(raw_countries)} countries)")


if __name__ == "__main__":
    main()
