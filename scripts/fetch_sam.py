#!/usr/bin/env python3
"""
fetch_sam.py — SAM.gov procurement signal pipeline for HARPY

Single API call, last 45 days, limit=10.
Filters to defense/state/USAID agencies.
Writes data/sam_signals.json.
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import urllib.request
import urllib.parse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SAM_API_BASE = "https://api.sam.gov/opportunities/v2/search"
LOOKBACK_DAYS = 45
BATCH_SIZE = 10

AGENCY_KEYWORDS = [
    "DEFENSE, DEPARTMENT OF",
    "ARMY, DEPARTMENT OF",
    "NAVY, DEPARTMENT OF",
    "AIR FORCE, DEPARTMENT OF",
    "STATE, DEPARTMENT OF",
    "AGENCY FOR INTERNATIONAL DEVELOPMENT",
]

# ---------------------------------------------------------------------------
# ISO alpha-3 → alpha-2
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
    "LSO": "LS", "KOS": "XK",
}

# Country name → ISO alpha-2 (fallback: title scan)
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

_COUNTRY_NAMES_SORTED = sorted(COUNTRY_NAME_TO_ISO2.keys(), key=len, reverse=True)
_COUNTRY_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(n) for n in _COUNTRY_NAMES_SORTED) + r")\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_api_key():
    key = os.environ.get("SAM_API_KEY", "")
    if not key:
        env_path = Path(__file__).parent.parent / ".env"
        try:
            with open(env_path) as f:
                for line in f:
                    if line.startswith("SAM_API_KEY="):
                        key = line.strip().split("=", 1)[1]
                        break
        except FileNotFoundError:
            pass
    return key


def is_agency_match(record):
    path = (record.get("fullParentPathName") or "").upper()
    return any(kw in path for kw in AGENCY_KEYWORDS)


def extract_country(record):
    pop = record.get("placeOfPerformance") or {}
    country_obj = pop.get("country") or {}
    code = (country_obj.get("code") or "").strip().upper()

    if code:
        if len(code) == 3:
            iso2 = ALPHA3_TO_ALPHA2.get(code)
            if iso2:
                return iso2
        elif len(code) == 2:
            return code

    # Fallback: scan title for country name
    title = record.get("title") or ""
    m = _COUNTRY_PATTERN.search(title)
    if m:
        return COUNTRY_NAME_TO_ISO2.get(m.group(1).lower())

    return None


def to_signal(record):
    award = record.get("award") or {}
    value = award.get("amount")
    try:
        value = float(value) if value is not None else None
    except (TypeError, ValueError):
        value = None

    return {
        "iso": extract_country(record),
        "source": "sam",
        "signal_date": record.get("postedDate"),
        "title": record.get("title"),
        "value_usd": value,
        "description": None,
        "raw_score": 1.0,
        "page_url": record.get("uiLink"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    out_path = Path(__file__).parent.parent / "data" / "sam_signals.json"

    key = load_api_key()
    if not key:
        print("ERROR: SAM_API_KEY not set", file=sys.stderr)
        out_path.write_text(json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": ["sam"],
            "error": "missing SAM_API_KEY",
            "signals": [],
        }, indent=2))
        sys.exit(0)

    today = datetime.now(timezone.utc)
    from_date = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    to_date = today.strftime("%m/%d/%Y")

    params = {
        "api_key": key,
        "limit": BATCH_SIZE,
        "offset": 0,
        "postedFrom": from_date,
        "postedTo": to_date,
    }
    url = SAM_API_BASE + "?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"ERROR: API call failed: {e}", file=sys.stderr)
        out_path.write_text(json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": ["sam"],
            "error": str(e),
            "signals": [],
        }, indent=2))
        sys.exit(0)

    batch = data.get("opportunitiesData") or []
    signals = [to_signal(r) for r in batch if is_agency_match(r)]

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": ["sam"],
        "signals": signals,
    }
    out_path.write_text(json.dumps(output, indent=2))
    print(f"Wrote {len(signals)} signals to {out_path}")


if __name__ == "__main__":
    main()
