#!/usr/bin/env python3
"""
fetch_fara.py — FARA new registrant pipeline for HARPY

Source: https://efile.fara.gov/api/v1/Registrants/New.json
        date range: yesterday → today

Emits one signal per new registration where the foreign principal country
resolves to a known ISO code. Unknown countries are kept (iso = "XX").
Appends to data/fara_signals.json; deduplicates by registration_number.
"""

import json
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FARA_API_BASE = "https://efile.fara.gov/api/v1/Registrants/New.json"
LOOKBACK_DAYS = 1

# ---------------------------------------------------------------------------
# Country name → ISO alpha-2
# ---------------------------------------------------------------------------

COUNTRY_NAME_TO_ISO2 = {
    "afghanistan": "AF", "albania": "AL", "algeria": "DZ", "angola": "AO",
    "armenia": "AM", "australia": "AU", "austria": "AT", "azerbaijan": "AZ",
    "bahrain": "BH", "bangladesh": "BD", "belarus": "BY", "belgium": "BE",
    "belize": "BZ", "benin": "BJ", "bolivia": "BO",
    "bosnia and herzegovina": "BA", "bosnia": "BA",
    "botswana": "BW", "brazil": "BR", "bulgaria": "BG", "burkina faso": "BF",
    "burma": "MM", "burundi": "BI", "cambodia": "KH", "cameroon": "CM",
    "canada": "CA", "central african republic": "CF", "chad": "TD",
    "chile": "CL", "china": "CN", "colombia": "CO", "comoros": "KM",
    "congo": "CG", "democratic republic of the congo": "CD", "drc": "CD",
    "costa rica": "CR", "croatia": "HR", "cuba": "CU", "cyprus": "CY",
    "czech republic": "CZ", "denmark": "DK", "djibouti": "DJ",
    "dominican republic": "DO", "ecuador": "EC", "egypt": "EG",
    "el salvador": "SV", "equatorial guinea": "GQ", "eritrea": "ER",
    "estonia": "EE", "ethiopia": "ET", "finland": "FI", "france": "FR",
    "gabon": "GA", "gambia": "GM", "georgia": "GE", "germany": "DE",
    "ghana": "GH", "greece": "GR", "guatemala": "GT", "guinea": "GN",
    "guinea-bissau": "GW", "haiti": "HT", "honduras": "HN", "hungary": "HU",
    "india": "IN", "indonesia": "ID", "iran": "IR", "iraq": "IQ",
    "ireland": "IE", "israel": "IL", "italy": "IT", "ivory coast": "CI",
    "cote d'ivoire": "CI", "jamaica": "JM", "japan": "JP", "jordan": "JO",
    "kazakhstan": "KZ", "kenya": "KE", "kosovo": "XK", "kuwait": "KW",
    "kyrgyzstan": "KG", "laos": "LA", "latvia": "LV", "lebanon": "LB",
    "liberia": "LR", "libya": "LY", "lithuania": "LT", "madagascar": "MG",
    "malawi": "MW", "malaysia": "MY", "mali": "ML", "mauritania": "MR",
    "mexico": "MX", "moldova": "MD", "mongolia": "MN", "montenegro": "ME",
    "morocco": "MA", "mozambique": "MZ", "myanmar": "MM", "namibia": "NA",
    "nepal": "NP", "netherlands": "NL", "nicaragua": "NI", "niger": "NE",
    "nigeria": "NG", "north korea": "KP", "north macedonia": "MK",
    "norway": "NO", "oman": "OM", "pakistan": "PK", "panama": "PA",
    "papua new guinea": "PG", "paraguay": "PY", "peru": "PE",
    "philippines": "PH", "poland": "PL", "portugal": "PT", "qatar": "QA",
    "romania": "RO", "russia": "RU", "russian federation": "RU",
    "rwanda": "RW", "saudi arabia": "SA", "senegal": "SN", "serbia": "RS",
    "sierra leone": "SL", "singapore": "SG", "slovakia": "SK",
    "slovenia": "SI", "somalia": "SO", "south africa": "ZA",
    "south korea": "KR", "south sudan": "SS", "spain": "ES",
    "sri lanka": "LK", "sudan": "SD", "sweden": "SE", "switzerland": "CH",
    "syria": "SY", "taiwan": "TW", "tajikistan": "TJ", "tanzania": "TZ",
    "thailand": "TH", "timor-leste": "TL", "togo": "TG", "tunisia": "TN",
    "turkey": "TR", "turkmenistan": "TM", "uganda": "UG", "ukraine": "UA",
    "united arab emirates": "AE", "uae": "AE", "united kingdom": "GB",
    "united states": "US", "usa": "US", "uruguay": "UY", "uzbekistan": "UZ",
    "venezuela": "VE", "vietnam": "VN", "viet nam": "VN",
    "west bank": "PS", "gaza": "PS", "palestine": "PS",
    "yemen": "YE", "zambia": "ZM", "zimbabwe": "ZW",
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).parent.parent
SIGNALS_PATH = REPO_ROOT / "data" / "fara_signals.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def country_to_iso2(name: str) -> str:
    if not name:
        return "XX"
    return COUNTRY_NAME_TO_ISO2.get(name.strip().lower(), "XX")


def load_profile_score(iso2: str):
    p = REPO_ROOT / "data" / "profiles" / f"{iso2}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text()).get("structural_interest_score")
    except Exception:
        return None


def raw_score_for(iso2: str) -> float:
    if iso2 == "XX":
        return 0.0
    score = load_profile_score(iso2)
    return float(score) if score is not None else 0.0


def extract_rows(data: dict) -> list:
    """
    Return the list of registrant records from the API response.
    REGISTRANT_NEW_LIST is the expected key; if absent or empty, log all
    top-level keys so the actual key name is visible in the run log.
    """
    rows = data.get("REGISTRANT_NEW_LIST") or []
    if not rows:
        top_keys = list(data.keys())
        print(f"[fara] REGISTRANT_NEW_LIST missing or empty. "
              f"Top-level keys in response: {top_keys}")
        # Try common alternates before giving up
        for alt in ("registrantNewList", "RegistrantNewList", "registrants", "results"):
            rows = data.get(alt) or []
            if rows:
                print(f"[fara] Found records under key '{alt}' ({len(rows)} rows)")
                break
    return rows


def to_signal(record: dict) -> dict:
    registrant        = (record.get("registrantName") or "").strip()
    foreign_principal = (record.get("foreignPrincipalName") or "").strip()
    country_name      = (record.get("country") or "").strip()
    filed_date        = (record.get("registrationDate") or "").strip()
    reg_number        = record.get("registrationNumber")

    iso = country_to_iso2(country_name)

    title = f"{registrant} — {foreign_principal}" if foreign_principal else registrant
    description = ", ".join(filter(None, [registrant, foreign_principal, country_name]))

    return {
        "registration_number": reg_number,
        "iso": iso,
        "source": "fara",
        "signal_date": filed_date,
        "title": title,
        "value_usd": None,
        "description": description,
        "raw_score": raw_score_for(iso),
        "weight": 1.0,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today     = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=LOOKBACK_DAYS)
    from_str  = yesterday.strftime("%m-%d-%Y")
    to_str    = today.strftime("%m-%d-%Y")

    params = {"from": from_str, "to": to_str}
    url = FARA_API_BASE + "?" + urllib.parse.urlencode(params)

    print(f"[fara] GET {url}")
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"[fara] ERROR: request failed: {e}", file=sys.stderr)
        _write_error(str(e))
        sys.exit(0)

    rows = extract_rows(data)
    print(f"[fara] {len(rows)} new registrant(s) returned")

    # Load existing signals; deduplicate by registration_number
    if SIGNALS_PATH.exists():
        try:
            existing = json.loads(SIGNALS_PATH.read_text())
        except Exception:
            existing = {"generated_at": None, "sources": ["fara"], "signals": []}
    else:
        existing = {"generated_at": None, "sources": ["fara"], "signals": []}

    known_reg_numbers = {
        s.get("registration_number")
        for s in existing.get("signals", [])
        if s.get("registration_number") is not None
    }

    new_signals = []
    for row in rows:
        reg_num = row.get("registrationNumber")
        if reg_num in known_reg_numbers:
            print(f"[fara] reg {reg_num} already present — skip")
            continue
        sig = to_signal(row)
        new_signals.append(sig)
        known_reg_numbers.add(reg_num)
        print(f"[fara] + reg {reg_num}  {sig['iso']}  {sig['title'][:60]}")

    print(f"[fara] {len(new_signals)} new signal(s)")

    all_signals = existing.get("signals", []) + new_signals
    all_signals.sort(key=lambda s: s.get("signal_date") or "")

    SIGNALS_PATH.write_text(json.dumps({
        "generated_at": today.isoformat(),
        "sources": ["fara"],
        "signals": all_signals,
    }, indent=2))
    print(f"[fara] Wrote {len(all_signals)} total signals ({len(new_signals)} new) → {SIGNALS_PATH}")


def _write_error(error: str):
    try:
        existing = json.loads(SIGNALS_PATH.read_text()) if SIGNALS_PATH.exists() else {}
    except Exception:
        existing = {}
    existing.update({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": ["fara"],
        "error": error,
    })
    existing.setdefault("signals", [])
    SIGNALS_PATH.write_text(json.dumps(existing, indent=2))


if __name__ == "__main__":
    main()
