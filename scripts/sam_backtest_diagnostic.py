#!/usr/bin/env python3
"""
sam_backtest_diagnostic.py — SAM.gov date-range diagnostic for HARPY backtest

Tests two pre-Ukraine-invasion windows to determine:
  1. Whether SAM.gov allows historical queries (2021–2022 range)
  2. Whether a tighter window returns meaningful record counts

Usage:
    SAM_API_KEY=your_key python scripts/sam_backtest_diagnostic.py
    # or with .env:
    set -a && source .env && set +a && python scripts/sam_backtest_diagnostic.py
"""

import json
import os
import sys

import requests

SAM_API_BASE = "https://api.sam.gov/opportunities/v2/search"

TESTS = [
    {
        "label": "Test 1 — broad pre-invasion window (2021-11-01 → 2022-02-24)",
        "postedFrom": "11/01/2021",
        "postedTo":   "02/24/2022",
    },
    {
        "label": "Test 2 — tight pre-invasion window (2022-02-01 → 2022-02-24)",
        "postedFrom": "02/01/2022",
        "postedTo":   "02/24/2022",
    },
]


def run_test(api_key, label, posted_from, posted_to):
    params = {
        "api_key":     api_key,
        "postedFrom":  posted_from,
        "postedTo":    posted_to,
        "limit":       10,
        "offset":      0,
    }

    # Print URL with key redacted
    req = requests.Request("GET", SAM_API_BASE, params=params).prepare()
    safe_url = req.url.replace(api_key, "***")
    print(f"\n{'='*70}")
    print(f"{label}")
    print(f"URL: {safe_url}")

    try:
        resp = requests.get(SAM_API_BASE, params=params, timeout=30)
    except requests.RequestException as e:
        print(f"REQUEST FAILED: {e}")
        return

    print(f"HTTP {resp.status_code}")

    if resp.status_code != 200:
        try:
            print(f"Error body: {json.dumps(resp.json(), indent=2)[:500]}")
        except Exception:
            print(f"Error body (raw): {resp.text[:500]}")
        return

    try:
        data = resp.json()
    except Exception:
        print(f"Response is not JSON: {resp.text[:500]}")
        return

    total = data.get("totalRecords", "N/A")
    print(f"totalRecords: {total}")

    opps = data.get("opportunitiesData") or []
    if not opps:
        print("No records in this page.")
        return

    print(f"\nFirst {min(3, len(opps))} titles:")
    for opp in opps[:3]:
        title   = opp.get("title") or "(no title)"
        posted  = opp.get("postedDate") or "?"
        agency  = (opp.get("fullParentPathName") or "").split(".")[-1].strip()
        pop     = opp.get("placeOfPerformance") or {}
        country = (pop.get("country") or {}).get("code") or "?"
        print(f"  [{posted}] {title[:80]}")
        print(f"           agency: {agency[:60]}  country: {country}")


def main():
    api_key = os.environ.get("SAM_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] SAM_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    print(f"Key prefix: {api_key[:12]}...")

    for test in TESTS:
        run_test(
            api_key=api_key,
            label=test["label"],
            posted_from=test["postedFrom"],
            posted_to=test["postedTo"],
        )

    print(f"\n{'='*70}")
    print("Done.")


if __name__ == "__main__":
    main()
