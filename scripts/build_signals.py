#!/usr/bin/env python3
"""
build_signals.py — Aggregation layer for HARPY.
Reads all data/*_signals.json source files, merges into data/signals.json.
"""

import json
import glob
import os
from datetime import datetime, timezone

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "signals.json")


def main():
    pattern = os.path.join(DATA_DIR, "*_signals.json")
    source_files = sorted(glob.glob(pattern))

    all_signals = []
    sources_found = []
    counts_per_source = {}

    for path in source_files:
        # Skip the output file if it somehow matches
        if os.path.abspath(path) == os.path.abspath(OUTPUT_FILE):
            continue

        filename = os.path.basename(path)
        # Derive source name: strip "_signals.json" suffix
        source_name = filename.replace("_signals.json", "")

        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ERROR reading {filename}: {e}")
            continue

        signals = data.get("signals", [])

        # Add weight field to every record
        weighted = []
        for sig in signals:
            record = dict(sig)
            record["weight"] = 1.0
            weighted.append(record)

        all_signals.extend(weighted)
        sources_found.append(source_name)
        counts_per_source[source_name] = len(weighted)
        print(f"  Found: {filename}  ({len(weighted)} records)")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": sources_found,
        "signals": all_signals,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nTotal records: {len(all_signals)}")
    print(f"Sources: {sources_found}")
    print("Records per source:")
    for src, count in counts_per_source.items():
        print(f"  {src}: {count}")

    print("\nFirst 3 records:")
    for i, rec in enumerate(all_signals[:3]):
        print(f"  [{i}] {json.dumps(rec)}")

    print(f"\nWrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
