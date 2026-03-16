import json
from pathlib import Path
from country_data import COUNTRIES

PROFILES_DIR = Path("data/profiles")
PROFILES_DIR.mkdir(exist_ok=True)

for i, country in enumerate(COUNTRIES, 1):
    iso = country["iso"]
    name = country["name"]
    profile = {
        "iso": iso,
        "name": name,
        "structural_interest_score": country.get("structural_interest_score", 0),
        "score_rationale": country.get("score_rationale", "pending"),
        "key_structural_interests": country.get("key_structural_interests", []),
        "suppression_baseline": country.get("suppression_baseline", "low_coverage"),
        "last_reviewed": "2026-03-15",
        "last_generated": "2026-03-16",
    }
    out = PROFILES_DIR / f"{iso}.json"
    out.write_text(json.dumps(profile, indent=2))
    print(f"[{i}/{len(COUNTRIES)}] {iso} → {profile['structural_interest_score']}")

print(f"\nDone. {len(COUNTRIES)} files written to {PROFILES_DIR}/")
