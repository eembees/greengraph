"""Generate ISO 3166 reference data files for greengraph.

Writes two CSV files (no header) to the init/ directory:
  - countries_iso3166_1.txt   code,name        (ISO 3166-1 alpha-2)
  - regions_iso3166_2.txt     code,country_code,name  (ISO 3166-2 subdivisions)

Usage:
    python scripts/generate_iso3166_data.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

try:
    import pycountry
except ImportError:
    print("pycountry not installed. Run: pip install pycountry", file=sys.stderr)
    sys.exit(1)

INIT_DIR = Path(__file__).parent.parent / "init"
COUNTRIES_FILE = INIT_DIR / "countries_iso3166_1.txt"
REGIONS_FILE = INIT_DIR / "regions_iso3166_2.txt"


def generate_countries() -> int:
    rows: list[tuple[str, str]] = []
    for country in sorted(pycountry.countries, key=lambda c: c.alpha_2):
        rows.append((country.alpha_2, country.name))

    with COUNTRIES_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)

    return len(rows)


def generate_regions() -> int:
    rows: list[tuple[str, str, str]] = []
    for sub in sorted(pycountry.subdivisions, key=lambda s: s.code):
        # Only include direct country subdivisions (parent_code is None means top-level)
        rows.append((sub.code, sub.country_code, sub.name))

    with REGIONS_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)

    return len(rows)


if __name__ == "__main__":
    INIT_DIR.mkdir(exist_ok=True)
    n_countries = generate_countries()
    n_regions = generate_regions()
    print(f"Written {n_countries} countries → {COUNTRIES_FILE}")
    print(f"Written {n_regions} regions    → {REGIONS_FILE}")
