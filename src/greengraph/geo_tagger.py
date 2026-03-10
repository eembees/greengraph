"""ISO 3166 geo-tagging for documents.

Detects country and region mentions in text using:
  - ISO 3166-2 region codes via regex (e.g. AR-V, US-CA, GB-ENG)
  - Country names via case-insensitive word-boundary matching

When a region is detected its parent country is also tagged automatically,
implementing the required AR-V → AR hierarchy.

Data is loaded once from the ``iso3166_countries`` / ``iso3166_regions``
PostgreSQL tables (populated by ``init/03-load-iso3166.sql``) and cached
in module-level variables for the lifetime of the process.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import psycopg

from greengraph.config import settings
from greengraph.db import load_age

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level cache (populated on first call to _ensure_loaded)
# ---------------------------------------------------------------------------

_countries: dict[str, str] | None = None          # alpha-2 code → name
_country_name_re: re.Pattern[str] | None = None   # compiled OR of all country names
_name_to_country: dict[str, str] | None = None    # lower(name) → alpha-2 code

_region_code_set: set[str] | None = None           # valid region codes
_region_to_country: dict[str, str] | None = None  # region code → alpha-2 code
_region_name_re: re.Pattern[str] | None = None    # compiled OR of all region names
_name_to_region: dict[str, str] | None = None     # lower(name) → region code

# Regex for bare region codes in running text (e.g. "AR-V", "US-CA", "GB-ENG")
# Must be uppercase and match the format: 2 uppercase letters, dash, 1-3 alphanumeric chars.
_REGION_CODE_PATTERN = re.compile(r"\b([A-Z]{2}-[A-Z0-9]{1,3})\b")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_name_regex(names: list[str]) -> re.Pattern[str]:
    """Build a word-boundary regex that matches any of *names* (longest first)."""
    # Sort longest-first so that "United States" is tried before "United"
    sorted_names = sorted(names, key=len, reverse=True)
    alternation = "|".join(re.escape(n) for n in sorted_names)
    return re.compile(r"\b(?:" + alternation + r")\b", re.IGNORECASE)


def _ensure_loaded(conn: psycopg.Connection[Any]) -> None:
    """Load ISO 3166 data from PostgreSQL into the module cache (once per process)."""
    global _countries, _country_name_re, _name_to_country
    global _region_code_set, _region_to_country, _region_name_re, _name_to_region

    if _countries is not None:
        return  # already loaded

    try:
        country_rows = conn.execute(
            "SELECT code, name FROM iso3166_countries ORDER BY code"
        ).fetchall()
        region_rows = conn.execute(
            "SELECT code, country_code, name FROM iso3166_regions ORDER BY code"
        ).fetchall()
    except Exception as exc:
        log.warning(
            "iso3166 tables not found — geo-tagging disabled. "
            "Run init/03-load-iso3166.sql to populate them. (%s)",
            exc,
        )
        # Set to empty so we don't retry on every document
        _countries = {}
        _country_name_re = re.compile(r"(?!)")  # never matches
        _name_to_country = {}
        _region_code_set = set()
        _region_to_country = {}
        _region_name_re = re.compile(r"(?!)")
        _name_to_region = {}
        return

    _countries = {r["code"]: r["name"] for r in country_rows}
    _name_to_country = {r["name"].lower(): r["code"] for r in country_rows}
    # Skip very short names (≤3 chars) to avoid false positives in free text
    country_names_for_re = [n for n in _name_to_country if len(n) > 3]
    _country_name_re = _build_name_regex(country_names_for_re)

    _region_code_set = {r["code"] for r in region_rows}
    _region_to_country = {r["code"]: r["country_code"] for r in region_rows}
    _name_to_region = {r["name"].lower(): r["code"] for r in region_rows}
    region_names_for_re = [n for n in _name_to_region if len(n) > 3]
    _region_name_re = _build_name_regex(region_names_for_re)

    log.debug(
        "Loaded %d ISO 3166-1 countries and %d ISO 3166-2 regions",
        len(_countries),
        len(_region_code_set),
    )


def reset_cache() -> None:
    """Clear the module-level cache (useful in tests)."""
    global _countries, _country_name_re, _name_to_country
    global _region_code_set, _region_to_country, _region_name_re, _name_to_region
    _countries = None
    _country_name_re = None
    _name_to_country = None
    _region_code_set = None
    _region_to_country = None
    _region_name_re = None
    _name_to_region = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_geo_entities(
    text: str,
    conn: psycopg.Connection[Any],
) -> tuple[list[str], list[str]]:
    """Detect ISO 3166 country and region codes mentioned in *text*.

    Detection strategy:
    1. Scan for ISO 3166-2 region codes via regex (uppercase, e.g. ``AR-V``).
    2. Scan for country and region names via word-boundary regex.
    3. For every detected region, its parent country is added automatically.

    Args:
        text: Document text (or concatenated chunks) to scan.
        conn: Active psycopg connection (used to load reference data on first call).

    Returns:
        ``(country_codes, region_codes)`` — both sorted, deduplicated lists.
    """
    _ensure_loaded(conn)
    assert _countries is not None
    assert _region_code_set is not None
    assert _region_to_country is not None
    assert _country_name_re is not None
    assert _region_name_re is not None
    assert _name_to_country is not None
    assert _name_to_region is not None

    country_codes: set[str] = set()
    region_codes: set[str] = set()

    # 1. Region codes by regex (most reliable signal — very specific format)
    for match in _REGION_CODE_PATTERN.finditer(text):
        code = match.group(1)
        if code in _region_code_set:
            region_codes.add(code)
            country_codes.add(_region_to_country[code])  # hierarchy: tag parent too

    # 2. Country names
    for match in _country_name_re.finditer(text):
        name_lower = match.group(0).lower()
        code = _name_to_country.get(name_lower)
        if code:
            country_codes.add(code)

    # 3. Region names
    for match in _region_name_re.finditer(text):
        name_lower = match.group(0).lower()
        code = _name_to_region.get(name_lower)
        if code:
            region_codes.add(code)
            country_codes.add(_region_to_country[code])

    return sorted(country_codes), sorted(region_codes)


def tag_document_in_graph(
    conn: psycopg.Connection[Any],
    doc_id: int,
    country_codes: list[str],
    region_codes: list[str],
    graph_name: str | None = None,
) -> int:
    """Create MENTIONS edges from a Document node to Country/Region nodes in AGE.

    Also updates ``documents.metadata`` with ``geo_countries`` and
    ``geo_regions`` lists so the tags are queryable from the relational layer.

    Args:
        conn: Active psycopg connection (AGE will be loaded if needed).
        doc_id: Relational document id.
        country_codes: List of ISO 3166-1 alpha-2 codes.
        region_codes: List of ISO 3166-2 codes.
        graph_name: AGE graph name (defaults to settings.graph_name).

    Returns:
        Number of MENTIONS edges created/merged.
    """
    if not country_codes and not region_codes:
        return 0

    gname = graph_name or settings.graph_name
    edges_created = 0

    # --- Relational metadata update ---
    conn.execute(
        """
        UPDATE documents
        SET metadata = metadata
            || jsonb_build_object('geo_countries', %s::jsonb)
            || jsonb_build_object('geo_regions',   %s::jsonb)
        WHERE id = %s
        """,
        (json.dumps(country_codes), json.dumps(region_codes), doc_id),
    )

    # --- AGE graph edges ---
    try:
        load_age(conn)

        for code in country_codes:
            safe_code = code.replace("\\", "\\\\").replace('"', '\\"')
            cypher = (
                f'MATCH (d:Document {{id: {doc_id}}}), '
                f'(c:Country {{code: "{safe_code}"}}) '
                f'MERGE (d)-[:MENTIONS]->(c)'
            )
            sql = f"SELECT * FROM cypher('{gname}', $$ {cypher} $$) AS (v agtype)"
            conn.execute(sql)
            edges_created += 1

        for code in region_codes:
            safe_code = code.replace("\\", "\\\\").replace('"', '\\"')
            cypher = (
                f'MATCH (d:Document {{id: {doc_id}}}), '
                f'(r:Region {{code: "{safe_code}"}}) '
                f'MERGE (d)-[:MENTIONS]->(r)'
            )
            sql = f"SELECT * FROM cypher('{gname}', $$ {cypher} $$) AS (v agtype)"
            conn.execute(sql)
            edges_created += 1

    except Exception as exc:
        log.warning("Geo graph edge creation failed for doc %d: %s", doc_id, exc)

    return edges_created
