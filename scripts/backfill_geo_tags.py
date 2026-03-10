"""Backfill ISO 3166 geo-tags for all existing documents.

For each document already in the database this script:
  1. Reconstructs the document text by concatenating its stored chunks.
  2. Runs the same ``detect_geo_entities`` logic used during ingestion.
  3. Writes ``geo_countries`` and ``geo_regions`` lists into
     ``documents.metadata``.
  4. Creates ``Document -[:MENTIONS]-> Country/Region`` edges in the AGE graph.

Documents that already have geo-tags (i.e. ``metadata ? 'geo_countries'``) are
skipped unless ``--force`` is passed.

Usage:
    python scripts/backfill_geo_tags.py [--force] [--skip-graph] [--dry-run]
    python scripts/backfill_geo_tags.py --doc-id 42          # single document
    python scripts/backfill_geo_tags.py --dry-run            # preview only
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from greengraph.db import get_conn
from greengraph.geo_tagger import detect_geo_entities, tag_document_in_graph

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _reconstruct_text(conn, doc_id: int) -> str:  # type: ignore[no-untyped-def]
    """Concatenate stored chunk content to reconstruct the full document text."""
    rows = conn.execute(
        "SELECT content FROM chunks WHERE document_id = %s ORDER BY chunk_index",
        (doc_id,),
    ).fetchall()
    return " ".join(r["content"] for r in rows)


def backfill(
    *,
    force: bool = False,
    skip_graph: bool = False,
    dry_run: bool = False,
    doc_id: int | None = None,
) -> dict[str, int]:
    """Run the geo-tag backfill.

    Args:
        force:      Re-tag documents that already have geo_countries set.
        skip_graph: Only update relational metadata; skip AGE edge creation.
        dry_run:    Detect and report tags without writing anything.
        doc_id:     Limit to a single document ID.

    Returns:
        Summary dict with counts for processed / tagged / skipped / errors.
    """
    counts = {"processed": 0, "tagged": 0, "skipped": 0, "errors": 0}

    # Step 1: Fetch the document list in a short-lived connection.
    # Keeping this separate ensures the query connection is clean before we
    # start mutating rows.
    with get_conn() as conn:
        if doc_id is not None:
            where = "WHERE id = %s"
            params: tuple[object, ...] = (doc_id,)
        elif force:
            where = ""
            params = ()
        else:
            # Skip documents that already carry geo tags
            where = "WHERE NOT (metadata ? 'geo_countries')"
            params = ()

        rows = conn.execute(
            f"SELECT id, title FROM documents {where} ORDER BY id",
            params,
        ).fetchall()

    if not rows:
        log.info("No documents to process.")
        return counts

    log.info("Processing %d document(s)...", len(rows))

    # Step 2: Process each document in its own connection + transaction.
    # A PostgreSQL error in one document (e.g. a Cypher failure) would abort
    # the whole transaction if we shared one connection across all documents.
    # Using a fresh connection per document ensures failures are isolated.
    for row in rows:
        d_id: int = row["id"]
        title: str = row["title"]
        counts["processed"] += 1

        try:
            with get_conn() as conn:
                text = _reconstruct_text(conn, d_id)
                if not text.strip():
                    log.warning("doc %d (%s): no chunk content found, skipping", d_id, title)
                    counts["skipped"] += 1
                    continue

                country_codes, region_codes = detect_geo_entities(text, conn)

                if not country_codes and not region_codes:
                    log.debug("doc %d (%s): no geo entities detected", d_id, title)
                    counts["skipped"] += 1
                    continue

                log.info(
                    "doc %d (%s): %d countries %s, %d regions %s",
                    d_id,
                    title,
                    len(country_codes),
                    country_codes,
                    len(region_codes),
                    region_codes,
                )

                if dry_run:
                    counts["tagged"] += 1
                    continue

                if skip_graph:
                    import json

                    conn.execute(
                        """
                        UPDATE documents
                        SET metadata = metadata
                            || jsonb_build_object('geo_countries', %s::jsonb)
                            || jsonb_build_object('geo_regions',   %s::jsonb)
                        WHERE id = %s
                        """,
                        (json.dumps(country_codes), json.dumps(region_codes), d_id),
                    )
                else:
                    tag_document_in_graph(conn, d_id, country_codes, region_codes)

                counts["tagged"] += 1

        except Exception as exc:
            log.error("doc %d (%s): error — %s", d_id, title, exc)
            counts["errors"] += 1

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-process documents that already have geo_countries in metadata.",
    )
    parser.add_argument(
        "--skip-graph",
        action="store_true",
        help="Update relational metadata only; do not create AGE graph edges.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect and log tags without writing anything to the database.",
    )
    parser.add_argument(
        "--doc-id",
        type=int,
        default=None,
        metavar="ID",
        help="Backfill a single document by its relational ID.",
    )
    args = parser.parse_args()

    result = backfill(
        force=args.force,
        skip_graph=args.skip_graph,
        dry_run=args.dry_run,
        doc_id=args.doc_id,
    )

    mode = "[DRY RUN] " if args.dry_run else ""
    print(
        f"\n{mode}Backfill complete:\n"
        f"  Processed : {result['processed']}\n"
        f"  Tagged    : {result['tagged']}\n"
        f"  Skipped   : {result['skipped']}\n"
        f"  Errors    : {result['errors']}"
    )
    if result["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
