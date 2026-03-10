#!/usr/bin/env python3
"""Ingest a Webhose news dataset zip into greengraph.

Usage:
    python scripts/ingest_webhose_zip.py <path_to_zip> [options]

Examples:
    # Dry run — show what would be ingested without writing to DB
    python scripts/ingest_webhose_zip.py news.zip --dry-run

    # Ingest (mock embeddings, no entity extraction)
    python scripts/ingest_webhose_zip.py news.zip

    # Ingest with real OpenAI embeddings and entity extraction
    EMBEDDING_PROVIDER=openai python scripts/ingest_webhose_zip.py news.zip

    # Skip the graph layer (faster, no AGE required)
    python scripts/ingest_webhose_zip.py news.zip --skip-graph
"""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from greengraph.ingest import ingest_document
from greengraph.models import DocumentCreate, IngestionResult


def parse_article(raw: dict) -> DocumentCreate | None:
    """Convert a Webhose article dict into a DocumentCreate.

    Returns None if the article lacks required fields or is not in English.
    """
    text = (raw.get("text") or "").strip()
    title = (raw.get("title") or "").strip()

    if not text or not title:
        return None
    if raw.get("language", "english") != "english":
        return None

    metadata: dict = {
        "source": "webhose",
        "sentiment": raw.get("sentiment"),
        "categories": raw.get("categories", []),
        "topics": raw.get("topics", []),
        "author": raw.get("author"),
        "published": raw.get("published"),
        "language": raw.get("language"),
    }

    return DocumentCreate(
        title=title,
        content=text,
        source_url=raw.get("url"),
        metadata=metadata,
    )


def ingest_zip(
    zip_path: Path,
    *,
    dry_run: bool = False,
    skip_graph: bool = False,
    skip_entities: bool = False,
    limit: int | None = None,
) -> None:
    results: list[IngestionResult] = []
    skipped_parse = 0

    with zipfile.ZipFile(zip_path) as zf:
        json_entries = [e for e in zf.namelist() if e.endswith(".json")]
        if limit:
            json_entries = json_entries[:limit]

        total = len(json_entries)
        print(f"Found {total} article(s) in {zip_path.name}")
        if dry_run:
            print("DRY RUN — no data will be written\n")

        for i, entry in enumerate(json_entries, 1):
            with zf.open(entry) as f:
                try:
                    raw = json.load(f)
                except json.JSONDecodeError as exc:
                    print(f"  [{i}/{total}] SKIP (bad JSON): {entry} — {exc}")
                    skipped_parse += 1
                    continue

            doc = parse_article(raw)
            if doc is None:
                print(f"  [{i}/{total}] SKIP (missing fields or non-English): {entry}")
                skipped_parse += 1
                continue

            if dry_run:
                print(f"  [{i}/{total}] WOULD INGEST: {doc.title[:80]}")
                print(f"           url={doc.source_url}")
                print(f"           hash={doc.source_hash[:16]}...")
                continue

            try:
                result = ingest_document(
                    doc,
                    skip_graph=skip_graph,
                    skip_entities=skip_entities,
                )
                results.append(result)
                status = "SKIP (duplicate)" if result.skipped else f"OK  chunks={result.chunks_created} entities={result.entities_extracted}"
                print(f"  [{i}/{total}] {status}: {doc.title[:70]}")
            except Exception as exc:
                print(f"  [{i}/{total}] ERROR: {entry} — {exc}")

    if not dry_run:
        ingested = [r for r in results if not r.skipped]
        dupes = [r for r in results if r.skipped]
        total_chunks = sum(r.chunks_created for r in ingested)
        total_entities = sum(r.entities_extracted for r in ingested)
        print(f"\nDone.")
        print(f"  Ingested:    {len(ingested)} documents")
        print(f"  Duplicates:  {len(dupes)}")
        print(f"  Parse errors:{skipped_parse}")
        print(f"  Chunks:      {total_chunks}")
        print(f"  Entities:    {total_entities}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("zip_path", type=Path, help="Path to the Webhose zip file")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be ingested, don't write to DB")
    parser.add_argument("--skip-graph", action="store_true", help="Skip Apache AGE graph sync")
    parser.add_argument("--skip-entities", action="store_true", help="Skip LLM entity extraction")
    parser.add_argument("--limit", type=int, default=None, help="Only ingest the first N articles")
    args = parser.parse_args()

    if not args.zip_path.exists():
        print(f"Error: file not found: {args.zip_path}", file=sys.stderr)
        sys.exit(1)

    ingest_zip(
        args.zip_path,
        dry_run=args.dry_run,
        skip_graph=args.skip_graph,
        skip_entities=args.skip_entities,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
