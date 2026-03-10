"""Document ingestion pipeline.

Stages:
  1. Document intake — deduplicate by source_hash, store in documents table.
  2. Chunking — split content into overlapping text chunks.
  3. Embedding — batch-embed chunks via the configured backend.
  4. Entity extraction — use Claude (LLM-based NER) to extract named entities.
  5. Geo-tagging — detect ISO 3166 country/region codes and tag the document.
  6. Graph sync — create AGE nodes and edges for documents, chunks, entities.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import psycopg

from greengraph.chunker import split_text
from greengraph.config import settings
from greengraph.db import get_conn, load_age, merge_graph_node
from greengraph.embeddings import EmbeddingBackend, embed_texts, get_backend
from greengraph.geo_tagger import detect_geo_entities, tag_document_in_graph
from greengraph.models import DocumentCreate, IngestionResult

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def ingest_document(
    doc: DocumentCreate,
    embedding_backend: EmbeddingBackend | None = None,
    *,
    skip_graph: bool = False,
    skip_entities: bool = False,
    skip_geo: bool = False,
) -> IngestionResult:
    """Ingest a single document through the full pipeline.

    Args:
        doc: Document to ingest.
        embedding_backend: Optional custom embedding backend (uses configured default).
        skip_graph: If True, skip Apache AGE graph sync (useful when AGE is not available).
        skip_entities: If True, skip entity extraction.
        skip_geo: If True, skip ISO 3166 geo-tagging step.

    Returns:
        IngestionResult summarising what was created.
    """
    backend = embedding_backend or get_backend()

    with get_conn() as conn:
        # Stage 1: Intake
        doc_id, skipped = _upsert_document(conn, doc)
        if skipped:
            return IngestionResult(
                document_id=doc_id,
                title=doc.title,
                chunks_created=0,
                entities_extracted=0,
                relationships_created=0,
                skipped=True,
                skip_reason="Document already ingested (duplicate source_hash)",
            )

        # Stage 2 & 3: Chunk + embed
        chunk_texts = split_text(doc.content, settings.chunk_size, settings.chunk_overlap)
        embeddings = embed_texts(chunk_texts, backend)
        chunk_ids = _insert_chunks(conn, doc_id, chunk_texts, embeddings)
        log.info("Created %d chunks for document %d", len(chunk_ids), doc_id)

        # Stage 4: Entity extraction
        entities_extracted = 0
        relationships_created = 0
        entity_records: list[dict[str, Any]] = []

        if not skip_entities:
            all_text = " ".join(chunk_texts[:5])  # sample first 5 chunks for extraction
            raw_entities = _extract_entities(all_text)
            entity_ids = _insert_entities(conn, raw_entities, backend)
            entities_extracted = len(entity_ids)
            entity_records = list(zip(raw_entities, entity_ids, strict=True))

        # Stage 5: Geo-tagging (ISO 3166 country + region detection)
        if not skip_geo:
            geo_text = " ".join(chunk_texts)  # use full document text for geo detection
            country_codes, region_codes = detect_geo_entities(geo_text, conn)
            if country_codes or region_codes:
                log.info(
                    "Geo-tagged doc %d: %d countries, %d regions",
                    doc_id,
                    len(country_codes),
                    len(region_codes),
                )
                if not skip_graph:
                    try:
                        tag_document_in_graph(conn, doc_id, country_codes, region_codes)
                    except Exception as exc:
                        log.warning("Geo graph tagging failed for doc %d: %s", doc_id, exc)
                else:
                    # Still update relational metadata even when skipping graph
                    _update_geo_metadata(conn, doc_id, country_codes, region_codes)

        # Stage 6: Graph sync
        if not skip_graph:
            try:
                load_age(conn)
                _sync_document_node(conn, doc_id, doc)
                _sync_chunk_nodes(conn, doc_id, chunk_ids, chunk_texts)
                if entity_records:
                    rel_count = _sync_entity_nodes_and_edges(
                        conn, doc_id, chunk_ids, entity_records
                    )
                    relationships_created = rel_count
                # Derive edges: CONTAINS, MENTIONS, MENTIONED_TOGETHER_WITH
                row = conn.execute(
                    "SELECT public.sync_graph_edges(%s) AS result", (doc_id,)
                ).fetchone()
                if row:
                    log.debug("sync_graph_edges: %s", row["result"])
            except Exception as exc:
                log.warning("Graph sync failed (AGE may not be available): %s", exc)

        return IngestionResult(
            document_id=doc_id,
            title=doc.title,
            chunks_created=len(chunk_ids),
            entities_extracted=entities_extracted,
            relationships_created=relationships_created,
        )


# ---------------------------------------------------------------------------
# Stage implementations
# ---------------------------------------------------------------------------


def _upsert_document(conn: psycopg.Connection[Any], doc: DocumentCreate) -> tuple[int, bool]:
    """Insert document if not exists (dedup by source_hash). Returns (id, skipped)."""
    existing = conn.execute(
        "SELECT id FROM documents WHERE source_hash = %s",
        (doc.source_hash,),
    ).fetchone()

    if existing:
        return existing["id"], True

    row = conn.execute(
        """
        INSERT INTO documents (title, source_url, source_hash, metadata)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (doc.title, doc.source_url, doc.source_hash, json.dumps(doc.metadata)),
    ).fetchone()
    assert row is not None
    return row["id"], False


def _insert_chunks(
    conn: psycopg.Connection[Any],
    document_id: int,
    texts: list[str],
    embeddings: list[list[float]],
) -> list[int]:
    """Bulk-insert chunks with embeddings. Returns list of created chunk ids."""
    ids: list[int] = []
    for idx, (text, emb) in enumerate(zip(texts, embeddings, strict=True)):
        row = conn.execute(
            """
            INSERT INTO chunks (document_id, content, chunk_index, embedding)
            VALUES (%s, %s, %s, %s::vector)
            ON CONFLICT (document_id, chunk_index) DO UPDATE
                SET content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding
            RETURNING id
            """,
            (document_id, text, idx, _format_vector(emb)),
        ).fetchone()
        assert row is not None
        ids.append(row["id"])
    return ids


def _insert_entities(
    conn: psycopg.Connection[Any],
    entities: list[dict[str, Any]],
    backend: EmbeddingBackend,
) -> list[int]:
    """Insert entities (upsert by name+type) and store embeddings."""
    if not entities:
        return []

    entity_texts = [e["name"] for e in entities]
    embeddings = backend.embed(entity_texts)
    ids: list[int] = []

    for entity, emb in zip(entities, embeddings, strict=True):
        row = conn.execute(
            """
            INSERT INTO entities (name, type, properties, embedding)
            VALUES (%s, %s, %s, %s::vector)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (
                entity["name"],
                entity["type"],
                json.dumps(entity.get("properties", {})),
                _format_vector(emb),
            ),
        ).fetchone()
        if row is None:
            # Already exists — fetch id
            row = conn.execute(
                "SELECT id FROM entities WHERE name = %s AND type = %s",
                (entity["name"], entity["type"]),
            ).fetchone()
        assert row is not None
        ids.append(row["id"])

    return ids


def _extract_entities(text: str) -> list[dict[str, Any]]:
    """Use OpenAI to extract named entities from text.

    Falls back to an empty list if OPENAI_API_KEY is not set.
    """
    if not settings.openai_api_key:
        log.debug("OPENAI_API_KEY not set — skipping LLM entity extraction")
        return []

    try:
        from openai import OpenAI

        client = OpenAI(api_key=settings.openai_api_key)
        prompt = (
            "Extract named entities from the following text. "
            "Return a JSON array where each element has: "
            '{"name": "...", "type": "person|organization|concept|location|other", '
            '"properties": {}}. '
            "Only return the JSON array, no explanation.\n\n"
            f"Text:\n{text[:3000]}"
        )
        response = client.chat.completions.create(
            model=settings.openai_chat_model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        content = response.choices[0].message.content or ""
        content = content.strip()
        # Strip markdown code fences if present
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        entities: list[dict[str, Any]] = json.loads(content)
        return [e for e in entities if "name" in e and "type" in e]
    except Exception as exc:
        log.warning("Entity extraction failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Graph sync helpers
# ---------------------------------------------------------------------------


def _sync_document_node(conn: psycopg.Connection[Any], doc_id: int, doc: DocumentCreate) -> None:
    merge_graph_node(
        conn,
        settings.graph_name,
        "Document",
        match_props={"id": doc_id},
        set_props={"title": doc.title, "source": doc.source_url or ""},
    )


def _sync_chunk_nodes(
    conn: psycopg.Connection[Any],
    doc_id: int,
    chunk_ids: list[int],
    chunk_texts: list[str],
) -> None:
    for chunk_id, text in zip(chunk_ids, chunk_texts, strict=True):
        merge_graph_node(
            conn,
            settings.graph_name,
            "Chunk",
            match_props={"id": chunk_id},
            set_props={"content_preview": text[:200], "document_id": doc_id},
        )
        # Document -[CONTAINS]-> Chunk
        _create_edge_if_not_exists(conn, "Document", doc_id, "Chunk", chunk_id, "CONTAINS")


def _sync_entity_nodes_and_edges(
    conn: psycopg.Connection[Any],
    doc_id: int,
    chunk_ids: list[int],
    entity_records: list[tuple[dict[str, Any], int]],
) -> int:
    rel_count = 0
    for entity_dict, entity_id in entity_records:
        merge_graph_node(
            conn,
            settings.graph_name,
            "Entity",
            match_props={"id": entity_id},
            set_props={"name": entity_dict["name"], "type": entity_dict["type"]},
        )
        # Document -[MENTIONS]-> Entity (via first chunk for simplicity)
        if chunk_ids:
            _create_edge_if_not_exists(conn, "Chunk", chunk_ids[0], "Entity", entity_id, "MENTIONS")
            rel_count += 1
    return rel_count


def _create_edge_if_not_exists(
    conn: psycopg.Connection[Any],
    from_label: str,
    from_id: int,
    to_label: str,
    to_id: int,
    edge_label: str,
) -> None:
    """Create edge only if it does not already exist (idempotent)."""
    cypher = (
        f"MATCH (a:{from_label} {{id: {from_id}}}), (b:{to_label} {{id: {to_id}}}) "
        f"MERGE (a)-[:{edge_label}]->(b)"
    )
    sql = f"SELECT * FROM cypher('{settings.graph_name}', $$ {cypher} $$) AS (v agtype)"
    try:
        conn.execute(sql)
    except Exception as exc:
        log.debug("Edge creation skipped: %s", exc)


def _format_vector(embedding: list[float]) -> str:
    """Format a Python float list as a pgvector literal string."""
    return "[" + ",".join(str(x) for x in embedding) + "]"


def _update_geo_metadata(
    conn: psycopg.Connection[Any],
    doc_id: int,
    country_codes: list[str],
    region_codes: list[str],
) -> None:
    """Write geo-tag lists into documents.metadata without touching AGE."""
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
