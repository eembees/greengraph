"""Two-phase retrieval: vector similarity search + graph context expansion.

Phase 1: pgvector HNSW search returns top-K candidate chunks.
Phase 2: Apache AGE graph traversal expands context around each chunk.
Phase 3 (caller): Assemble into a RetrievalResult for LLM consumption.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

from greengraph.config import settings
from greengraph.db import get_conn, load_age
from greengraph.embeddings import EmbeddingBackend, embed_text, get_backend
from greengraph.models import GraphContext, RetrievalResult, RetrievedChunk

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def retrieve(
    query: str,
    *,
    top_k: int | None = None,
    similarity_threshold: float | None = None,
    metadata_filter: dict[str, Any] | None = None,
    include_graph_context: bool = True,
    embedding_backend: EmbeddingBackend | None = None,
) -> RetrievalResult:
    """Run two-phase retrieval for a natural-language query.

    Args:
        query: User's natural-language query string.
        top_k: Maximum number of chunks to return (default: settings.top_k_chunks).
        similarity_threshold: Minimum cosine similarity (default: settings.vector_similarity_threshold).  # noqa: E501
        metadata_filter: Optional dict of document metadata key/value filters.
        include_graph_context: Whether to run Phase 2 graph expansion.
        embedding_backend: Optional custom embedding backend.

    Returns:
        RetrievalResult with ranked chunks and optional graph context.
    """
    k = top_k or settings.top_k_chunks
    threshold = similarity_threshold or settings.vector_similarity_threshold
    backend = embedding_backend or get_backend()

    query_embedding = embed_text(query, backend)

    with get_conn() as conn:
        # Configure HNSW recall
        conn.execute(f"SET hnsw.ef_search = {settings.hnsw_ef_search}")

        # Phase 1: Vector search
        candidates = _vector_search(conn, query_embedding, k, threshold, metadata_filter)

        # Phase 2: Graph context expansion
        if include_graph_context and candidates:
            _enrich_with_graph_context(conn, candidates)

    return RetrievalResult(
        query=query,
        chunks=candidates,
        total_chunks=len(candidates),
    )


def retrieve_entity_context(
    entity_name: str,
    *,
    hop_depth: int | None = None,
) -> list[dict[str, Any]]:
    """Entity-centric subgraph query — find all context around a named entity.

    Args:
        entity_name: Exact entity name to search for.
        hop_depth: Number of hops to traverse (default: settings.graph_hop_depth).

    Returns:
        List of dicts with keys: title, content, chunk_index, related_entities.
    """
    depth = hop_depth or settings.graph_hop_depth

    with get_conn() as conn:
        try:
            load_age(conn)
            results = _entity_subgraph_query(conn, entity_name, depth)
            return results
        except Exception as exc:
            log.warning("Entity context query failed (AGE may not be available): %s", exc)
            return []


# ---------------------------------------------------------------------------
# Phase 1: Vector search
# ---------------------------------------------------------------------------


def _vector_search(
    conn: psycopg.Connection[Any],
    query_embedding: list[float],
    top_k: int,
    threshold: float,
    metadata_filter: dict[str, Any] | None,
) -> list[RetrievedChunk]:
    """Run pgvector HNSW similarity search, optionally filtered by document metadata."""
    emb_str = _format_vector(query_embedding)

    if metadata_filter:
        # Build WHERE clauses for metadata filters
        filter_clauses = []
        filter_params: list[Any] = [emb_str, emb_str, threshold]
        for key, value in metadata_filter.items():
            filter_clauses.append("d.metadata->>'%s' = %s")
            filter_params.extend([key, str(value)])

        where_extra = " AND ".join(filter_clauses) if filter_clauses else "TRUE"
        sql = f"""  # noqa: S608
            SELECT c.id AS chunk_id, c.document_id, c.content,
                   1 - (c.embedding <=> %s::vector) AS similarity
            FROM chunks c
            JOIN documents d ON c.document_id = d.id
            WHERE 1 - (c.embedding <=> %s::vector) > %s
              AND {where_extra}
            ORDER BY c.embedding <=> %s::vector
            LIMIT %s
        """
        filter_params.extend([emb_str, top_k])
        rows = conn.execute(sql, filter_params).fetchall()
    else:
        sql = """
            SELECT id AS chunk_id, document_id, content,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM chunks
            WHERE 1 - (embedding <=> %s::vector) > %s
            ORDER BY embedding <=> %s::vector
            LIMIT %s
        """
        rows = conn.execute(sql, (emb_str, emb_str, threshold, emb_str, top_k)).fetchall()

    return [
        RetrievedChunk(
            chunk_id=row["chunk_id"],
            document_id=row["document_id"],
            content=row["content"],
            similarity=float(row["similarity"]),
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Phase 2: Graph context expansion
# ---------------------------------------------------------------------------


def _enrich_with_graph_context(
    conn: psycopg.Connection[Any],
    chunks: list[RetrievedChunk],
) -> None:
    """Mutate chunks in-place by adding graph context from AGE traversal."""
    try:
        load_age(conn)
    except Exception as exc:
        log.warning("Could not load AGE — skipping graph expansion: %s", exc)
        return

    for chunk in chunks:
        try:
            context = _graph_expand_chunk(conn, chunk.chunk_id)
            chunk.graph_context = context
        except Exception as exc:
            log.debug("Graph expansion failed for chunk %d: %s", chunk.chunk_id, exc)


def _graph_expand_chunk(
    conn: psycopg.Connection[Any],
    chunk_id: int,
) -> list[GraphContext]:
    """2-hop graph traversal from a chunk node to related entities."""
    cypher = f"""
        MATCH (c:Chunk {{id: {chunk_id}}})-[:MENTIONS]->(e:Entity)
              -[r:RELATED_TO|AUTHORED_BY*1..2]->(related)
        RETURN e.name AS entity,
               type(r) AS relationship,
               related.name AS connected_entity,
               labels(related) AS entity_type
        LIMIT 20
    """
    sql = f"""  # noqa: S608
        SELECT * FROM cypher('{settings.graph_name}', $$ {cypher} $$) AS (
            entity agtype,
            relationship agtype,
            connected_entity agtype,
            entity_type agtype
        )
    """
    rows = conn.execute(sql).fetchall()

    contexts: list[GraphContext] = []
    for row in rows:
        try:
            contexts.append(
                GraphContext(
                    entity=_parse_agtype(row["entity"]),
                    relationship=_parse_agtype(row["relationship"]),
                    connected_entity=_parse_agtype(row["connected_entity"]),
                    entity_type=_parse_agtype(row["entity_type"]),
                )
            )
        except Exception as exc:
            log.debug("Could not parse graph row: %s — %s", row, exc)

    return contexts


def _entity_subgraph_query(
    conn: psycopg.Connection[Any],
    entity_name: str,
    hop_depth: int,
) -> list[dict[str, Any]]:
    """Entity-centric subgraph: find documents and chunks mentioning an entity."""
    escaped = entity_name.replace('"', '\\"')
    cypher = f"""
        MATCH (e:Entity {{name: "{escaped}"}})
              <-[:MENTIONS]-(c:Chunk)
              <-[:CONTAINS]-(d:Document)
        OPTIONAL MATCH (e)-[:RELATED_TO*1..{hop_depth}]->(related)
        RETURN d.title AS title,
               c.content_preview AS content,
               c.chunk_index AS chunk_index,
               collect(DISTINCT related.name) AS related_entities
    """
    sql = f"""  # noqa: S608
        SELECT * FROM cypher('{settings.graph_name}', $$ {cypher} $$) AS (
            title agtype,
            content agtype,
            chunk_index agtype,
            related_entities agtype
        )
    """
    rows = conn.execute(sql).fetchall()
    return [
        {
            "title": _parse_agtype(row["title"]),
            "content": _parse_agtype(row["content"]),
            "chunk_index": _parse_agtype(row["chunk_index"]),
            "related_entities": _parse_agtype(row["related_entities"]),
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_vector(embedding: list[float]) -> str:
    return "[" + ",".join(str(x) for x in embedding) + "]"


def _parse_agtype(value: Any) -> str:
    """Convert an agtype value (returned as string by psycopg) to a Python string."""
    if value is None:
        return ""
    s = str(value)
    # AGE returns strings wrapped in double quotes: "foo" → foo
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s
