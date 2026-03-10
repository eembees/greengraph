"""Entity deduplication utilities.

Two strategies are available:

``string``  -- groups entities whose names match after case-folding and
               whitespace normalisation (same type required).  Fast, no
               embedding calls needed.

``embedding`` -- groups entities whose embedding vectors are within a
                cosine-distance threshold.  Not yet implemented.

Merging always:
1. Keeps the entity with the lowest id as the canonical record.
2. Redirects ``Chunk -[MENTIONS]->`` edges in the AGE graph to the
   canonical entity, then DETACH DELETEs the duplicate AGE node.
3. Deletes the duplicate row from the relational ``entities`` table
   (cascades to ``metadata_tags``).
4. Calls ``sync_graph_edges`` to rebuild derived edges
   (Document-MENTIONS-Entity, co-mention pairs).
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

from greengraph.config import settings
from greengraph.db import get_conn, load_age

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def dedup_entities(
    strategy: str = "string",
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Deduplicate entities in the database.

    Args:
        strategy: ``"string"`` (case-insensitive name match) or
                  ``"embedding"`` (cosine distance — not yet implemented).
        dry_run:  If True, find duplicates and return counts without
                  making any changes.

    Returns:
        Dict with ``groups_found``, ``entities_merged``, ``dry_run``.
    """
    if strategy == "embedding":
        raise NotImplementedError(
            "Embedding-based deduplication is not yet implemented."
        )
    if strategy != "string":
        raise ValueError(f"Unknown strategy {strategy!r}. Choose 'string' or 'embedding'.")

    with get_conn() as conn:
        groups = _find_duplicate_groups_string(conn)
        log.info("Found %d duplicate groups", len(groups))

        if dry_run:
            total = sum(len(g) - 1 for g in groups)
            return {"groups_found": len(groups), "entities_merged": total, "dry_run": True}

        load_age(conn)
        merged = 0
        for canonical_id, *dup_ids in groups:
            for dup_id in dup_ids:
                _merge_entity(conn, canonical_id=canonical_id, dup_id=dup_id)
                merged += 1
            log.debug("Merged %d duplicates into entity %d", len(dup_ids), canonical_id)

        # Rebuild derived edges for all documents now that entity nodes changed.
        conn.execute("SELECT sync_graph_edges()")

    return {"groups_found": len(groups), "entities_merged": merged, "dry_run": False}


# ---------------------------------------------------------------------------
# Strategy implementations
# ---------------------------------------------------------------------------


def _find_duplicate_groups_string(
    conn: psycopg.Connection[Any],
) -> list[list[int]]:
    """Return groups of entity IDs that share the same normalised name+type.

    Normalisation: LOWER(TRIM(name)), LOWER(TRIM(type)).
    Each group is sorted ascending by id; index 0 is the canonical entity.
    Only groups with 2+ members are returned.
    """
    rows = conn.execute(
        """
        SELECT ARRAY_AGG(id ORDER BY id) AS ids
        FROM   entities
        GROUP  BY LOWER(TRIM(name)), LOWER(TRIM(type))
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    return [list(row["ids"]) for row in rows]


def _find_duplicate_groups_embedding(
    conn: psycopg.Connection[Any],
    threshold: float = 0.05,
) -> list[list[int]]:
    """Group entities whose embeddings are within *threshold* cosine distance.

    Not yet implemented.
    """
    raise NotImplementedError(
        "Embedding-based deduplication is not yet implemented. "
        "Planned approach: fetch all entity embeddings, build a neighbourhood "
        "graph with cosine distance <= threshold, then extract connected "
        "components as duplicate groups."
    )


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------


def _merge_entity(
    conn: psycopg.Connection[Any],
    *,
    canonical_id: int,
    dup_id: int,
) -> None:
    """Redirect all references from *dup_id* to *canonical_id* then delete it.

    Steps
    -----
    1. Relational: ``metadata_tags.entity_id`` → canonical.
    2. AGE graph: redirect ``Chunk -[MENTIONS]-> dup`` to canonical.
    3. AGE graph: DETACH DELETE the duplicate Entity node.
    4. Relational: DELETE the duplicate entity row (cascades metadata_tags).
    """
    graph = settings.graph_name

    # 1. Relational: redirect metadata tags
    conn.execute(
        "UPDATE metadata_tags SET entity_id = %s WHERE entity_id = %s",
        (canonical_id, dup_id),
    )

    # 2. AGE: redirect Chunk -[MENTIONS]-> dup  →  Chunk -[MENTIONS]-> canonical
    _age_exec(
        conn,
        graph,
        f"MATCH (c:Chunk)-[:MENTIONS]->(dup:Entity {{id: {dup_id}}}), "
        f"(canon:Entity {{id: {canonical_id}}}) "
        f"MERGE (c)-[:MENTIONS]->(canon)",
    )

    # 3. AGE: remove the duplicate node (and all its remaining edges)
    _age_exec(
        conn,
        graph,
        f"MATCH (n:Entity {{id: {dup_id}}}) DETACH DELETE n",
    )

    # 4. Relational: delete the duplicate entity
    conn.execute("DELETE FROM entities WHERE id = %s", (dup_id,))


def _age_exec(
    conn: psycopg.Connection[Any],
    graph: str,
    cypher: str,
) -> None:
    sql = (
        "SELECT * FROM cypher("
        + "'" + graph + "'"
        + ", $$ " + cypher + " $$) AS (v agtype)"
    )
    try:
        conn.execute(sql)
    except Exception as exc:
        log.debug("AGE exec skipped (%s): %s", cypher[:60], exc)
