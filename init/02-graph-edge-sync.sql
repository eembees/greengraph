-- ==============================================
-- Graph Edge Sync Function
-- Derives and creates all relationship edges
-- from existing nodes and Chunk-MENTIONS-Entity edges.
-- ==============================================

LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- ---------------------------------------------------------------------------
-- sync_graph_edges(p_doc_id BIGINT DEFAULT NULL)
--
-- Creates (idempotently via MERGE):
--   Document -[CONTAINS]-> Chunk         (from relational chunks table)
--   Document -[MENTIONS]-> Entity        (via Document->Chunk->Entity path)
--   Entity -[MENTIONED_TOGETHER_WITH]-> Entity  (co-mentions per document)
--
-- Assumes Document and Chunk nodes already exist in the AGE graph.
-- Requires Chunk -[MENTIONS]-> Entity edges to exist for derived edges.
--
-- Args:
--   p_doc_id  Only sync this document. NULL = sync all documents.
--
-- Returns JSONB: { "docs_processed": N, "contains_edges": N }
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.sync_graph_edges(p_doc_id BIGINT DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql AS
$func$
DECLARE
    v_graph       TEXT   := 'context_graph';
    v_doc_id      BIGINT;
    v_chunk_id    BIGINT;
    v_contains    INT    := 0;
    v_docs        INT    := 0;
    v_cypher      TEXT;
    v_sql         TEXT;
BEGIN
    LOAD 'age';
    SET LOCAL search_path = ag_catalog, "$user", public;

    FOR v_doc_id IN
        SELECT id FROM documents
        WHERE p_doc_id IS NULL OR id = p_doc_id
        ORDER BY id
    LOOP
        v_docs := v_docs + 1;

        -- 1. Document -[CONTAINS]-> Chunk
        --    Source of truth: relational chunks table (document_id FK).
        FOR v_chunk_id IN
            SELECT id FROM chunks
            WHERE document_id = v_doc_id
            ORDER BY chunk_index
        LOOP
            v_cypher := 'MATCH (a:Document {id: ' || v_doc_id || '}), '
                     || '(b:Chunk {id: ' || v_chunk_id || '}) '
                     || 'MERGE (a)-[:CONTAINS]->(b)';
            v_sql := 'SELECT * FROM cypher(' || quote_literal(v_graph)
                  || ', $$ ' || v_cypher || ' $$) AS (v agtype)';
            EXECUTE v_sql;
            v_contains := v_contains + 1;
        END LOOP;

        -- 2. Document -[MENTIONS]-> Entity
        --    Derived from: Document -[CONTAINS]-> Chunk -[MENTIONS]-> Entity.
        v_cypher := 'MATCH (d:Document {id: ' || v_doc_id || '})'
                 || '-[:CONTAINS]->(c:Chunk)-[:MENTIONS]->(e:Entity) '
                 || 'MERGE (d)-[:MENTIONS]->(e)';
        v_sql := 'SELECT * FROM cypher(' || quote_literal(v_graph)
              || ', $$ ' || v_cypher || ' $$) AS (v agtype)';
        EXECUTE v_sql;

        -- 3. Entity -[MENTIONED_TOGETHER_WITH]-> Entity
        --    Symmetric: any two distinct entities co-occurring in the same
        --    document (via any chunk) get a directed edge in both directions.
        v_cypher := 'MATCH (d:Document {id: ' || v_doc_id || '})'
                 || '-[:CONTAINS]->(:Chunk)-[:MENTIONS]->(e1:Entity), '
                 || '(d)-[:CONTAINS]->(:Chunk)-[:MENTIONS]->(e2:Entity) '
                 || 'WHERE e1 <> e2 '
                 || 'MERGE (e1)-[:MENTIONED_TOGETHER_WITH]->(e2)';
        v_sql := 'SELECT * FROM cypher(' || quote_literal(v_graph)
              || ', $$ ' || v_cypher || ' $$) AS (v agtype)';
        EXECUTE v_sql;

    END LOOP;

    RETURN jsonb_build_object(
        'docs_processed',  v_docs,
        'contains_edges',  v_contains
    );
END;
$func$;
