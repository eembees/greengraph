-- ==============================================
-- Context Graph Database Initialization
-- Runs once on first container startup
-- ==============================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS age;

-- Load AGE into the search path
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- ==============================================
-- Relational Schema
-- ==============================================

CREATE TABLE documents (
    id              BIGSERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    source_url      TEXT,
    source_hash     TEXT UNIQUE,  -- deduplication key
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    metadata        JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE chunks (
    id              BIGSERIAL PRIMARY KEY,
    document_id     BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    content         TEXT NOT NULL,
    chunk_index     INTEGER NOT NULL,
    embedding       vector(1536),  -- adjust dimension to your model
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (document_id, chunk_index)
);

CREATE TABLE entities (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    type            TEXT NOT NULL,  -- person, organization, concept, etc.
    properties      JSONB DEFAULT '{}'::jsonb,
    embedding       vector(1536),
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE metadata_tags (
    id              BIGSERIAL PRIMARY KEY,
    entity_id       BIGINT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value           TEXT NOT NULL,
    valid_from      TIMESTAMPTZ,
    valid_to        TIMESTAMPTZ
);

-- ==============================================
-- Indexes
-- ==============================================

-- Vector indexes (HNSW for approximate nearest neighbor)
CREATE INDEX idx_chunks_embedding ON chunks
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 128);

CREATE INDEX idx_entities_embedding ON entities
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 128);

-- Relational indexes for common query patterns
CREATE INDEX idx_chunks_document_id ON chunks(document_id);
CREATE INDEX idx_entities_type ON entities(type);
CREATE INDEX idx_entities_name ON entities(name);
CREATE INDEX idx_metadata_tags_key_value ON metadata_tags(key, value);
CREATE INDEX idx_documents_metadata ON documents USING gin(metadata);
CREATE INDEX idx_documents_source_hash ON documents(source_hash);

-- ==============================================
-- Graph Layer (Apache AGE)
-- ==============================================

SELECT create_graph('context_graph');

-- ==============================================
-- Utility: Set search path for AGE in new sessions
-- ==============================================

ALTER DATABASE context_graph_db SET search_path = ag_catalog, "$user", public;

-- Done
SELECT 'Context graph database initialized successfully' AS status;
