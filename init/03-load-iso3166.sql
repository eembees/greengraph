-- ==============================================
-- ISO 3166 Reference Data
-- Loads country and region codes into:
--   1. Relational lookup tables (iso3166_countries, iso3166_regions)
--   2. Apache AGE graph nodes (Country, Region) with PART_OF hierarchy edges
-- ==============================================

LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- ==============================================
-- Relational lookup tables
-- ==============================================

CREATE TABLE iso3166_countries (
    code  CHAR(2) PRIMARY KEY,
    name  TEXT    NOT NULL
);

CREATE TABLE iso3166_regions (
    code          TEXT    PRIMARY KEY,    -- e.g. AR-V
    country_code  CHAR(2) NOT NULL REFERENCES iso3166_countries(code),
    name          TEXT    NOT NULL
);

CREATE INDEX idx_iso3166_regions_country ON iso3166_regions(country_code);
CREATE INDEX idx_iso3166_countries_name  ON iso3166_countries(name);
CREATE INDEX idx_iso3166_regions_name    ON iso3166_regions(name);

-- Load from the data files (copied alongside this script into /docker-entrypoint-initdb.d/)
COPY iso3166_countries FROM '/docker-entrypoint-initdb.d/countries_iso3166_1.txt'
    WITH (FORMAT CSV, ENCODING 'UTF8');

COPY iso3166_regions FROM '/docker-entrypoint-initdb.d/regions_iso3166_2.txt'
    WITH (FORMAT CSV, ENCODING 'UTF8');

-- ==============================================
-- AGE graph nodes: Country
-- ==============================================

DO $$
DECLARE
    rec       RECORD;
    v_graph   TEXT := 'context_graph';
    v_cypher  TEXT;
    v_sql     TEXT;
    safe_code TEXT;
    safe_name TEXT;
BEGIN
    FOR rec IN SELECT code, name FROM iso3166_countries ORDER BY code LOOP
        -- Escape backslashes first, then double-quotes, for safe Cypher double-quoted strings
        safe_code := replace(replace(rec.code, '\', '\\'), '"', '\"');
        safe_name := replace(replace(rec.name, '\', '\\'), '"', '\"');

        v_cypher := 'MERGE (:Country {code: "' || safe_code || '", name: "' || safe_name || '"})';
        v_sql    := 'SELECT * FROM cypher(' || quote_literal(v_graph)
                 || ', $$ ' || v_cypher || ' $$) AS (v agtype)';
        EXECUTE v_sql;
    END LOOP;
END;
$$;

-- ==============================================
-- AGE graph nodes: Region + PART_OF edges
-- ==============================================

DO $$
DECLARE
    rec              RECORD;
    v_graph          TEXT := 'context_graph';
    v_cypher         TEXT;
    v_sql            TEXT;
    safe_code        TEXT;
    safe_name        TEXT;
    safe_country     TEXT;
BEGIN
    FOR rec IN SELECT code, country_code, name FROM iso3166_regions ORDER BY code LOOP
        safe_code    := replace(replace(rec.code,         '\', '\\'), '"', '\"');
        safe_name    := replace(replace(rec.name,         '\', '\\'), '"', '\"');
        safe_country := replace(replace(rec.country_code, '\', '\\'), '"', '\"');

        -- Create Region node
        v_cypher := 'MERGE (:Region {code: "' || safe_code
                 || '", name: "' || safe_name
                 || '", country_code: "' || safe_country || '"})';
        v_sql    := 'SELECT * FROM cypher(' || quote_literal(v_graph)
                 || ', $$ ' || v_cypher || ' $$) AS (v agtype)';
        EXECUTE v_sql;

        -- Create PART_OF edge: Region -[:PART_OF]-> Country
        v_cypher := 'MATCH (r:Region {code: "' || safe_code || '"}), '
                 || '(c:Country {code: "' || safe_country || '"}) '
                 || 'MERGE (r)-[:PART_OF]->(c)';
        v_sql    := 'SELECT * FROM cypher(' || quote_literal(v_graph)
                 || ', $$ ' || v_cypher || ' $$) AS (v agtype)';
        EXECUTE v_sql;
    END LOOP;
END;
$$;

SELECT
    (SELECT count(*) FROM iso3166_countries) AS countries_loaded,
    (SELECT count(*) FROM iso3166_regions)   AS regions_loaded;
