"""Database connection and query helpers."""

from __future__ import annotations

import contextlib
from collections.abc import Generator
from typing import Any

import psycopg
from psycopg.rows import dict_row

from greengraph.config import settings


def get_connection_string() -> str:
    return settings.database_url


@contextlib.contextmanager
def get_conn() -> Generator[psycopg.Connection[dict[str, Any]], None, None]:
    """Yield a psycopg connection with dict row factory and auto-commit off."""
    conn = psycopg.connect(get_connection_string(), row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> None:
    """Execute a statement that returns no rows."""
    with get_conn() as conn:
        conn.execute(sql, params)


def fetchall(
    sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Execute a query and return all rows as dicts."""
    with get_conn() as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall()


def fetchone(
    sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """Execute a query and return a single row as a dict, or None."""
    with get_conn() as conn:
        cur = conn.execute(sql, params)
        return cur.fetchone()


def configure_hnsw_ef_search(conn: psycopg.Connection[Any], ef_search: int) -> None:
    """Set per-session HNSW ef_search for better recall."""
    conn.execute(f"SET hnsw.ef_search = {ef_search}")


def load_age(conn: psycopg.Connection[Any]) -> None:
    """Load the AGE extension and set search path for Cypher queries."""
    conn.execute("LOAD 'age'")
    conn.execute('SET search_path = ag_catalog, "$user", public')


# ---------------------------------------------------------------------------
# AGE / Graph helpers
# ---------------------------------------------------------------------------


def cypher_query(
    conn: psycopg.Connection[Any],
    graph_name: str,
    cypher: str,
    columns: list[tuple[str, str]],
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Execute a Cypher query via AGE and return results as dicts.

    Args:
        conn: Active psycopg connection (AGE must already be loaded).
        graph_name: Name of the AGE graph.
        cypher: The Cypher query string. Use $param_name for parameters.
        columns: List of (column_name, pg_type) pairs matching the RETURN clause.
        params: Optional dict of parameters to substitute into the query.

    Returns:
        List of result rows as dicts.
    """
    # AGE parameters are embedded via string substitution (AGE doesn't support
    # parameterized Cypher natively), so we JSON-encode values.
    import json

    if params:
        for key, value in params.items():
            placeholder = f"${key}"
            if isinstance(value, str):
                cypher = cypher.replace(placeholder, f'"{value}"')
            else:
                cypher = cypher.replace(placeholder, json.dumps(value))

    col_defs = ", ".join(f"{name} {pg_type}" for name, pg_type in columns)
    sql = f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS ({col_defs})"

    cur = conn.execute(sql)
    rows = cur.fetchall()
    return rows


def create_graph_node(
    conn: psycopg.Connection[Any],
    graph_name: str,
    label: str,
    properties: dict[str, Any],
) -> None:
    """Create a node in the AGE graph."""
    load_age(conn)
    props_cypher = ", ".join(f"{k}: {_cypher_value(v)}" for k, v in properties.items())
    cypher = f"CREATE (:{label} {{{props_cypher}}})"
    # CREATE returns nothing useful; execute and ignore
    sql = f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS (v agtype)"
    conn.execute(sql)


def create_graph_edge(
    conn: psycopg.Connection[Any],
    graph_name: str,
    from_label: str,
    from_id: int,
    to_label: str,
    to_id: int,
    edge_label: str,
    properties: dict[str, Any] | None = None,
) -> None:
    """Create a directed edge between two nodes identified by their relational id."""
    load_age(conn)
    props_cypher = ""
    if properties:
        prop_pairs = ", ".join(f"{k}: {_cypher_value(v)}" for k, v in properties.items())
        props_cypher = f" {{{prop_pairs}}}"

    cypher = (
        f"MATCH (a:{from_label} {{id: {from_id}}}), (b:{to_label} {{id: {to_id}}}) "
        f"CREATE (a)-[:{edge_label}{props_cypher}]->(b)"
    )
    sql = f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS (v agtype)"
    conn.execute(sql)


def merge_graph_node(
    conn: psycopg.Connection[Any],
    graph_name: str,
    label: str,
    match_props: dict[str, Any],
    set_props: dict[str, Any] | None = None,
) -> None:
    """MERGE a node (create if not exists) and optionally set additional properties.

    AGE does not support ON CREATE SET inside MERGE, so extra properties are
    applied via a subsequent MATCH ... SET query.
    """
    load_age(conn)
    match_cypher = ", ".join(f"{k}: {_cypher_value(v)}" for k, v in match_props.items())

    # Step 1: MERGE on key properties only
    cypher = f"MERGE (n:{label} {{{match_cypher}}})"
    sql = f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS (v agtype)"  # noqa: S608
    conn.execute(sql)

    # Step 2: SET extra properties (AGE supports SET but not ON CREATE SET)
    if set_props:
        set_pairs = ", ".join(f"n.{k} = {_cypher_value(v)}" for k, v in set_props.items())
        cypher_set = f"MATCH (n:{label} {{{match_cypher}}}) SET {set_pairs}"
        sql_set = f"SELECT * FROM cypher('{graph_name}', $$ {cypher_set} $$) AS (v agtype)"  # noqa: S608
        conn.execute(sql_set)


def _cypher_value(v: Any) -> str:
    """Convert a Python value to its Cypher literal representation."""
    if isinstance(v, str):
        escaped = v.replace("'", "\\'").replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    return str(v)
