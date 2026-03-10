"""Command-line interface for greengraph."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from greengraph.config import settings

app = typer.Typer(
    name="greengraph",
    help="Context Graph for RAG Retrieval — PostgreSQL + pgvector + Apache AGE",
    no_args_is_help=True,
)
console = Console()
err_console = Console(stderr=True)

db_app = typer.Typer(help="Database management commands")
app.add_typer(db_app, name="db")

graph_app = typer.Typer(help="Graph inspection and export commands")
app.add_typer(graph_app, name="graph")

entity_app = typer.Typer(help="Entity management commands")
app.add_typer(entity_app, name="entities")

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


# ---------------------------------------------------------------------------
# DB sub-commands
# ---------------------------------------------------------------------------


@db_app.command("status")
def db_status() -> None:
    """Check database connectivity and extension status."""
    from greengraph.db import fetchall, fetchone

    try:
        row = fetchone("SELECT version() AS v")
        assert row is not None
        console.print(Panel(f"[green]Connected[/green]\n{row['v']}", title="PostgreSQL"))

        exts = fetchall(
            "SELECT name, default_version FROM pg_available_extensions WHERE installed_version IS NOT NULL ORDER BY name"  # noqa: E501
        )
        t = Table("Extension", "Version", title="Installed Extensions")
        for ext in exts:
            t.add_row(ext["name"], ext.get("default_version") or "")
        console.print(t)

        graphs = fetchall("SELECT name FROM ag_catalog.ag_graph")
        if graphs:
            console.print(f"[green]AGE graph(s):[/green] {', '.join(r['name'] for r in graphs)}")
        else:
            console.print("[yellow]No AGE graphs found[/yellow]")

    except Exception as exc:
        err_console.print(f"[red]Connection failed:[/red] {exc}")
        raise typer.Exit(1) from exc


@db_app.command("init")
def db_init(
    force: Annotated[bool, typer.Option("--force", help="Drop and recreate schema")] = False,
) -> None:
    """Apply the initialization SQL schema to the connected database.

    Normally you should let Docker run init/01-init-extensions.sql automatically.
    Use this command when connecting to an existing PostgreSQL instance.
    """
    init_sql = Path(__file__).parent.parent.parent / "init" / "01-init-extensions.sql"
    if not init_sql.exists():
        err_console.print(f"[red]Init SQL not found:[/red] {init_sql}")
        raise typer.Exit(1)

    from greengraph.db import execute

    if force:
        console.print("[yellow]Dropping existing schema...[/yellow]")
        execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")

    sql = init_sql.read_text()
    try:
        execute(sql)
        console.print("[green]Database initialized successfully.[/green]")
    except Exception as exc:
        err_console.print(f"[red]Init failed:[/red] {exc}")
        raise typer.Exit(1) from exc


# ---------------------------------------------------------------------------
# Ingest command
# ---------------------------------------------------------------------------


@app.command()
def ingest(
    path: Annotated[Path, typer.Argument(help="Path to a text, markdown, or PDF file to ingest")],
    title: Annotated[str | None, typer.Option("--title", "-t", help="Document title")] = None,
    source_url: Annotated[str | None, typer.Option("--url", "-u", help="Source URL")] = None,
    skip_graph: Annotated[bool, typer.Option("--skip-graph", help="Skip AGE graph sync")] = False,
    skip_entities: Annotated[
        bool, typer.Option("--skip-entities", help="Skip entity extraction")
    ] = False,
    skip_geo: Annotated[
        bool, typer.Option("--skip-geo", help="Skip ISO 3166 geo-tagging")
    ] = False,
    metadata: Annotated[
        str | None,
        typer.Option("--metadata", "-m", help='JSON metadata string, e.g. \'{"category":"tech"}\''),
    ] = None,
) -> None:
    """Ingest a document into the context graph."""
    if not path.exists():
        err_console.print(f"[red]File not found:[/red] {path}")
        raise typer.Exit(1)

    import json

    from greengraph.ingest import ingest_document
    from greengraph.models import DocumentCreate

    content = path.read_text(encoding="utf-8", errors="replace")
    meta: dict[str, Any] = {}
    if metadata:
        try:
            meta = json.loads(metadata)
        except json.JSONDecodeError as exc:
            err_console.print(f"[red]Invalid JSON metadata:[/red] {exc}")
            raise typer.Exit(1) from exc

    doc = DocumentCreate(
        title=title or path.stem,
        content=content,
        source_url=source_url or str(path.resolve()),
        metadata=meta,
    )

    with console.status(f"Ingesting [bold]{doc.title}[/bold]..."):
        result = ingest_document(
            doc, skip_graph=skip_graph, skip_entities=skip_entities, skip_geo=skip_geo
        )

    if result.skipped:
        console.print(f"[yellow]Skipped:[/yellow] {result.skip_reason}")
    else:
        console.print(
            Panel(
                f"Document ID:  {result.document_id}\n"
                f"Chunks:       {result.chunks_created}\n"
                f"Entities:     {result.entities_extracted}\n"
                f"Relationships:{result.relationships_created}",
                title=f"[green]Ingested: {result.title}[/green]",
            )
        )


# ---------------------------------------------------------------------------
# Retrieve command
# ---------------------------------------------------------------------------


@app.command()
def retrieve(
    query: Annotated[str, typer.Argument(help="Natural-language query")],
    top_k: Annotated[int, typer.Option("--top-k", "-k")] = 5,
    threshold: Annotated[float, typer.Option("--threshold")] = 0.7,
    no_graph: Annotated[
        bool, typer.Option("--no-graph", help="Skip graph context expansion")
    ] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Output raw JSON")] = False,
) -> None:
    """Retrieve relevant chunks for a query using vector + graph search."""
    from greengraph.retrieve import retrieve as do_retrieve

    with console.status("Retrieving..."):
        result = do_retrieve(
            query,
            top_k=top_k,
            similarity_threshold=threshold,
            include_graph_context=not no_graph,
        )

    if json_output:
        import json

        console.print(json.dumps(result.model_dump(), indent=2))
        return

    if not result.chunks:
        console.print("[yellow]No results found above the similarity threshold.[/yellow]")
        return

    console.print(Panel(f"[bold]{query}[/bold]", title="Query"))
    for i, chunk in enumerate(result.chunks, 1):
        graph_info = ""
        if chunk.graph_context:
            lines = [
                f"  {c.entity} --[{c.relationship}]--> {c.connected_entity}"
                for c in chunk.graph_context[:3]
            ]
            graph_info = "\n[dim]Graph context:[/dim]\n" + "\n".join(lines)
        console.print(
            Panel(
                f"{chunk.content[:500]}{'...' if len(chunk.content) > 500 else ''}{graph_info}",
                title=f"[cyan]#{i}[/cyan] chunk_id={chunk.chunk_id} sim={chunk.similarity:.3f}",
            )
        )


# ---------------------------------------------------------------------------
# Graph sub-commands
# ---------------------------------------------------------------------------


@graph_app.command("sync-edges")
def graph_sync_edges(
    doc_id: Annotated[
        int | None,
        typer.Option("--doc-id", "-d", help="Only sync this document ID. Omit for all documents."),
    ] = None,
) -> None:
    """Sync derived graph edges for all documents (or a single one).

    Creates / refreshes:
      Document -[CONTAINS]-> Chunk
      Document -[MENTIONS]-> Entity
      Entity   -[MENTIONED_TOGETHER_WITH]-> Entity

    All edges are MERGE'd so the command is safe to re-run.
    """
    from greengraph.db import fetchone

    with console.status("Syncing graph edges..."):
        try:
            row = fetchone("SELECT public.sync_graph_edges(%s) AS result", (doc_id,))
        except Exception as exc:
            err_console.print(f"[red]sync_graph_edges failed:[/red] {exc}")
            raise typer.Exit(1) from exc

    result = row["result"] if row else {}
    console.print(
        Panel(
            f"Docs processed:  {result.get('docs_processed', '?')}\n"
            f"CONTAINS edges:  {result.get('contains_edges', '?')}",
            title="[green]Graph edges synced[/green]",
        )
    )


@graph_app.command("export")
def graph_export(
    output: Annotated[Path, typer.Argument(help="Output GEXF file (default: context_graph.gexf)")]
    = Path("context_graph.gexf"),
    graph_name: Annotated[
        str, typer.Option("--graph", "-g", help="AGE graph name")
    ] = settings.graph_name,
    limit: Annotated[
        int, typer.Option("--limit", help="Max nodes/edges to fetch (0 = all)")
    ] = 0,
) -> None:
    """Export the AGE graph to GEXF format for visualisation in Gephi.

    Example:
        greengraph graph export my_graph.gexf
        gephi my_graph.gexf
    """
    import json

    try:
        import networkx as nx
    except ImportError:
        err_console.print("[red]networkx is required:[/red] pip install networkx")
        raise typer.Exit(1)

    from greengraph.db import get_conn, load_age

    def parse_agtype(raw: Any) -> dict[str, Any]:
        """Strip AGE type annotation (::vertex / ::edge) and parse JSON."""
        s = str(raw)
        if "::" in s:
            s = s[: s.rfind("::")]
        return json.loads(s)

    def node_key(node: dict[str, Any]) -> str:
        """Stable, unique, human-readable node key: '<Label>_<relational_id>'.

        Using AGE internal IDs as networkx keys risks silent mismatches between
        the node query and edge start_id/end_id values. Compound string keys
        derived from the relational id are unique across labels and survive
        round-trips through GEXF without integer-overflow issues in Gephi.
        """
        kind = node.get("label", "Unknown")
        props = node.get("properties", {})
        return f"{kind}_{props.get('id', node['id'])}"

    def add_node_to_graph(G: nx.DiGraph, node: dict[str, Any]) -> None:  # noqa: N803
        kind = node.get("label", "Unknown")
        props = node.get("properties", {})
        display = props.get("name") or props.get("title") or str(props.get("id", node["id"]))
        G.add_node(node_key(node), label=display, kind=kind, **{k: str(v) for k, v in props.items() if k != "id"})

    limit_clause = f" LIMIT {limit}" if limit else ""
    G: nx.DiGraph = nx.DiGraph()

    with console.status("Fetching graph from AGE..."):
        with get_conn() as conn:
            load_age(conn)

            # Single query returns nodes and edges together so IDs are guaranteed
            # consistent — no risk of start_id/end_id not matching node query IDs.
            edge_rows = conn.execute(
                f"SELECT * FROM cypher('{graph_name}', $$"
                f" MATCH (a)-[r]->(b) RETURN a, r, b{limit_clause}"
                f" $$) AS (a agtype, r agtype, b agtype)"
            ).fetchall()
            for row in edge_rows:
                a = parse_agtype(row["a"])
                r = parse_agtype(row["r"])
                b = parse_agtype(row["b"])
                add_node_to_graph(G, a)
                add_node_to_graph(G, b)
                G.add_edge(node_key(a), node_key(b), label=r.get("label", ""))

            # Isolated nodes (not connected to any edge) need a separate pass.
            all_node_rows = conn.execute(
                f"SELECT * FROM cypher('{graph_name}', $$"
                f" MATCH (n) RETURN n{limit_clause}"
                f" $$) AS (n agtype)"
            ).fetchall()
            for row in all_node_rows:
                node = parse_agtype(row["n"])
                add_node_to_graph(G, node)  # no-op if already added

    nx.write_gexf(G, output)

    console.print(
        Panel(
            f"Nodes:  {G.number_of_nodes()}\n"
            f"Edges:  {G.number_of_edges()}\n"
            f"File:   {output.resolve()}",
            title="[green]Graph exported[/green]",
        )
    )
    console.print(f"\n[dim]Open with:[/dim]  gephi {output}")


# ---------------------------------------------------------------------------
# Config command
# ---------------------------------------------------------------------------


@app.command()
def config() -> None:
    """Show the current runtime configuration."""
    t = Table("Setting", "Value", title="Configuration")
    for field_name in settings.model_fields:
        value = getattr(settings, field_name)
        # Redact secrets
        if any(secret in field_name for secret in ("password", "api_key", "secret")):
            display = "***" if value else "(not set)"
        else:
            display = str(value)
        t.add_row(field_name, display)
    console.print(t)


# ---------------------------------------------------------------------------
# Entity sub-commands
# ---------------------------------------------------------------------------


@entity_app.command("dedup")
def entity_dedup(
    strategy: Annotated[
        str,
        typer.Option(
            "--strategy",
            "-s",
            help="Deduplication strategy: 'string' (default) or 'embedding' (not yet implemented).",
        ),
    ] = "string",
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Report duplicates without making changes."),
    ] = False,
) -> None:
    """Deduplicate entities in the graph database.

    String strategy: merges entities with the same name (case-insensitive,
    trimmed) and type.  The entity with the lowest id is kept as canonical;
    duplicates are removed and their graph edges are redirected.
    """
    from greengraph.dedup import dedup_entities

    label = "[dim](dry run)[/dim] " if dry_run else ""
    with console.status(f"{label}Deduplicating entities ({strategy})..."):
        try:
            result = dedup_entities(strategy=strategy, dry_run=dry_run)
        except NotImplementedError as exc:
            err_console.print(f"[yellow]Not implemented:[/yellow] {exc}")
            raise typer.Exit(1) from exc
        except Exception as exc:
            err_console.print(f"[red]Deduplication failed:[/red] {exc}")
            raise typer.Exit(1) from exc

    title = "[green]Deduplication complete[/green]"
    if result["dry_run"]:
        title = "[yellow]Dry run — no changes made[/yellow]"

    console.print(
        Panel(
            f"Duplicate groups found: {result['groups_found']}\n"
            f"Entities merged:        {result['entities_merged']}",
            title=title,
        )
    )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
