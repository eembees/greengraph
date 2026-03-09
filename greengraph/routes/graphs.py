from typing import Any

from fastapi import APIRouter, HTTPException
from litegraph_sdk import Edge, Graph, Node

from greengraph.config import configure_global, graph_ctx
from greengraph.models import GraphCreate, GraphRead, GraphSync, GraphWithData

router = APIRouter(prefix="/api/graphs", tags=["graphs"])


# ── helpers ──────────────────────────────────────────────────────────────────


def _graph_read(g) -> GraphRead:
    meta = g.data or {}
    return GraphRead(
        guid=g.guid,
        name=g.name,
        description=meta.get("description", ""),
    )


def _reconstruct_litegraph(nodes, edges) -> dict[str, Any]:
    """Turn DB nodes + edges back into LiteGraph.js serialization format."""
    guid_to_lg_id: dict[str, int] = {}
    lg_nodes = []

    for node in nodes:
        node_data: dict = node.data or {}
        lg_id: int = node_data.get("id", len(lg_nodes) + 1)
        guid_to_lg_id[node.guid] = lg_id
        lg_nodes.append(node_data)

    lg_links = []
    for edge in edges:
        from_guid = getattr(edge, "from_node_guid", None) or getattr(edge, "from_node", None)
        to_guid = getattr(edge, "to_node_guid", None) or getattr(edge, "to_node", None)
        from_lg = guid_to_lg_id.get(from_guid)
        to_lg = guid_to_lg_id.get(to_guid)
        if from_lg is None or to_lg is None:
            continue
        ed: dict = edge.data or {}
        lg_links.append([
            ed.get("link_id", len(lg_links) + 1),
            from_lg,
            ed.get("from_slot", 0),
            to_lg,
            ed.get("to_slot", 0),
            ed.get("link_type", ""),
        ])

    last_node_id = max((n.get("id", 0) for n in lg_nodes), default=0)
    last_link_id = max((lnk[0] for lnk in lg_links), default=0)

    return {
        "last_node_id": last_node_id,
        "last_link_id": last_link_id,
        "nodes": lg_nodes,
        "links": lg_links,
    }


# ── routes ───────────────────────────────────────────────────────────────────


@router.get("/", response_model=list[GraphRead])
def list_graphs():
    configure_global()
    try:
        graphs = Graph.retrieve_all()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"LiteGraph error: {exc}") from exc
    return [_graph_read(g) for g in (graphs or [])]


@router.post("/", response_model=GraphRead, status_code=201)
def create_graph(body: GraphCreate):
    configure_global()
    try:
        g = Graph.create(name=body.name, data={"description": body.description})
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"LiteGraph error: {exc}") from exc
    return _graph_read(g)


@router.get("/{graph_guid}", response_model=GraphWithData)
def get_graph(graph_guid: str):
    with graph_ctx(graph_guid):
        try:
            g = Graph.retrieve(resource_guid=graph_guid)
        except Exception as exc:
            raise HTTPException(status_code=404, detail="Graph not found") from exc

        nodes = Node.retrieve_all() or []
        edges = Edge.retrieve_all() or []

    graph_data = _reconstruct_litegraph(nodes, edges)
    meta = g.data or {}
    return GraphWithData(
        guid=g.guid,
        name=g.name,
        description=meta.get("description", ""),
        graph_data=graph_data,
    )


@router.put("/{graph_guid}", response_model=GraphWithData)
def sync_graph(graph_guid: str, body: GraphSync):
    """Full save: replace all nodes/edges with the LiteGraph.js canvas state."""
    with graph_ctx(graph_guid):
        try:
            g = Graph.retrieve(resource_guid=graph_guid)
        except Exception as exc:
            raise HTTPException(status_code=404, detail="Graph not found") from exc

        # Clear existing topology
        Node.delete_all()
        Edge.delete_all()

        lg_nodes: list[dict] = body.graph_data.get("nodes", [])
        lg_links: list = body.graph_data.get("links", [])

        # Create nodes — store full LiteGraph.js node dict as `data`
        lg_id_to_guid: dict[int, str] = {}
        for node in lg_nodes:
            created = Node.create(
                name=node.get("type", "node"),
                data=node,
            )
            lg_id_to_guid[node["id"]] = created.guid

        # Create edges — LiteGraph.js link: [link_id, from_id, from_slot, to_id, to_slot, type]
        for link in lg_links:
            link_id, from_lg_id, from_slot, to_lg_id, to_slot, *rest = link
            from_guid = lg_id_to_guid.get(from_lg_id)
            to_guid = lg_id_to_guid.get(to_lg_id)
            if not from_guid or not to_guid:
                continue
            Edge.create(
                from_node=from_guid,
                to_node=to_guid,
                name=f"link_{link_id}",
                cost=1,
                data={
                    "link_id": link_id,
                    "from_slot": from_slot,
                    "to_slot": to_slot,
                    "link_type": rest[0] if rest else "",
                },
            )

        # Return reconstructed state (guids may differ from original lg ids)
        nodes_out = Node.retrieve_all() or []
        edges_out = Edge.retrieve_all() or []

    meta = g.data or {}
    return GraphWithData(
        guid=g.guid,
        name=g.name,
        description=meta.get("description", ""),
        graph_data=_reconstruct_litegraph(nodes_out, edges_out),
    )


@router.patch("/{graph_guid}", response_model=GraphRead)
def update_graph_meta(graph_guid: str, body: GraphCreate):
    """Update name/description without touching nodes/edges."""
    configure_global()
    try:
        g = Graph.update(
            resource_guid=graph_guid,
            name=body.name,
            data={"description": body.description},
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Graph not found") from exc
    return _graph_read(g)


@router.delete("/{graph_guid}", status_code=204)
def delete_graph(graph_guid: str):
    configure_global()
    try:
        Graph.delete(resource_guid=graph_guid)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Graph not found") from exc
