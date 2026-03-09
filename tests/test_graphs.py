"""Tests for the graph API routes, with litegraph_sdk fully mocked."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from greengraph.main import app

client = TestClient(app)

# ── fixtures / helpers ────────────────────────────────────────────────────────

GRAPH_GUID = "aaaaaaaa-0000-0000-0000-000000000001"
NODE_GUID_1 = "bbbbbbbb-0000-0000-0000-000000000001"
NODE_GUID_2 = "bbbbbbbb-0000-0000-0000-000000000002"
EDGE_GUID_1 = "cccccccc-0000-0000-0000-000000000001"


def _make_graph(guid=GRAPH_GUID, name="Test", description="desc"):
    return SimpleNamespace(
        guid=guid,
        name=name,
        data={"description": description},
    )


def _make_node(guid, lg_id, node_type="basic/const", pos=None):
    return SimpleNamespace(
        guid=guid,
        name=node_type,
        data={"id": lg_id, "type": node_type, "pos": pos or [100, 100]},
    )


def _make_edge(guid, from_guid, to_guid, link_id=1):
    return SimpleNamespace(
        guid=guid,
        from_node_guid=from_guid,
        to_node_guid=to_guid,
        data={"link_id": link_id, "from_slot": 0, "to_slot": 0, "link_type": "number"},
    )


# ── list graphs ───────────────────────────────────────────────────────────────


@patch("greengraph.routes.graphs.Graph")
@patch("greengraph.routes.graphs.configure_global")
def test_list_graphs_empty(mock_cfg, MockGraph):
    MockGraph.retrieve_all.return_value = []
    res = client.get("/api/graphs/")
    assert res.status_code == 200
    assert res.json() == []


@patch("greengraph.routes.graphs.Graph")
@patch("greengraph.routes.graphs.configure_global")
def test_list_graphs(mock_cfg, MockGraph):
    MockGraph.retrieve_all.return_value = [_make_graph()]
    res = client.get("/api/graphs/")
    assert res.status_code == 200
    data = res.json()
    assert len(data) == 1
    assert data[0]["guid"] == GRAPH_GUID
    assert data[0]["name"] == "Test"


# ── create graph ──────────────────────────────────────────────────────────────


@patch("greengraph.routes.graphs.Graph")
@patch("greengraph.routes.graphs.configure_global")
def test_create_graph(mock_cfg, MockGraph):
    MockGraph.create.return_value = _make_graph(name="New")
    res = client.post("/api/graphs/", json={"name": "New", "description": "hello"})
    assert res.status_code == 201
    assert res.json()["name"] == "New"
    MockGraph.create.assert_called_once_with(name="New", data={"description": "hello"})


# ── get graph ─────────────────────────────────────────────────────────────────


@patch("greengraph.routes.graphs.Edge")
@patch("greengraph.routes.graphs.Node")
@patch("greengraph.routes.graphs.Graph")
@patch("greengraph.routes.graphs.graph_ctx")
def test_get_graph_empty(mock_ctx, MockGraph, MockNode, MockEdge):
    mock_ctx.return_value.__enter__ = lambda s: None
    mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
    MockGraph.retrieve.return_value = _make_graph()
    MockNode.retrieve_all.return_value = []
    MockEdge.retrieve_all.return_value = []

    res = client.get(f"/api/graphs/{GRAPH_GUID}")
    assert res.status_code == 200
    body = res.json()
    assert body["guid"] == GRAPH_GUID
    assert body["graph_data"]["nodes"] == []
    assert body["graph_data"]["links"] == []


@patch("greengraph.routes.graphs.Edge")
@patch("greengraph.routes.graphs.Node")
@patch("greengraph.routes.graphs.Graph")
@patch("greengraph.routes.graphs.graph_ctx")
def test_get_graph_with_topology(mock_ctx, MockGraph, MockNode, MockEdge):
    mock_ctx.return_value.__enter__ = lambda s: None
    mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
    MockGraph.retrieve.return_value = _make_graph()
    MockNode.retrieve_all.return_value = [
        _make_node(NODE_GUID_1, lg_id=1),
        _make_node(NODE_GUID_2, lg_id=2, node_type="basic/watch"),
    ]
    MockEdge.retrieve_all.return_value = [
        _make_edge(EDGE_GUID_1, NODE_GUID_1, NODE_GUID_2),
    ]

    res = client.get(f"/api/graphs/{GRAPH_GUID}")
    assert res.status_code == 200
    body = res.json()
    assert len(body["graph_data"]["nodes"]) == 2
    assert len(body["graph_data"]["links"]) == 1
    link = body["graph_data"]["links"][0]
    assert link[1] == 1   # from lg_id
    assert link[3] == 2   # to lg_id


# ── sync graph ────────────────────────────────────────────────────────────────


@patch("greengraph.routes.graphs.Edge")
@patch("greengraph.routes.graphs.Node")
@patch("greengraph.routes.graphs.Graph")
@patch("greengraph.routes.graphs.graph_ctx")
def test_sync_graph(mock_ctx, MockGraph, MockNode, MockEdge):
    mock_ctx.return_value.__enter__ = lambda s: None
    mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
    MockGraph.retrieve.return_value = _make_graph()

    created_node = _make_node(NODE_GUID_1, lg_id=1)
    MockNode.create.return_value = created_node
    MockNode.retrieve_all.return_value = [created_node]
    MockEdge.retrieve_all.return_value = []

    payload = {
        "graph_data": {
            "last_node_id": 1,
            "last_link_id": 0,
            "nodes": [{"id": 1, "type": "basic/const", "pos": [50, 50]}],
            "links": [],
        }
    }
    res = client.put(f"/api/graphs/{GRAPH_GUID}", json=payload)
    assert res.status_code == 200
    MockNode.delete_all.assert_called_once()
    MockEdge.delete_all.assert_called_once()
    MockNode.create.assert_called_once()


# ── delete graph ──────────────────────────────────────────────────────────────


@patch("greengraph.routes.graphs.Graph")
@patch("greengraph.routes.graphs.configure_global")
def test_delete_graph(mock_cfg, MockGraph):
    res = client.delete(f"/api/graphs/{GRAPH_GUID}")
    assert res.status_code == 204
    MockGraph.delete.assert_called_once_with(resource_guid=GRAPH_GUID)


@patch("greengraph.routes.graphs.Graph")
@patch("greengraph.routes.graphs.configure_global")
def test_delete_graph_not_found(mock_cfg, MockGraph):
    MockGraph.delete.side_effect = Exception("not found")
    res = client.delete(f"/api/graphs/{GRAPH_GUID}")
    assert res.status_code == 404
