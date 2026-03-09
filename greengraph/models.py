from typing import Any

from pydantic import BaseModel


class GraphCreate(BaseModel):
    name: str
    description: str = ""


class GraphRead(BaseModel):
    guid: str
    name: str
    description: str


class GraphWithData(GraphRead):
    """Graph metadata + LiteGraph.js-compatible graph_data for the editor."""

    graph_data: dict[str, Any]


class GraphSync(BaseModel):
    """Payload for saving the full canvas state into the graph DB."""

    graph_data: dict[str, Any]  # LiteGraph.js serialized graph (nodes + links)
