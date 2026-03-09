# GreenGraph

A graph editor with a FastAPI backend, LiteGraph.js frontend, and **LiteGraph DB** as the graph database.

## Stack

| Layer    | Technology                          |
|----------|-------------------------------------|
| Backend  | FastAPI                             |
| Database | [LiteGraph DB](https://github.com/litegraphdb/litegraph) via `litegraph_sdk` |
| Frontend | LiteGraph.js (CDN)                  |

## Prerequisites

Start a LiteGraph DB server (Docker):

```bash
docker run -d -p 8701:8701 litegraphdb/litegraph
```

Environment variables (all optional, shown with defaults):

```bash
LITEGRAPH_ENDPOINT=http://localhost:8701
LITEGRAPH_TENANT_GUID=00000000-0000-0000-0000-000000000000
LITEGRAPH_ACCESS_KEY=litegraphadmin
```

## Getting started

```bash
pip install -e ".[dev]"
greengraph          # starts on http://localhost:8000
```

Or with uvicorn directly:

```bash
uvicorn greengraph.main:app --reload
```

## How data is stored

Each canvas in the editor maps 1:1 to a **Graph** in LiteGraph DB.
Each LiteGraph.js node → a **Node** in the graph (full node JSON stored in `data`).
Each LiteGraph.js link → an **Edge** in the graph (slot info stored in `data`).

Saving reconstructs the full topology from the canvas and replaces the DB state atomically (delete-all + recreate).

## API

| Method | Path                   | Description                              |
|--------|------------------------|------------------------------------------|
| GET    | `/api/graphs/`         | List all graphs                          |
| POST   | `/api/graphs/`         | Create a new graph                       |
| GET    | `/api/graphs/{guid}`   | Get graph + full LiteGraph.js JSON       |
| PUT    | `/api/graphs/{guid}`   | Sync canvas state → DB nodes/edges       |
| PATCH  | `/api/graphs/{guid}`   | Update name/description only             |
| DELETE | `/api/graphs/{guid}`   | Delete graph and all its nodes/edges     |

Interactive docs at `/docs`.

## Development

```bash
pytest          # run tests (SDK is mocked, no server needed)
ruff check .
ruff format .
```
