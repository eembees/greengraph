# GreenGraph

A graph editor with a FastAPI backend, LiteGraph.js frontend, and SQLite database.

## Stack

| Layer    | Technology               |
|----------|--------------------------|
| Backend  | FastAPI + SQLModel       |
| Database | SQLite (via SQLModel)    |
| Frontend | LiteGraph.js (CDN)       |

## Getting started

```bash
pip install -e ".[dev]"
greengraph          # starts on http://localhost:8000
```

Or with uvicorn directly:

```bash
uvicorn greengraph.main:app --reload
```

## API

| Method | Path                  | Description         |
|--------|-----------------------|---------------------|
| GET    | `/api/graphs/`        | List all graphs     |
| POST   | `/api/graphs/`        | Create a new graph  |
| GET    | `/api/graphs/{id}`    | Get a graph         |
| PUT    | `/api/graphs/{id}`    | Update a graph      |
| DELETE | `/api/graphs/{id}`    | Delete a graph      |

Interactive docs at `/docs`.

## Development

```bash
pytest          # run tests
ruff check .    # lint
ruff format .   # format
```
