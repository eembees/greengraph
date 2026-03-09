# Greengraph — Context Graph for RAG Retrieval

Self-hosted knowledge graph engine using **PostgreSQL + pgvector + Apache AGE** for multi-hop contextual retrieval in LLM applications.

## Quick start

```bash
# 1. Install Python dependencies
make install

# 2. Copy and edit the environment file
make env
# edit .env — set POSTGRES_PASSWORD, ANTHROPIC_API_KEY, OPENAI_API_KEY as needed

# 3. Deploy the database
make deploy

# 4. Verify everything is running
make db-status

# 5. Ingest a document
greengraph ingest path/to/document.md --title "My Doc"

# 6. Retrieve relevant context
greengraph retrieve "What is the main topic?"
```

## Available make targets

| Target | Description |
|---|---|
| `make install` | Install Python deps into `.venv` |
| `make run` | Start DB and show status |
| `make deploy` | Build image + start services |
| `make test` | Run unit tests |
| `make test-all` | Run all tests (needs running DB) |
| `make lint` | Ruff lint check |
| `make format` | Ruff formatter |
| `make typecheck` | ty type check |
| `make check` | All quality checks |
| `make db-shell` | psql shell |
| `make backup` | Dump database |
| `make down` | Stop services |

## Architecture

See the spec for full architecture details. Key components:

- **PostgreSQL 16** — relational backbone
- **pgvector 0.8** — HNSW vector similarity search (1536-dim embeddings)
- **Apache AGE 1.5** — openCypher graph queries for multi-hop traversal
- **Python CLI** — ingestion pipeline + retrieval orchestration

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_PASSWORD` | `changeme_in_production` | Database password |
| `EMBEDDING_PROVIDER` | `mock` | `openai` or `mock` |
| `OPENAI_API_KEY` | — | Required for OpenAI embeddings |
| `ANTHROPIC_API_KEY` | — | Required for LLM entity extraction |
| `CHUNK_SIZE` | `512` | Chunk size in characters |
| `CHUNK_OVERLAP` | `50` | Overlap between chunks |
