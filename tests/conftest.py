"""Pytest fixtures shared across all tests."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from greengraph.embeddings import MockEmbeddingBackend
from greengraph.models import DocumentCreate

# ---------------------------------------------------------------------------
# Embedding fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_backend() -> MockEmbeddingBackend:
    """A deterministic mock embedding backend (no external calls)."""
    return MockEmbeddingBackend(dim=1536)


@pytest.fixture
def sample_doc() -> DocumentCreate:
    """A simple DocumentCreate for testing."""
    return DocumentCreate(
        title="Test Document",
        content=(
            "Alice works at Acme Corp. She manages the engineering team. "
            "Bob is the CTO of Acme Corp. "
            "The engineering team uses Python and PostgreSQL for their projects. "
            "Alice and Bob presented at PyCon 2025 in Berlin. "
            "Their talk covered distributed systems and graph databases."
        ),
        source_url="https://example.com/test",
        metadata={"category": "tech", "year": "2025"},
    )


@pytest.fixture
def long_doc() -> DocumentCreate:
    """A document long enough to produce multiple chunks."""
    paragraphs = [f"This is paragraph {i}. " * 20 for i in range(20)]
    return DocumentCreate(
        title="Long Document",
        content="\n\n".join(paragraphs),
        metadata={"category": "test"},
    )


# ---------------------------------------------------------------------------
# DB mock fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_conn() -> MagicMock:
    """A MagicMock that mimics a psycopg connection."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    conn.execute.return_value = cursor
    return conn


@pytest.fixture
def mock_get_conn(mock_conn: MagicMock) -> Generator[MagicMock, None, None]:
    """Patch greengraph.db.get_conn to return mock_conn."""
    from contextlib import contextmanager

    @contextmanager
    def _ctx() -> Generator[MagicMock, None, None]:
        yield mock_conn

    with patch("greengraph.db.get_conn", side_effect=_ctx) as p:
        yield p
