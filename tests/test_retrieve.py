"""Unit tests for the retrieval pipeline (database calls are mocked)."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

from greengraph.embeddings import MockEmbeddingBackend


def _make_vector_rows(n: int = 3, base_similarity: float = 0.9) -> list[dict[str, Any]]:
    return [
        {
            "chunk_id": 100 + i,
            "document_id": 1,
            "content": f"Content of chunk {i}.",
            "similarity": base_similarity - i * 0.05,
        }
        for i in range(n)
    ]


def make_retrieval_conn(
    *,
    vector_rows: list[dict[str, Any]] | None = None,
    graph_rows: list[dict[str, Any]] | None = None,
) -> MagicMock:
    conn = MagicMock()
    _vrows = _make_vector_rows() if vector_rows is None else vector_rows
    _grows = graph_rows if graph_rows is not None else []

    def _execute(sql: str, params: Any = None) -> MagicMock:
        cursor = MagicMock()
        sql_lower = sql.lower()

        if "from chunks" in sql_lower or ("embedding" in sql_lower and "select" in sql_lower):
            cursor.fetchall.return_value = _vrows
        elif "cypher" in sql_lower:
            cursor.fetchall.return_value = _grows
        else:
            cursor.fetchall.return_value = []
            cursor.fetchone.return_value = None

        return cursor

    conn.execute.side_effect = _execute
    return conn


@contextmanager
def _ctx(conn: MagicMock) -> Generator[MagicMock, None, None]:
    yield conn


class TestRetrieve:
    def test_returns_retrieval_result(self, mock_backend: MockEmbeddingBackend) -> None:
        mock_conn = make_retrieval_conn()

        with patch("greengraph.retrieve.get_conn", return_value=_ctx(mock_conn)):
            with patch("greengraph.retrieve.load_age"):
                from greengraph.retrieve import retrieve

                result = retrieve(
                    "test query",
                    embedding_backend=mock_backend,
                    include_graph_context=False,
                )

        assert result.query == "test query"
        assert len(result.chunks) > 0
        assert result.total_chunks == len(result.chunks)

    def test_chunks_have_correct_fields(self, mock_backend: MockEmbeddingBackend) -> None:
        rows = _make_vector_rows(2)
        mock_conn = make_retrieval_conn(vector_rows=rows)

        with patch("greengraph.retrieve.get_conn", return_value=_ctx(mock_conn)):
            from greengraph.retrieve import retrieve

            result = retrieve(
                "q",
                embedding_backend=mock_backend,
                include_graph_context=False,
            )

        assert len(result.chunks) == 2
        chunk = result.chunks[0]
        assert chunk.chunk_id == 100
        assert chunk.document_id == 1
        assert "Content of chunk 0" in chunk.content
        assert 0 < chunk.similarity <= 1.0

    def test_no_graph_context_when_disabled(self, mock_backend: MockEmbeddingBackend) -> None:
        mock_conn = make_retrieval_conn()

        with patch("greengraph.retrieve.get_conn", return_value=_ctx(mock_conn)):
            from greengraph.retrieve import retrieve

            result = retrieve(
                "q",
                embedding_backend=mock_backend,
                include_graph_context=False,
            )

        assert all(len(c.graph_context) == 0 for c in result.chunks)

    def test_empty_result_when_no_chunks_found(self, mock_backend: MockEmbeddingBackend) -> None:
        mock_conn = make_retrieval_conn(vector_rows=[])

        with patch("greengraph.retrieve.get_conn", return_value=_ctx(mock_conn)):
            from greengraph.retrieve import retrieve

            result = retrieve(
                "q",
                embedding_backend=mock_backend,
                include_graph_context=False,
            )

        assert result.chunks == []
        assert result.total_chunks == 0

    def test_graph_context_failure_is_graceful(self, mock_backend: MockEmbeddingBackend) -> None:
        """AGE not available — graph context should be empty, not raise."""
        mock_conn = make_retrieval_conn()

        def _execute(sql: str, params: Any = None) -> MagicMock:
            cursor = MagicMock()
            if "cypher" in sql.lower():
                raise Exception("AGE extension not available")
            cursor.fetchall.return_value = _make_vector_rows(2)
            return cursor

        mock_conn.execute.side_effect = _execute

        with patch("greengraph.retrieve.get_conn", return_value=_ctx(mock_conn)):
            with patch("greengraph.retrieve.load_age"):
                from greengraph.retrieve import retrieve

                result = retrieve(
                    "q",
                    embedding_backend=mock_backend,
                    include_graph_context=True,
                )

        # Should have chunks but no graph context
        assert len(result.chunks) > 0
        assert all(len(c.graph_context) == 0 for c in result.chunks)

    def test_context_text_is_non_empty_for_results(
        self, mock_backend: MockEmbeddingBackend
    ) -> None:
        mock_conn = make_retrieval_conn()

        with patch("greengraph.retrieve.get_conn", return_value=_ctx(mock_conn)):
            from greengraph.retrieve import retrieve

            result = retrieve(
                "test",
                embedding_backend=mock_backend,
                include_graph_context=False,
            )

        assert len(result.context_text) > 0


class TestParseAgtype:
    def test_strips_double_quotes(self) -> None:
        from greengraph.retrieve import _parse_agtype

        assert _parse_agtype('"hello"') == "hello"

    def test_passthrough_non_quoted(self) -> None:
        from greengraph.retrieve import _parse_agtype

        assert _parse_agtype("42") == "42"

    def test_none_returns_empty_string(self) -> None:
        from greengraph.retrieve import _parse_agtype

        assert _parse_agtype(None) == ""


class TestFormatVector:
    def test_basic(self) -> None:
        from greengraph.retrieve import _format_vector

        result = _format_vector([1.0, 2.0])
        assert result == "[1.0,2.0]"

    def test_empty(self) -> None:
        from greengraph.retrieve import _format_vector

        assert _format_vector([]) == "[]"
