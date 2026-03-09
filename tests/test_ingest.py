"""Unit tests for the ingestion pipeline (database calls are mocked)."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

from greengraph.embeddings import MockEmbeddingBackend
from greengraph.models import DocumentCreate


def make_mock_conn(
    *,
    existing_doc_id: int | None = None,
    new_doc_id: int = 42,
    chunk_ids: list[int] | None = None,
    entity_ids: list[int] | None = None,
) -> MagicMock:
    """Build a mock psycopg connection with configurable return values."""
    conn = MagicMock()
    call_count: dict[str, int] = {"execute": 0}

    def _execute(sql: str, params: Any = None) -> MagicMock:
        call_count["execute"] += 1
        cursor = MagicMock()

        sql_lower = sql.lower().strip()

        if "select id from documents where source_hash" in sql_lower:
            # Dedup check
            if existing_doc_id is not None:
                cursor.fetchone.return_value = {"id": existing_doc_id}
            else:
                cursor.fetchone.return_value = None

        elif "insert into documents" in sql_lower:
            cursor.fetchone.return_value = {"id": new_doc_id}

        elif "insert into chunks" in sql_lower:
            idx = call_count["execute"] - 1
            _ids = chunk_ids or [100, 101, 102, 103, 104]
            chunk_id = _ids[idx % len(_ids)]
            cursor.fetchone.return_value = {"id": chunk_id}

        elif "insert into entities" in sql_lower:
            idx = call_count["execute"] - 1
            _eids = entity_ids or [200, 201]
            entity_id = _eids[idx % len(_eids)]
            cursor.fetchone.return_value = {"id": entity_id}

        elif "select id from entities" in sql_lower:
            cursor.fetchone.return_value = {"id": 200}

        else:
            cursor.fetchone.return_value = None
            cursor.fetchall.return_value = []

        return cursor

    conn.execute.side_effect = _execute
    return conn


@contextmanager
def _mock_conn_ctx(mock_conn: MagicMock) -> Generator[MagicMock, None, None]:
    yield mock_conn


class TestIngestDocument:
    def test_new_document_creates_chunks(
        self,
        sample_doc: DocumentCreate,
        mock_backend: MockEmbeddingBackend,
    ) -> None:
        mock_conn = make_mock_conn(new_doc_id=1)

        with patch("greengraph.ingest.get_conn", return_value=_mock_conn_ctx(mock_conn)):
            from greengraph.ingest import ingest_document

            result = ingest_document(
                sample_doc,
                embedding_backend=mock_backend,
                skip_graph=True,
                skip_entities=True,
            )

        assert result.document_id == 1
        assert result.chunks_created > 0
        assert not result.skipped

    def test_duplicate_document_is_skipped(
        self,
        sample_doc: DocumentCreate,
        mock_backend: MockEmbeddingBackend,
    ) -> None:
        mock_conn = make_mock_conn(existing_doc_id=99)

        with patch("greengraph.ingest.get_conn", return_value=_mock_conn_ctx(mock_conn)):
            from greengraph.ingest import ingest_document

            result = ingest_document(
                sample_doc,
                embedding_backend=mock_backend,
                skip_graph=True,
                skip_entities=True,
            )

        assert result.skipped
        assert result.document_id == 99
        assert result.chunks_created == 0

    def test_long_document_creates_multiple_chunks(
        self,
        long_doc: DocumentCreate,
        mock_backend: MockEmbeddingBackend,
    ) -> None:
        mock_conn = make_mock_conn(new_doc_id=5, chunk_ids=list(range(100, 150)))

        with patch("greengraph.ingest.get_conn", return_value=_mock_conn_ctx(mock_conn)):
            from greengraph.ingest import ingest_document

            result = ingest_document(
                long_doc,
                embedding_backend=mock_backend,
                skip_graph=True,
                skip_entities=True,
            )

        assert result.chunks_created > 1

    def test_entity_extraction_skipped_when_flag_set(
        self,
        sample_doc: DocumentCreate,
        mock_backend: MockEmbeddingBackend,
    ) -> None:
        mock_conn = make_mock_conn(new_doc_id=2)

        with patch("greengraph.ingest.get_conn", return_value=_mock_conn_ctx(mock_conn)):
            with patch("greengraph.ingest._extract_entities", return_value=[]) as mock_extract:
                from greengraph.ingest import ingest_document

                result = ingest_document(
                    sample_doc,
                    embedding_backend=mock_backend,
                    skip_graph=True,
                    skip_entities=True,
                )

        mock_extract.assert_not_called()
        assert result.entities_extracted == 0

    def test_ingest_result_has_correct_title(
        self,
        sample_doc: DocumentCreate,
        mock_backend: MockEmbeddingBackend,
    ) -> None:
        mock_conn = make_mock_conn(new_doc_id=7)

        with patch("greengraph.ingest.get_conn", return_value=_mock_conn_ctx(mock_conn)):
            from greengraph.ingest import ingest_document

            result = ingest_document(
                sample_doc,
                embedding_backend=mock_backend,
                skip_graph=True,
                skip_entities=True,
            )

        assert result.title == sample_doc.title


class TestExtractEntities:
    def test_returns_empty_without_api_key(self) -> None:
        with patch("greengraph.ingest.settings") as mock_settings:
            mock_settings.openai_api_key = None
            from greengraph.ingest import _extract_entities

            result = _extract_entities("Alice works at Acme Corp.")
        assert result == []

    def test_returns_entities_from_llm(self) -> None:
        mock_choice = MagicMock()
        mock_choice.message.content = '[{"name": "Alice", "type": "person", "properties": {}}]'

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response

        with patch("greengraph.ingest.settings") as mock_settings:
            mock_settings.openai_api_key = "sk-test"
            mock_settings.openai_chat_model = "gpt-4o-mini"
            with patch("openai.OpenAI", return_value=mock_client):
                from greengraph.ingest import _extract_entities

                result = _extract_entities("Alice works at Acme Corp.")

        assert len(result) == 1
        assert result[0]["name"] == "Alice"
        assert result[0]["type"] == "person"

    def test_gracefully_handles_llm_error(self) -> None:
        with patch("greengraph.ingest.settings") as mock_settings:
            mock_settings.openai_api_key = "sk-test"
            mock_settings.openai_chat_model = "gpt-4o-mini"
            with patch("openai.OpenAI", side_effect=Exception("API error")):
                from greengraph.ingest import _extract_entities

                result = _extract_entities("Some text")

        assert result == []


class TestFormatVector:
    def test_format_single_value(self) -> None:
        from greengraph.ingest import _format_vector

        result = _format_vector([1.0, 2.0, 3.0])
        assert result == "[1.0,2.0,3.0]"

    def test_format_empty(self) -> None:
        from greengraph.ingest import _format_vector

        result = _format_vector([])
        assert result == "[]"
