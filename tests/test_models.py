"""Unit tests for Pydantic models."""

from __future__ import annotations

from greengraph.models import (
    ChunkCreate,
    DocumentCreate,
    GraphContext,
    IngestionResult,
    RetrievalResult,
    RetrievedChunk,
)


class TestDocumentCreate:
    def test_source_hash_is_deterministic(self) -> None:
        doc1 = DocumentCreate(title="A", content="hello world")
        doc2 = DocumentCreate(title="B", content="hello world")
        assert doc1.source_hash == doc2.source_hash

    def test_different_content_yields_different_hash(self) -> None:
        doc1 = DocumentCreate(title="A", content="hello")
        doc2 = DocumentCreate(title="A", content="world")
        assert doc1.source_hash != doc2.source_hash

    def test_source_hash_is_hex_string(self) -> None:
        doc = DocumentCreate(title="A", content="test")
        assert len(doc.source_hash) == 64
        assert all(c in "0123456789abcdef" for c in doc.source_hash)

    def test_default_metadata_is_empty_dict(self) -> None:
        doc = DocumentCreate(title="A", content="x")
        assert doc.metadata == {}

    def test_metadata_preserved(self) -> None:
        doc = DocumentCreate(title="A", content="x", metadata={"key": "val"})
        assert doc.metadata["key"] == "val"


class TestChunkCreate:
    def test_basic_creation(self) -> None:
        chunk = ChunkCreate(document_id=1, content="hello", chunk_index=0)
        assert chunk.content == "hello"
        assert chunk.embedding is None

    def test_with_embedding(self) -> None:
        emb = [0.1] * 1536
        chunk = ChunkCreate(document_id=1, content="hi", chunk_index=0, embedding=emb)
        assert len(chunk.embedding) == 1536  # type: ignore[arg-type]


class TestIngestionResult:
    def test_not_skipped_by_default(self) -> None:
        result = IngestionResult(
            document_id=1,
            title="T",
            chunks_created=5,
            entities_extracted=2,
            relationships_created=3,
        )
        assert not result.skipped
        assert result.skip_reason is None

    def test_skipped_result(self) -> None:
        result = IngestionResult(
            document_id=1,
            title="T",
            chunks_created=0,
            entities_extracted=0,
            relationships_created=0,
            skipped=True,
            skip_reason="duplicate",
        )
        assert result.skipped
        assert result.skip_reason == "duplicate"


class TestRetrievalResult:
    def test_context_text_assembly(self) -> None:
        ctx = GraphContext(
            entity="Alice",
            relationship="WORKS_AT",
            connected_entity="Acme",
            entity_type="Organization",
        )
        chunk = RetrievedChunk(
            chunk_id=1,
            document_id=1,
            content="Alice works at Acme.",
            similarity=0.92,
            graph_context=[ctx],
        )
        result = RetrievalResult(query="Alice", chunks=[chunk], total_chunks=1)
        text = result.context_text

        assert "Alice works at Acme." in text
        assert "Alice" in text
        assert "WORKS_AT" in text
        assert "Acme" in text
        assert "0.920" in text

    def test_empty_context_text(self) -> None:
        result = RetrievalResult(query="nothing", chunks=[], total_chunks=0)
        assert result.context_text == ""

    def test_context_text_without_graph(self) -> None:
        chunk = RetrievedChunk(
            chunk_id=2,
            document_id=1,
            content="Some content.",
            similarity=0.85,
        )
        result = RetrievalResult(query="q", chunks=[chunk], total_chunks=1)
        assert "Some content." in result.context_text
        assert "Related context:" not in result.context_text
