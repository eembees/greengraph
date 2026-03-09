"""Unit tests for embedding backends."""

from __future__ import annotations

import math

from greengraph.embeddings import MockEmbeddingBackend, embed_text, embed_texts


class TestMockEmbeddingBackend:
    def test_dim_property(self) -> None:
        backend = MockEmbeddingBackend(dim=128)
        assert backend.dim == 128

    def test_embed_returns_correct_length(self) -> None:
        backend = MockEmbeddingBackend(dim=256)
        vecs = backend.embed(["hello", "world"])
        assert len(vecs) == 2
        assert all(len(v) == 256 for v in vecs)

    def test_embed_is_deterministic(self) -> None:
        backend = MockEmbeddingBackend(dim=64)
        v1 = backend.embed(["test text"])
        v2 = backend.embed(["test text"])
        assert v1 == v2

    def test_different_texts_yield_different_embeddings(self) -> None:
        backend = MockEmbeddingBackend(dim=64)
        v1 = backend.embed_one("hello")
        v2 = backend.embed_one("world")
        assert v1 != v2

    def test_embeddings_are_unit_normalized(self) -> None:
        backend = MockEmbeddingBackend(dim=64)
        v = backend.embed_one("normalize me")
        norm = math.sqrt(sum(x * x for x in v))
        assert abs(norm - 1.0) < 1e-6

    def test_batch_embed_matches_single(self) -> None:
        backend = MockEmbeddingBackend(dim=32)
        texts = ["a", "b", "c"]
        batch = backend.embed(texts)
        singles = [backend.embed_one(t) for t in texts]
        assert batch == singles

    def test_default_dim_is_1536(self) -> None:
        backend = MockEmbeddingBackend()
        assert backend.dim == 1536

    def test_empty_batch(self) -> None:
        backend = MockEmbeddingBackend(dim=32)
        result = backend.embed([])
        assert result == []


class TestEmbedFunctions:
    def test_embed_texts_uses_backend(self, mock_backend: MockEmbeddingBackend) -> None:
        result = embed_texts(["hello", "world"], backend=mock_backend)
        assert len(result) == 2

    def test_embed_text_single(self, mock_backend: MockEmbeddingBackend) -> None:
        result = embed_text("hello", backend=mock_backend)
        assert len(result) == 1536
        assert isinstance(result[0], float)
