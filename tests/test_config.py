"""Unit tests for the Settings configuration."""

from __future__ import annotations

import pytest


class TestSettings:
    def test_default_embedding_provider_is_mock(self) -> None:
        from greengraph.config import Settings

        s = Settings()
        assert s.embedding_provider == "mock"

    def test_graph_name_computed_field(self) -> None:
        from greengraph.config import Settings

        s = Settings()
        assert s.graph_name == "context_graph"

    def test_chunk_size_default(self) -> None:
        from greengraph.config import Settings

        s = Settings()
        assert s.chunk_size == 512

    def test_chunk_overlap_default(self) -> None:
        from greengraph.config import Settings

        s = Settings()
        assert s.chunk_overlap == 50

    def test_embedding_dim_default(self) -> None:
        from greengraph.config import Settings

        s = Settings()
        assert s.embedding_dim == 1536

    def test_top_k_default(self) -> None:
        from greengraph.config import Settings

        s = Settings()
        assert s.top_k_chunks == 20

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHUNK_SIZE", "256")
        monkeypatch.setenv("EMBEDDING_PROVIDER", "mock")
        from greengraph.config import Settings

        s = Settings()
        assert s.chunk_size == 256
