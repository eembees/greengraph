"""Embedding generation backends.

Supported providers:
  - openai: Uses OpenAI text-embedding-ada-002 (1536 dims) or any OpenAI embedding model.
  - mock:   Returns deterministic pseudo-random vectors (for testing / CI).
"""

from __future__ import annotations

import hashlib
import math
from abc import ABC, abstractmethod

from greengraph.config import settings


class EmbeddingBackend(ABC):
    """Abstract base for embedding backends."""

    @property
    @abstractmethod
    def dim(self) -> int:
        """Dimensionality of the produced embeddings."""

    @abstractmethod
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts. Returns a list of float vectors."""

    def embed_one(self, text: str) -> list[float]:
        return self.embed([text])[0]


class MockEmbeddingBackend(EmbeddingBackend):
    """Deterministic pseudo-random embeddings for testing.

    Uses SHA-256 of the text as a seed so identical inputs produce identical
    (but meaningless) vectors. Unit-normalized so cosine distance works.
    """

    def __init__(self, dim: int = 1536) -> None:
        self._dim = dim

    @property
    def dim(self) -> int:
        return self._dim

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_single(t) for t in texts]

    def _embed_single(self, text: str) -> list[float]:
        digest = hashlib.sha256(text.encode()).digest()
        # Expand digest into `dim` floats using a simple LCG seeded by digest bytes
        seed = int.from_bytes(digest[:8], "big")
        vec: list[float] = []
        for _i in range(self._dim):
            seed = (seed * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
            vec.append((seed / 0xFFFFFFFFFFFFFFFF) * 2 - 1)
        # L2-normalize
        norm = math.sqrt(sum(x * x for x in vec))
        return [x / norm for x in vec]


class OpenAIEmbeddingBackend(EmbeddingBackend):
    """OpenAI embedding backend (uses the openai Python SDK)."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "text-embedding-ada-002",
        dim: int = 1536,
    ) -> None:
        try:
            from openai import OpenAI
        except ImportError as e:
            raise ImportError(
                "openai package is required for OpenAIEmbeddingBackend. "
                "Install it with: uv add openai"
            ) from e

        self._client = OpenAI(api_key=api_key or settings.openai_api_key)
        self._model = model
        self._dim = dim

    @property
    def dim(self) -> int:
        return self._dim

    def embed(self, texts: list[str]) -> list[list[float]]:
        # Strip newlines per OpenAI recommendation
        cleaned = [t.replace("\n", " ") for t in texts]
        response = self._client.embeddings.create(input=cleaned, model=self._model)
        return [item.embedding for item in response.data]


def get_embedding_backend() -> EmbeddingBackend:
    """Factory: return the configured embedding backend."""
    provider = settings.embedding_provider
    if provider == "openai":
        return OpenAIEmbeddingBackend(
            api_key=settings.openai_api_key,
            model=settings.embedding_model,
            dim=settings.embedding_dim,
        )
    if provider == "mock":
        return MockEmbeddingBackend(dim=settings.embedding_dim)
    raise ValueError(f"Unknown embedding provider: {provider!r}")


# Module-level singleton (lazily initialized)
_backend: EmbeddingBackend | None = None


def get_backend() -> EmbeddingBackend:
    global _backend
    if _backend is None:
        _backend = get_embedding_backend()
    return _backend


def embed_texts(texts: list[str], backend: EmbeddingBackend | None = None) -> list[list[float]]:
    """Embed a list of texts using the configured (or provided) backend."""
    b = backend or get_backend()
    return b.embed(texts)


def embed_text(text: str, backend: EmbeddingBackend | None = None) -> list[float]:
    """Embed a single text."""
    return embed_texts([text], backend)[0]
