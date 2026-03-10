"""Pydantic models for the context graph domain."""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, computed_field


class DocumentCreate(BaseModel):
    """Input model for creating a new document."""

    title: str
    content: str
    source_url: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def source_hash(self) -> str:
        """SHA-256 of the content for deduplication."""
        return hashlib.sha256(self.content.encode()).hexdigest()


class Document(BaseModel):
    """A persisted document record."""

    id: int
    title: str
    source_url: str | None = None
    source_hash: str | None = None
    ingested_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class ChunkCreate(BaseModel):
    """Input model for creating a chunk."""

    document_id: int
    content: str
    chunk_index: int
    embedding: list[float] | None = None


class Chunk(BaseModel):
    """A persisted text chunk record."""

    id: int
    document_id: int
    content: str
    chunk_index: int
    embedding: list[float] | None = None
    created_at: datetime


class EntityCreate(BaseModel):
    """Input model for creating an entity."""

    name: str
    type: str  # person, organization, concept, location, etc.
    properties: dict[str, Any] = Field(default_factory=dict)
    embedding: list[float] | None = None


class Entity(BaseModel):
    """A persisted entity record."""

    id: int
    name: str
    type: str
    properties: dict[str, Any] = Field(default_factory=dict)
    embedding: list[float] | None = None
    created_at: datetime


class MetadataTagCreate(BaseModel):
    """Input model for a metadata tag."""

    entity_id: int
    key: str
    value: str
    valid_from: datetime | None = None
    valid_to: datetime | None = None


class MetadataTag(BaseModel):
    """A persisted metadata tag."""

    id: int
    entity_id: int
    key: str
    value: str
    valid_from: datetime | None = None
    valid_to: datetime | None = None


class RetrievedChunk(BaseModel):
    """A chunk returned from vector similarity search."""

    chunk_id: int
    document_id: int
    content: str
    similarity: float
    graph_context: list[GraphContext] = Field(default_factory=list)


class GraphContext(BaseModel):
    """A piece of graph context attached to a retrieved chunk."""

    entity: str
    relationship: str
    connected_entity: str
    entity_type: str


class RetrievalResult(BaseModel):
    """Combined result from two-phase (vector + graph) retrieval."""

    query: str
    chunks: list[RetrievedChunk]
    total_chunks: int

    @computed_field  # type: ignore[prop-decorator]
    @property
    def context_text(self) -> str:
        """Assemble chunks and graph context into a single prompt-ready string."""
        parts: list[str] = []
        for chunk in self.chunks:
            parts.append(f"[Chunk {chunk.chunk_id} | similarity={chunk.similarity:.3f}]")
            parts.append(chunk.content)
            if chunk.graph_context:
                parts.append("Related context:")
                for ctx in chunk.graph_context:
                    parts.append(
                        f"  {ctx.entity} --[{ctx.relationship}]--> {ctx.connected_entity}"
                        f" ({ctx.entity_type})"
                    )
            parts.append("")
        return "\n".join(parts)


class IngestionResult(BaseModel):
    """Summary of a completed ingestion run."""

    document_id: int
    title: str
    chunks_created: int
    entities_extracted: int
    relationships_created: int
    skipped: bool = False
    skip_reason: str | None = None
