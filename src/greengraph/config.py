"""Application configuration via environment variables."""

from typing import Literal

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings, all overridable via environment variables or .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Database ---
    database_url: str = Field(
        default="postgresql://context_graph:changeme_in_production@localhost:5432/context_graph_db",
        description="PostgreSQL connection string",
    )
    pg_port: int = Field(default=5432)
    postgres_password: str = Field(default="changeme_in_production")

    # --- Embeddings ---
    embedding_provider: Literal["openai", "mock"] = Field(
        default="mock",
        description="Embedding backend: 'openai' or 'mock' (for testing)",
    )
    embedding_model: str = Field(default="text-embedding-ada-002")
    embedding_dim: int = Field(default=1536)
    openai_api_key: str | None = Field(default=None)

    # --- LLM (entity extraction) ---
    anthropic_api_key: str | None = Field(default=None)
    claude_model: str = Field(default="claude-sonnet-4-6")

    # --- Ingestion ---
    chunk_size: int = Field(default=512, description="Target chunk size in characters")
    chunk_overlap: int = Field(default=50, description="Overlap between chunks in characters")
    ingestion_batch_size: int = Field(default=32)

    # --- Retrieval ---
    vector_similarity_threshold: float = Field(default=0.7)
    top_k_chunks: int = Field(default=20)
    graph_hop_depth: int = Field(default=2)
    hnsw_ef_search: int = Field(default=100)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def graph_name(self) -> str:
        return "context_graph"


settings = Settings()
