from datetime import datetime
from typing import Any

from sqlmodel import JSON, Column, Field, SQLModel


class Graph(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: str = ""
    data: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class GraphCreate(SQLModel):
    name: str
    description: str = ""
    data: dict[str, Any] = Field(default_factory=dict)


class GraphUpdate(SQLModel):
    name: str | None = None
    description: str | None = None
    data: dict[str, Any] | None = None


class GraphRead(SQLModel):
    id: int
    name: str
    description: str
    data: dict[str, Any]
    created_at: datetime
    updated_at: datetime
