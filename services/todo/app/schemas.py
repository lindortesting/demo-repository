"""Pydantic schemas: request bodies in, `TodoOut` responses out."""
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TodoCreate(BaseModel):
    """Body for POST /todos."""

    title: str = Field(min_length=1, max_length=200)


class TodoUpdate(BaseModel):
    """Body for PATCH /todos/{id}; omitted fields are left unchanged."""

    title: str | None = Field(default=None, min_length=1, max_length=200)
    done: bool | None = None


class TodoOut(BaseModel):
    """Serialized todo returned by every endpoint."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    title: str
    done: bool
    created_at: datetime
