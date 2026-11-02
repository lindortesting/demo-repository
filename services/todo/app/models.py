"""Database model for the todo service: one `todos` table."""
from datetime import datetime, timezone

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all models."""


def utcnow() -> datetime:
    """Naive UTC now — SQLite stores datetimes without timezone info."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Todo(Base):
    """A single todo item."""

    __tablename__ = "todos"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(nullable=False)
    done: Mapped[bool] = mapped_column(nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=utcnow)
