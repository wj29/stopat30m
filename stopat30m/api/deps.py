"""FastAPI dependency injection utilities."""

from __future__ import annotations

from typing import Generator

from sqlalchemy.orm import Session

from stopat30m.storage.database import get_db as _get_db


def get_db_session() -> Generator[Session, None, None]:
    """FastAPI dependency that yields a database session."""
    with _get_db() as session:
        yield session
