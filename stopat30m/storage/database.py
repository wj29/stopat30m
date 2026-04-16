"""SQLAlchemy engine and session management for local SQLite storage.

Qlib binary data remains file-based; this module handles structured data:
analysis history, trade records, backtest summaries, signal history.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from stopat30m.config import get

_engine: Engine | None = None
_SessionFactory: sessionmaker | None = None


def _get_db_url() -> str:
    db_path = get("storage", "sqlite_path", "./data/stopat30m.db")
    p = Path(db_path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{p}"


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = _get_db_url()
        _engine = create_engine(
            url,
            echo=False,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False},
        )

        @event.listens_for(_engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return _engine


def _get_session_factory() -> sessionmaker:
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _SessionFactory


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """Context manager that yields a SQLAlchemy Session and auto-commits/rollbacks."""
    session = _get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """Create all tables if they don't exist, then run lightweight migrations."""
    from .models import Base
    Base.metadata.create_all(get_engine())
    _migrate_add_columns()


def _migrate_add_columns() -> None:
    """Add columns introduced after initial table creation (idempotent)."""
    engine = get_engine()
    with engine.connect() as conn:
        for table, col, col_type, default in [
            ("analysis_history", "data_source", "VARCHAR(30)", "''"),
            ("analysis_history", "user_id", "INTEGER", "NULL"),
            ("analysis_history", "status", "VARCHAR(10)", "'completed'"),
            ("backtest_runs", "user_id", "INTEGER", "NULL"),
        ]:
            try:
                conn.execute(
                    __import__("sqlalchemy").text(
                        f"ALTER TABLE {table} ADD COLUMN {col} {col_type} DEFAULT {default}"
                    )
                )
                conn.commit()
            except Exception:
                pass
