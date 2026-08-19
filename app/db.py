"""Database engine, session factory, and initialisation for SmartDialer.

The DATABASE_URL environment variable selects the backend:
- Default: ``sqlite:///smartdialer.db``  (WAL mode, same-process WAL journal)
- Override to Postgres: ``DATABASE_URL=postgresql+psycopg2://user:pw@host/db``

SQLite-specific configuration
------------------------------
On every new SQLite connection, three PRAGMAs are applied via a SQLAlchemy
*event listener* so they are set before any other statement runs:
- ``PRAGMA journal_mode=WAL``  — allows concurrent readers + one writer
- ``PRAGMA synchronous=NORMAL`` — safe on WAL, good balance of speed/durability
- ``PRAGMA foreign_keys=ON``    — enforce FK constraints (off by default)
"""

from __future__ import annotations

import os
from collections.abc import Generator

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.models import Base

# ─────────────────────────────────────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────────────────────────────────────

DATABASE_URL: str = os.environ.get(
    "DATABASE_URL", "sqlite:///smartdialer.db"
)

# check_same_thread=False is required for SQLite when the same connection is
# used across threads (our worker-thread model). SQLAlchemy's connection pool
# manages thread safety at a higher level.
_connect_args: dict = (
    {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
)

engine: Engine = create_engine(
    DATABASE_URL,
    connect_args=_connect_args,
    # pool_pre_ping keeps the connection alive across idle periods.
    pool_pre_ping=True,
    echo=False,
)


# ─────────────────────────────────────────────────────────────────────────────
# SQLite WAL pragma listener
# ─────────────────────────────────────────────────────────────────────────────

@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record) -> None:  # type: ignore[no-untyped-def]
    """Apply WAL-mode and other pragmas on each new raw DBAPI connection."""
    if DATABASE_URL.startswith("sqlite"):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


# ─────────────────────────────────────────────────────────────────────────────
# Session factory
# ─────────────────────────────────────────────────────────────────────────────

SessionLocal: sessionmaker[Session] = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
)


def get_db() -> Generator[Session, None, None]:
    """FastAPI / pytest dependency that yields a scoped session.

    Usage (FastAPI)::

        @app.get("/example")
        def example(db: Session = Depends(get_db)):
            ...

    Usage (test)::

        with next(get_db()) as db:
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Schema initialisation
# ─────────────────────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create all tables that do not already exist.

    Safe to call multiple times (``CREATE TABLE IF NOT EXISTS`` semantics).
    For production migrations, use Alembic instead.
    """
    Base.metadata.create_all(bind=engine)


def verify_wal_mode() -> str:
    """Return the current SQLite journal mode (should be 'wal' after init)."""
    with engine.connect() as conn:
        result = conn.execute(text("PRAGMA journal_mode"))
        row = result.fetchone()
        return row[0] if row else "unknown"
