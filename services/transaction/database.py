"""
database.py – Transaction Service

WHY SQLAlchemy?
  - ORM layer so we write Python instead of raw SQL
  - `create_all()` auto-creates tables on first boot
  - `get_db()` is a FastAPI dependency that yields a
    session and guarantees it's closed after each request
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://chaosmend:chaosmend_secret@localhost:5432/chaosmend")

# connect_args only needed for SQLite; psycopg2 handles pooling itself
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Each request gets its own session (thread-safe)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class all ORM models inherit from
Base = declarative_base()


def get_db():
    """FastAPI dependency: provides a DB session, cleans up after request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
