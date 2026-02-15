"""Database connection and session management."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from typing import Generator
from loguru import logger

from config import settings
from database.models import Base


# Create engine
engine = create_engine(
    settings.database_url,
    echo=False,  # Set to True for SQL debugging
    connect_args={"check_same_thread": False} if "sqlite" in settings.database_url else {}
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Initialize the database by creating all tables."""
    logger.info(f"Initializing database: {settings.database_url}")
    Base.metadata.create_all(bind=engine)
    logger.success("Database tables created successfully")


def get_db() -> Generator[Session, None, None]:
    """
    Get a database session.
    Use as a generator for dependency injection.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.
    
    Usage:
        with get_db_session() as db:
            db.query(...)
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        db.close()


class DatabaseManager:
    """
    High-level database operations manager.
    """
    
    def __init__(self):
        init_db()
    
    @property
    def session(self) -> Session:
        """Get a new session."""
        return SessionLocal()
    
    def execute_query(self, query: str):
        """Execute raw SQL query."""
        with get_db_session() as db:
            result = db.execute(query)
            return result.fetchall()


# Usage example
if __name__ == "__main__":
    # Initialize database
    init_db()
    print("Database initialized successfully!")
    
    # Test session
    with get_db_session() as db:
        print(f"Session created: {db}")
