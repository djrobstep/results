from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session


class SqlaSession:
    """A wrapper around SQLAlchemy Session for simplified usage."""

    def __init__(self, session: Session) -> None:
        self.s: Session = session

    def execute(self, *args, **kwargs):
        """Execute a SQL statement with automatic text conversion."""
        return self.s.execute(text(args[0]), *args[1:], **kwargs)


@contextmanager
def S(url):
    """Create a SQLAlchemy session context manager."""
    if hasattr(url, "url"):
        url = url.url

    # Create an Engine, which the Session will use for connection resources
    engine = create_engine(url)

    try:
        # Create session and add objects
        with Session(engine) as session:
            yield SqlaSession(session)
            session.commit()
    finally:
        # Dispose of the engine to close all connections in the pool
        # This prevents "server closed the connection unexpectedly" errors
        # when temporary databases are dropped
        engine.dispose()
