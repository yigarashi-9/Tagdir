from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Base


def setup_db(path):
    engine = create_engine(path, echo=False)
    Base.metadata.create_all(engine)
    from . import session
    session.Session = sessionmaker(bind=engine)  # type: ignore


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    from .session import Session
    session = Session()
    try:
        yield session
        session.commit()
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()
