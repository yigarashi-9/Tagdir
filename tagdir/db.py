from contextlib import contextmanager


@contextmanager
def session_scope(session_cls):
    """Provide a transactional scope around a series of operations."""
    session = session_cls()
    try:
        yield session
        session.commit()
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()
