import inspect

import pytest

from tagdir.db import setup_db, session_scope
from tagdir.tagdir import Tagdir


@pytest.fixture
def tagdir(_tagdir):
    from tagdir.session import Session
    session = Session()
    _tagdir.session = session
    yield _tagdir
    session.rollback()
    session.close()


def setup_tagdir_test(func):
    @pytest.fixture(scope="module")
    def _tagdir():
        setup_db("sqlite:///:memory:")
        tagdir = Tagdir()
        with session_scope() as session:
            func(session)
        return tagdir

    caller_globals = inspect.stack()[1][0].f_globals
    caller_globals["_tagdir"] = _tagdir
