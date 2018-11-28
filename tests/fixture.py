import inspect

import pytest
from sqlalchemy import create_engine

from tagdir.db import session_scope
from tagdir.tagdir import Tagdir


@pytest.fixture
def tagdir(base_tagdir):
    session = base_tagdir.Session()
    base_tagdir.session = session
    yield base_tagdir
    session.rollback()
    session.close()


def setup_tagdir_fixture(func):
    @pytest.fixture(scope="module")
    def base_tagdir():
        engine = create_engine("sqlite:///:memory:", echo=False)
        tagdir = Tagdir(engine)
        with session_scope(tagdir.Session) as session:
            func(session)
        return tagdir

    caller_globals = inspect.stack()[1][0].f_globals
    caller_globals["base_tagdir"] = base_tagdir
    caller_globals["tagdir"] = tagdir
