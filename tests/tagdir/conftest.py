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


def setup_tagdir_test(func, method_name=None, retval=None):
    @pytest.fixture(scope="module")
    def _tagdir():
        setup_db("sqlite:///:memory:")
        tagdir = Tagdir()
        with session_scope() as session:
            func(session)
        return tagdir

    @pytest.fixture(autouse=True)
    def method_mock(mocker):
        # Import after mocking
        from tagdir.fusepy.loopback import Loopback

        if method_name:
            mock = mocker.patch.object(Loopback, method_name)
            mock.return_value = retval
            return mock
        else:
            return None

    caller_globals = inspect.stack()[1][0].f_globals
    caller_globals["_tagdir"] = _tagdir
    caller_globals["method_mock"] = method_mock
