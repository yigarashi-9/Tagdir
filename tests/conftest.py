import inspect
import sys
from unittest.mock import MagicMock

import pytest

from tagdir.db import setup_db, session_scope


@pytest.fixture
def tagdir(_tagdir):
    from tagdir.session import Session
    session = Session()
    _tagdir.session = session
    yield _tagdir
    session.rollback()
    session.close()


@pytest.fixture(scope="session")
def _fuse_mock():
    mock_module = MagicMock()
    sys.modules["tagdir.fusepy.fuse"] = mock_module
    mock_module.Operations = type("Dummy", (object,), {})
    return mock_module


def setup_tagdir_test(func, method_name):
    @pytest.fixture(scope="module")
    def _tagdir(_fuse_mock):
        # Import after mocking
        from tagdir.tagdir import Tagdir
        setup_db("sqlite:///:memory:")
        tagdir = Tagdir()
        with session_scope() as session:
            func(session)
        return tagdir

    @ pytest.fixture(autouse=True)
    def _method_mock(_fuse_mock, mocker):
        # Import after mocking
        from tagdir.fusepy.fuse import Operations
        mock = mocker.patch.object(Operations, method_name, create=True)
        return mock

    caller_globals = inspect.stack()[1][0].f_globals
    caller_globals["_tagdir"] = _tagdir
    caller_globals["_method_mock"] = _method_mock
