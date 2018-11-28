import inspect
import sys
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine

from tagdir.db import session_scope


@pytest.fixture
def tagdir(_tagdir):
    session = _tagdir.Session()
    _tagdir.session = session
    yield _tagdir
    session.rollback()
    session.close()


@pytest.fixture(scope="session")
def _fuse_mock():
    mock_module = MagicMock()
    sys.modules["tagdir.fusepy.fuse"] = mock_module

    # Workaround to avoid metaclass conflict
    class Dummy:
        pass

    mock_module.Operations = Dummy
    return mock_module


def setup_tagdir_test(func, method_name, retval=None):
    @pytest.fixture(scope="module")
    def _tagdir(_fuse_mock):
        # Import after mocking
        from tagdir.tagdir import Tagdir

        engine = create_engine("sqlite:///:memory:", echo=False)
        tagdir = Tagdir(engine)
        with session_scope(tagdir.Session) as session:
            func(session)
        return tagdir

    @pytest.fixture(autouse=True)
    def method_mock(_fuse_mock, mocker):
        # Import after mocking
        from tagdir.fusepy.loopback import Loopback

        mock = mocker.patch.object(Loopback, method_name)
        mock.return_value = retval
        return mock

    caller_globals = inspect.stack()[1][0].f_globals
    caller_globals["_tagdir"] = _tagdir
    caller_globals["method_mock"] = method_mock
