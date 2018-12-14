from collections import namedtuple
import pytest
from unittest.mock import MagicMock

from tagdir.cli import untag


@pytest.fixture(autouse=True, scope="module")
def mock_xattr():
    import xattr
    xattr.xattr = MagicMock()
    xattr.xattr.return_value = {"test": b"/path/test,tag1,tag2", "dummy": b""}


@pytest.fixture(autouse=True, scope="module")
def mock_run():
    import subprocess
    mock = MagicMock()
    subprocess.run = mock
    return mock


Args = namedtuple("Args", ("tags", "path"))


def test_normal(mock_run):
    args = Args(["tag1", "tag2"], "/path/test")
    assert untag(args, "/mountpoint") == 0
    mock_run.assert_called_with(["unlink", "/mountpoint/@tag1/@tag2/test"],
                                capture_output=True)


def test_nonexistent_entity():
    args = Args(["tag1", "tag2"], "/path/not/found")
    assert untag(args, "/mountpoint") == -1


def test_invalid_path():
    args = Args(["tag1", "tag2"], "/path/invalid/test")
    assert untag(args, "/mountpoint") == -1
