from collections import namedtuple
import pytest
from unittest.mock import MagicMock

from tagdir.cli import listag


@pytest.fixture(autouse=True, scope="module")
def mock_xattr():
    import xattr
    xattr.xattr = MagicMock()
    xattr.xattr.return_value = {"test": b"/path/test,tag2,tag1", "dummy": b""}


@pytest.fixture(autouse=True, scope="module")
def mock_run():
    import subprocess
    mock = MagicMock()
    subprocess.run = mock
    return mock


@pytest.fixture(autouse=True, scope="module")
def mock_check_output():
    import subprocess
    subprocess.check_output = MagicMock()
    subprocess.check_output.return_value = b"@tag1\n@tag2\n@tag3\n"


Args = namedtuple("Args", ("path",))


def test_normal_all(capsys):
    args = Args(None)
    assert listag(args, "/mountpoint") == 0
    captured = capsys.readouterr()
    assert captured.out == "tag1\ntag2\ntag3\n"


def test_normal(capsys):
    args = Args("/path/test")
    assert listag(args, "/mountpoint") == 0
    captured = capsys.readouterr()
    assert captured.out == "tag1\ntag2\n"


def test_nonexistent_entity():
    args = Args("/path/not/found")
    assert listag(args, "/mountpoint") == -1


def test_invalid_path():
    args = Args("/path/invalid/test")
    assert listag(args, "/mountpoint") == -1
