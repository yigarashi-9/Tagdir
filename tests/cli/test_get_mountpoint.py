from collections import namedtuple

import psutil
import pytest
from unittest.mock import MagicMock

from tagdir.cli import get_mountpoint


@pytest.fixture
def mock_disk_partitions():
    def _mock_disk_partitions(ret_val):
        mock = MagicMock()
        mock.return_value = ret_val
        psutil.disk_partitions = mock
    return _mock_disk_partitions


Disk = namedtuple("Disk", ("device", "mountpoint"))
mock_data_1 = [
    Disk("Tagdir_test", "/path"),
    Disk("dummy", "/dummy")
]

mock_data_2 = [
    Disk("Tagdir_test1", "/path1"),
    Disk("Tagdir_test2", "/path2"),
    Disk("dummy", "/dummy")
]


def test_get_implicitly(mock_disk_partitions):
    mock_disk_partitions(mock_data_1)
    assert get_mountpoint(None) == "/path"


def test_get_implicitly_fail(mock_disk_partitions):
    mock_disk_partitions(mock_data_2)
    assert get_mountpoint(None) is None


def test_get_explicitly(mock_disk_partitions):
    mock_disk_partitions(mock_data_2)
    assert get_mountpoint("test1") == "/path1"


def test_get_explicitly_fail(mock_disk_partitions):
    mock_disk_partitions(mock_data_2)
    assert get_mountpoint("test_fail") is None
