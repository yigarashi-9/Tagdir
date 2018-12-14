import sys
from unittest.mock import MagicMock


def pytest_sessionstart(session):
    mock_module = MagicMock()
    sys.modules["tagdir.fusepy.fuse"] = mock_module
    mock_module.Operations = type("Dummy", (object,), {})
    mock_module.ENOTSUP = 100000  # Dummy value
