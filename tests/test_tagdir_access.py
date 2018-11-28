from errno import ENOENT

import pytest

from .fixture import setup_tagdir_fixture
from tagdir.fusepy.fuse import FuseOSError
from tagdir.fusepy.loopback import Loopback
from tagdir.models import Entity, Tag


def setup_func(session):
    tag1 = Tag("tag1")
    tag2 = Tag("tag2")
    entity1 = Entity("entity1", "/path1", [tag1, tag2])
    entity2 = Entity("entity2", "/path2", [])
    session.add_all([tag1, tag2, entity1, entity2])


setup_tagdir_fixture(setup_func)


@pytest.fixture(autouse=True)
def access_mock(mocker):
    access_mock = mocker.patch.object(Loopback, "access")
    access_mock.return_value = 0
    return access_mock


def test_root(tagdir):
    assert tagdir.access("/", 0) == 0


def test_existent_tag1(tagdir):
    assert tagdir.access("/@tag1", 0) == 0


def test_existent_entity1(tagdir):
    assert tagdir.access("/@tag1/entity1", 0) == 0


def test_existent_entity2(tagdir):
    assert tagdir.access("/@tag1/@tag2/entity1", 0) == 0


def test_pass_through(tagdir, access_mock):
    assert tagdir.access("/@tag1/entity1/test", 0) == 0
    access_mock.assert_called_with("/path1/test", 0)


def test_nonexistent_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.access("/@non_tag", 0)
    assert exc.value.errno == ENOENT


def test_no_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.access("/entity1", 0)
    assert exc.value.errno == ENOENT


def test_notag_entity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.access("/@tag1/entity2", 0)
    assert exc.value.errno == ENOENT
