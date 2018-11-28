from errno import ENOENT

import pytest

from .conftest import setup_tagdir_test
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Entity, Tag


def setup_func(session):
    tag1 = Tag("tag1")
    tag2 = Tag("tag2")
    entity1 = Entity("entity1", "/path1", [tag1, tag2])
    entity2 = Entity("entity2", "/path2", [])
    session.add_all([tag1, tag2, entity1, entity2])


# Dynamically define tagdir and method_mock fixtures
setup_tagdir_test(setup_func, "access")


def test_root(tagdir):
    assert tagdir.access("/", 0) == 0


def test_existent_tag1(tagdir):
    assert tagdir.access("/@tag1", 0) == 0


def test_existent_entity1(tagdir):
    assert tagdir.access("/@tag1/entity1", 0) == 0


def test_existent_entity2(tagdir):
    assert tagdir.access("/@tag1/@tag2/entity1", 0) == 0


def test_pass_through(tagdir, method_mock):
    tagdir.access("/@tag1/entity1/test", 0)
    method_mock.assert_called_with("/path1/test", 0)


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
