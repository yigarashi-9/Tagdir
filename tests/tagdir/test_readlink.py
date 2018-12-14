from errno import ENOENT, EINVAL

import pytest

from .conftest import setup_tagdir_test
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Attr, Entity, Tag


def setup_func(session):
    attr_tag = Attr.new_tag_attr()
    tag1 = Tag("tag1", attr_tag)
    tag2 = Tag("tag2", attr_tag)
    attr_ent = Attr.new_entity_attr()
    entity1 = Entity("entity1", attr_ent, "/path1/entity1", [tag1])
    session.add_all([attr_tag, attr_ent, tag1, tag2, entity1])


# Dynamically define tagdir fixture
setup_tagdir_test(setup_func)


def test_readlink(tagdir):
    assert tagdir.readlink("/@tag1/entity1") == "/path1/entity1"


def test_root(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.readlink("/")
    assert exc.value.errno == EINVAL


def test_no_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.readlink("/entity1")
    assert exc.value.errno == EINVAL


def test_no_entity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.readlink("/@tag1/@tag2")
    assert exc.value.errno == EINVAL


def test_nonexistetnt_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.readlink("/@tag3/entity1")
    assert exc.value.errno == ENOENT


def test_nonexistetnt_entity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.readlink("/@tag1/entity2")
    assert exc.value.errno == ENOENT


def test_invalid_tagging(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.readlink("/@tag2/entity1")
    assert exc.value.errno == ENOENT
