from errno import EINVAL, ENOENT

import pytest

from .conftest import setup_tagdir_test
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Attr, Entity, Tag


def setup_func(session):
    attr_tag = Attr.new_tag_attr()
    tag1 = Tag("tag1", attr_tag)
    tag2 = Tag("tag2", attr_tag)
    attr_ent = Attr.new_entity_attr()
    entity1 = Entity("entity1", attr_ent, "/path1", [tag1, tag2])
    entity2 = Entity("entity2", attr_ent, "/path2", [tag1])
    entity3 = Entity("entity3", attr_ent, "/path3", [])
    session.add_all([attr_ent, tag1, tag2,
                     attr_ent, entity1, entity2, entity3])


# Dynamically define tagdir fixtures
setup_tagdir_test(setup_func, "readdir")


def test_root(tagdir):
    assert sorted(tagdir.readdir("/", None)) == ["@tag1", "@tag2"]


def test_filter1(tagdir):
    assert sorted(tagdir.readdir("/@tag1", None)) == ["entity1", "entity2"]


def test_filter2(tagdir):
    assert sorted(tagdir.readdir("/@tag1/@tag2", None)) == ["entity1"]


def test_nonexistent_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.readdir("/@non_tag", None)
    assert exc.value.errno == ENOENT


def test_no_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.readdir("/entity1", None)
    assert exc.value.errno == EINVAL


def test_entity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.readdir("/@tag1/entity2", None)
    assert exc.value.errno == EINVAL
