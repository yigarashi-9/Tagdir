from errno import ENOENT

import pytest

from .conftest import setup_tagdir_test
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Attr, Entity, Tag


def setup_func(session):
    attr1 = Attr.new_tag_attr()
    attr2 = Attr.new_tag_attr()
    tag1 = Tag("tag1", attr1)
    tag2 = Tag("tag2", attr2)
    attr3 = Attr.new_entity_attr()
    attr4 = Attr.new_entity_attr()
    entity1 = Entity("entity1", attr3, "/path1", [tag1, tag2])
    entity2 = Entity("entity2", attr4, "/path2", [])
    session.add_all([attr1, attr2, attr3, attr4, tag1, tag2, entity1, entity2])


# Dynamically define tagdir fixture
setup_tagdir_test(setup_func)


def test_root(tagdir):
    expected = Attr.get_root_attr(tagdir.session).as_dict()
    assert tagdir.getattr(tagdir.session, "/") == expected


def test_existent_tag1(tagdir):
    expected = Tag.get_by_name(tagdir.session, "tag1").attr.as_dict()
    assert tagdir.getattr(tagdir.session, "/@tag1") == expected


def test_existent_entity1(tagdir):
    expected = Entity.get_by_name(tagdir.session, "entity1").attr.as_dict()
    assert tagdir.getattr(tagdir.session, "/@tag1/entity1") == expected


def test_existent_entity2(tagdir):
    expected = Entity.get_by_name(tagdir.session, "entity1").attr.as_dict()
    assert tagdir.getattr(tagdir.session, "/@tag1/@tag2/entity1") == expected


def test_nonexistent_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.getattr(tagdir.session, "/@non_tag")
    assert exc.value.errno == ENOENT


def test_no_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.getattr(tagdir.session, "/entity1")
    assert exc.value.errno == ENOENT


def test_notag_entity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.getattr(tagdir.session, "/@tag1/entity2")
    assert exc.value.errno == ENOENT
