from errno import EINVAL, ENOENT

import pytest
from sqlalchemy.orm.exc import NoResultFound

from .conftest import setup_tagdir_test
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Attr, Tag


name_tag_1 = "tag1"
name_tag_2 = "tag2"


def setup_func(session):
    attr1 = Attr.new_tag_attr()
    attr2 = Attr.new_tag_attr()
    tag1 = Tag("tag1", attr1)
    tag2 = Tag("tag2", attr2)
    session.add_all([attr1, attr2, tag1, tag2])


# Dynamically define tagdir fixture
setup_tagdir_test(setup_func)


def test_normal1(tagdir):
    assert tagdir.rmdir("/@" + name_tag_1) is None
    with pytest.raises(NoResultFound):
        Tag.get_by_name(tagdir.session, name_tag_1)
    assert Tag.get_by_name(tagdir.session, name_tag_2).name == name_tag_2


def test_normal2(tagdir):
    assert tagdir.rmdir("/@" + name_tag_1 + "/@" + name_tag_2) is None
    with pytest.raises(NoResultFound):
        Tag.get_by_name(tagdir.session, name_tag_1)
    with pytest.raises(NoResultFound):
        Tag.get_by_name(tagdir.session, name_tag_2)


def test_invalid_root(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.rmdir("/")
    assert exc.value.errno == EINVAL


def test_invalid_notag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.rmdir("/entity")
    assert exc.value.errno == EINVAL


def test_nonexistent_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.rmdir("/@tag_3")
    assert exc.value.errno == ENOENT
