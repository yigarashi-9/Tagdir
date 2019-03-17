from errno import EINVAL

import pytest
from sqlalchemy.orm.exc import NoResultFound

from .conftest import setup_tagdir_test
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Attr, Tag


name_tag_1 = "tag1"
name_tag_2 = "tag2"
name_tag_3 = "tag3"


def setup_func(session):
    attr1 = Attr.new_tag_attr()
    tag1 = Tag(name_tag_1, attr1)
    session.add_all([attr1, tag1])


# Dynamically define tagdir fixture
setup_tagdir_test(setup_func)


def test_normal1(tagdir):
    assert tagdir.mkdir(tagdir.session, "/@" + name_tag_2) is None
    try:
        Tag.get_by_name(tagdir.session, name_tag_1)
        Tag.get_by_name(tagdir.session, name_tag_2)
    except NoResultFound:
        pytest.fail("Unexpected NoResultFound error")


def test_normal2(tagdir):
    assert tagdir.mkdir(
        tagdir.session,
        "/@" + name_tag_1 + "/@" + name_tag_2 + "/@" + name_tag_3) is None
    try:
        Tag.get_by_name(tagdir.session, name_tag_1)
        Tag.get_by_name(tagdir.session, name_tag_2)
        Tag.get_by_name(tagdir.session, name_tag_3)
    except NoResultFound:
        pytest.fail("Unexpected NoResultFound error")


def test_invalid_root(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.mkdir(tagdir.session, "/")
    assert exc.value.errno == EINVAL


def test_invalid_notag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.mkdir(tagdir.session, "/entity")
    assert exc.value.errno == EINVAL


def test_invalid_entity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.mkdir(tagdir.session, "/@tag_1/entity")
    assert exc.value.errno == EINVAL
