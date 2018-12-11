from errno import EINVAL

import pytest

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


# Dynamically define tagdir fixtures
setup_tagdir_test(setup_func, "mkdir")


def test_normal1(tagdir):
    assert tagdir.mkdir("/@" + name_tag_2) is None
    assert Tag.get_by_name(tagdir.session, name_tag_1).name == name_tag_1
    assert Tag.get_by_name(tagdir.session, name_tag_2).name == name_tag_2


def test_normal2(tagdir):
    assert tagdir.mkdir(
        "/@" + name_tag_1 + "/@" + name_tag_2 + "/@" + name_tag_3) is None
    assert Tag.get_by_name(tagdir.session, name_tag_1).name == name_tag_1
    assert Tag.get_by_name(tagdir.session, name_tag_2).name == name_tag_2
    assert Tag.get_by_name(tagdir.session, name_tag_3).name == name_tag_3


def test_invalid_root(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.mkdir("/")
    assert exc.value.errno == EINVAL


def test_invalid_notag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.mkdir("/entity")
    assert exc.value.errno == EINVAL


def test_invalid_entity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.mkdir("/@tag_1/entity")
    assert exc.value.errno == EINVAL
