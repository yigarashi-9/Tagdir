from errno import ENODATA

import pytest

from tagdir import ENTINFO_PATH
from .conftest import setup_tagdir_test
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Attr, Entity, Tag


def setup_func(session):
    attr1 = Attr.new_tag_attr()
    attr2 = Attr.new_tag_attr()
    tag1 = Tag("tag1", attr1)
    tag2 = Tag("tag2", attr2)
    attr3 = Attr.new_entity_attr()
    entity1 = Entity("entity1", attr3, "/path1", [tag1, tag2])
    session.add_all([attr1, attr2, attr3, tag1, tag2, entity1])


# Dynamically define tagdir fixture
setup_tagdir_test(setup_func)


def test_normal(tagdir):
    expected = b"/path1,tag1,tag2"
    assert tagdir.getxattr(ENTINFO_PATH, "entity1") == expected


def test_nonexistent_entity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        tagdir.getxattr(ENTINFO_PATH, "nonexistent")
    assert exc.value.errno == ENODATA


@pytest.mark.parametrize("input", [
    "/",
    "/@tag1/@tag2",
    "/@tag1/entity1",
    "/@non_tag",
])
def test_invalid_path(tagdir, input):
    with pytest.raises(FuseOSError) as exc:
        tagdir.getxattr(input, "fail")

    # Import after mocking
    from tagdir.fusepy.fuse import ENOTSUP
    assert exc.value.errno == ENOTSUP
