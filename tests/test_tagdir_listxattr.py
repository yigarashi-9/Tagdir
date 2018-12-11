import pytest

from .conftest import setup_tagdir_test
from tagdir.tagdir import ENTINFO_PATH
from tagdir.fusepy.fuse import ENOTSUP
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Attr, Entity


def setup_func(session):
    attr1 = Attr.new_entity_attr()
    attr2 = Attr.new_entity_attr()
    entity1 = Entity("entity1", attr1, "/path1", [])
    entity2 = Entity("entity2", attr2, "/path2", [])
    session.add_all([attr1, attr2, entity1, entity2])


# Dynamically define tagdir fixtures
setup_tagdir_test(setup_func, "listxattr")


def test_correct(tagdir):
    expected = ["entity1", "entity2"]
    assert tagdir.listxattr(ENTINFO_PATH) == expected


@pytest.mark.parametrize("input", [
    "/",
    "/@tag1/@tag2",
    "/@tag1/entity1",
    "/@non_tag",
])
def test_invalid_path(tagdir, input):
    with pytest.raises(FuseOSError) as exc:
        tagdir.getxattr(input, "fail")
    assert exc.value.errno == ENOTSUP
