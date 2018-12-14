from errno import EINVAL, ENOENT

import pytest
from sqlalchemy.orm.exc import NoResultFound

from .conftest import setup_tagdir_test
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Attr, Entity, Tag
from tagdir.watch import EntityPathChangeObserver


def setup_func(session):
    attr1 = Attr.new_tag_attr()
    attr2 = Attr.new_tag_attr()
    attr3 = Attr.new_tag_attr()
    tag1 = Tag("tag1", attr1)
    tag2 = Tag("tag2", attr2)
    tag3 = Tag("tag3", attr3)
    attr4 = Attr.new_entity_attr()
    entity1 = Entity("entity1", attr4, "/path1", [tag1, tag2])
    session.add_all([attr1, attr2, attr3, attr4, tag1, tag2, tag3, entity1])


# Dynamically define tagdir fixture
setup_tagdir_test(setup_func)


@pytest.fixture(autouse=True)
def observer_mock(mocker):
    init_mock = mocker.patch.object(EntityPathChangeObserver, "__init__")
    init_mock.return_value = None
    return init_mock


@pytest.fixture(autouse=True)
def unschedule_mock(mocker):
    return mocker.patch.object(EntityPathChangeObserver,
                               "unschedule_redundant_handlers")


def test_unlink_tag(tagdir):
    assert tagdir.unlink("/@tag1/entity1") is None
    tag1 = Tag.get_by_name(tagdir.session, "tag1")
    entity1 = Entity.get_by_name(tagdir.session, "entity1")
    assert tag1 not in entity1.tags


def test_unlink_all_tags(tagdir, unschedule_mock):
    assert tagdir.unlink("/@tag1/@tag2/entity1") is None
    with pytest.raises(NoResultFound):
        Entity.get_by_name(tagdir.session, "entity1")
    unschedule_mock.assert_called()


def test_root(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.unlink("/")
    assert exc.value.errno == EINVAL


def test_notags(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.unlink("/entity1")
    assert exc.value.errno == EINVAL


def test_noentity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.unlink("/@tag1/@tag2")
    assert exc.value.errno == EINVAL


def test_nonexistent_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.unlink("/@tag3/entity1")
    assert exc.value.errno == ENOENT


def test_nonexistent_entity(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.unlink("/@tag1/entity2")
    assert exc.value.errno == ENOENT


def test_invalid_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.unlink("/@tag3/entity1")
    assert exc.value.errno == ENOENT
