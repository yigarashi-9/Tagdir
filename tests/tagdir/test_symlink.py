from errno import EINVAL, ENOENT, ENOTDIR
import pathlib

import pytest

from .conftest import setup_tagdir_test
from tagdir.fusepy.exceptions import FuseOSError
from tagdir.models import Attr, Entity, Tag
from tagdir.watch import EntityPathChangeObserver


def setup_func(session):
    attr1 = Attr.new_tag_attr()
    attr2 = Attr.new_tag_attr()
    tag1 = Tag("tag1", attr1)
    tag2 = Tag("tag2", attr2)
    attr3 = Attr.new_entity_attr()
    entity1 = Entity("entity1", attr3, "/path/to/entity1", [])
    session.add_all([attr1, attr2, tag1, tag2, attr3, entity1])


# Dynamically define tagdir fixture
setup_tagdir_test(setup_func)


@pytest.fixture(autouse=True)
def observer_mock(mocker):
    init_mock = mocker.patch.object(EntityPathChangeObserver, "__init__")
    init_mock.return_value = None
    return init_mock


@pytest.fixture(autouse=True)
def schedule_if_new_path_mock(mocker):
    return mocker.patch.object(EntityPathChangeObserver,
                               "schedule_if_new_path")


@pytest.fixture(autouse=True)
def is_dir_mock(mocker):
    is_dir_mock = mocker.patch.object(pathlib.Path, "is_dir")
    is_dir_mock.return_value = True
    return is_dir_mock


@pytest.fixture(autouse=True)
def exists_mock(mocker):
    exists_mock = mocker.patch.object(pathlib.Path, "exists")
    exists_mock.return_value = True
    return exists_mock


def test_link_tags(tagdir, schedule_if_new_path_mock):
    entity_name = "entity"
    source = "/path/" + entity_name
    assert tagdir.symlink("/@tag1/@tag2/" + entity_name, source) is None

    entity = Entity.get_by_name(tagdir.session, "entity")
    tag1 = Tag.get_by_name(tagdir.session, "tag1")
    tag2 = Tag.get_by_name(tagdir.session, "tag2")
    assert entity.has_tags([tag1, tag2])
    assert entity.path == source
    schedule_if_new_path_mock.assert_called_with(source)


def test_root(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.symlink("/", "")
    assert exc.value.errno == EINVAL


def test_notags(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.symlink("/entity", "/path/entity")
    assert exc.value.errno == EINVAL


def test_nonexistent_tag(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.symlink("/@tag3/entity", "/path/entity")
    assert exc.value.errno == ENOENT


def test_nonexistent_source(tagdir, exists_mock):
    exists_mock.return_value = False
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.symlink("/@tag3", "/path/entity")
    assert exc.value.errno == ENOENT


def test_strange_target(tagdir):
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.symlink("/@tag1/entity_strange", "path/entity")
    assert exc.value.errno == EINVAL


def test_notdir_source(tagdir, is_dir_mock):
    is_dir_mock.return_value = False
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.symlink("/@tag1/entity.txt", "/path/entity.txt")
    assert exc.value.errno == ENOTDIR


def test_invalid_source(tagdir):
    """
    This test tries to create "entity1" referring to /path/entity1, but
    "entity1" already exists, referring to /path/to/entity1.
    """
    with pytest.raises(FuseOSError) as exc:
        assert tagdir.symlink("/@tag1/entity1", "/path/entity1")
    assert exc.value.errno == EINVAL
