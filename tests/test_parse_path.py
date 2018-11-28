import pathlib

from tagdir.utils import parse_path


def test_threesome():
    arg = "/@python/@test/tagdir/test/foo.txt"
    tag_names, ent_name, rest_path = parse_path(arg)
    assert tag_names == ["python", "test"]
    assert ent_name == "tagdir"
    assert rest_path == pathlib.Path("test/foo.txt")


def test_twosome():
    arg = "/@python/@test/tagdir"
    tag_names, ent_name, rest_path = parse_path(arg)
    assert tag_names == ["python", "test"]
    assert ent_name == "tagdir"
    assert rest_path is None


def test_only_tags():
    arg = "/@python/@test"
    tag_names, ent_name, rest_path = parse_path(arg)
    assert tag_names == ["python", "test"]
    assert ent_name is None
    assert rest_path is None


def test_root():
    arg = "/"
    tag_names, ent_name, rest_path = parse_path(arg)
    assert not tag_names
    assert ent_name is None
    assert rest_path is None
