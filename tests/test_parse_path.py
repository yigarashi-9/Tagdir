import pathlib

from tagdir.utils import parse_path


class TestParsePath:
    def test_threesome(self):
        arg = "/@python/@test/tagdir/test/foo.txt"
        raw_tags, ent_name, rest_path = parse_path(arg)
        assert raw_tags == ["python", "test"]
        assert ent_name == "tagdir"
        assert rest_path == pathlib.Path("test/foo.txt")

    def test_twosome(self):
        arg = "/@python/@test/tagdir"
        raw_tags, ent_name, rest_path = parse_path(arg)
        assert raw_tags == ["python", "test"]
        assert ent_name == "tagdir"
        assert rest_path is None

    def test_only_tags(self):
        arg = "/@python/@test"
        raw_tags, ent_name, rest_path = parse_path(arg)
        assert raw_tags == ["python", "test"]
        assert ent_name is None
        assert rest_path is None

    def test_root(self):
        arg = "/"
        raw_tags, ent_name, rest_path = parse_path(arg)
        assert not raw_tags
        assert ent_name is None
        assert rest_path is None
