from tagdir.utils import parse_path


def test_twosome():
    arg = "/@python/@test/tagdir"
    tag_names, ent_name = parse_path(arg)
    assert tag_names == ["python", "test"]
    assert ent_name == "tagdir"


def test_only_tags():
    arg = "/@python/@test"
    tag_names, ent_name = parse_path(arg)
    assert tag_names == ["python", "test"]
    assert ent_name is None


def test_root():
    arg = "/"
    tag_names, ent_name = parse_path(arg)
    assert not tag_names
    assert ent_name is None
