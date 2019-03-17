from tagdir.tagdir import encode_path, parse_path, parse_path_for_tagging


# Tests for parse_path
def test_threesome():
    arg = "/@python/@test/tagdir/rest_path"
    tag_names, ent_name, rest_path = parse_path(arg)
    assert tag_names == ["python", "test"]
    assert ent_name == "tagdir"
    assert rest_path == "rest_path"


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


# Tests for parse_path_for_tagging
def test_normal():
    source = "/path/to/source"
    arg = "/@python/" + encode_path(source)
    tag_names, source_ret = parse_path_for_tagging(arg)
    assert tag_names == ["python"]
    assert source == source_ret


def test_no_delimiter():
    arg = "/@python/invalid/argument"
    tag_names, source = parse_path_for_tagging(arg)
    assert not tag_names
    assert source is None


def test_non_absolute_source_path():
    source = "non/absolute/path"
    arg = "/@python/" + encode_path(source)
    tag_names, source_ret = parse_path_for_tagging(arg)
    assert not tag_names
    assert source_ret is None
