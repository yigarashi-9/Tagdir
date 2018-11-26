import pathlib
import unittest

from tagdir.utils import parse_path


class TestParsePath(unittest.TestCase):
    def test_parse_path_1(self):
        tag_strs, ent_name, rest_path = \
            parse_path("/@python/@test/tagdir/test/")
        self.assertEqual(tag_strs, ["python", "test"])
        self.assertEqual(ent_name, "tagdir")
        self.assertEqual(rest_path, pathlib.Path("test"))
