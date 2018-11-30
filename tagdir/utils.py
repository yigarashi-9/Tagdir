import pathlib
from typing import List


def parse_path(raw_path: str) -> (List[str], str, pathlib.Path):
    """
    Pre-condition: s[0] == "/"
    Expected form of path: /@tag_1/.../@tag_n/ent_name/rest_path
    """
    tag_names = []
    ent_name = None
    rest_path = None

    parts = pathlib.Path(raw_path).parts[1:]
    index = 0

    for part in parts:
        if part[0] == "@":
            tag_names.append(part[1:])
            index += 1
        else:
            break

    rest = parts[index:]

    if len(rest) > 0:
        ent_name = rest[0]

    if len(rest) > 1:
        rest_path = pathlib.Path('/'.join(rest[1:]))

    return tag_names, ent_name, rest_path
