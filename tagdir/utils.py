import pathlib
from typing import List, Optional, Tuple


class InvalidPath(Exception):
    # This error should not occur
    pass


def parse_path(raw_path: str) -> Tuple[List[str], Optional[str]]:
    """
    Pre-condition: s[0] == "/"
    Expected form of path: /@tag_1/.../@tag_n/(ent_name)?
    """
    tag_names = []
    ent_name = None

    parts = pathlib.Path(raw_path).parts[1:]
    index = 0

    for part in parts:
        if part[0] == "@":
            tag_names.append(part[1:])
            index += 1
        else:
            break

    rest = parts[index:]

    if len(rest) > 1:
        raise InvalidPath

    if len(rest) == 1:
        ent_name = rest[0]

    return tag_names, ent_name
