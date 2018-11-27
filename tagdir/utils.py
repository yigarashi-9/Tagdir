import pathlib
from typing import List

from sqlalchemy.orm.exc import NoResultFound

from .models import Entity


def parse_path(raw_path: str) -> (List[str], str, pathlib.Path):
    """
    Pre-condition: s[0] == "/"
    Expected form of path: /@tag_1/.../@tag_n/ent_name/rest_path
    """
    raw_tags = []
    ent_name = None
    rest_path = None

    parts = pathlib.Path(raw_path).parts[1:]
    index = 0

    for part in parts:
        if part[0] == "@":
            raw_tags.append(part[1:])
            index += 1
        else:
            break

    rest = parts[index:]

    if len(rest) > 0:
        ent_name = rest[0]

    if len(rest) > 1:
        rest_path = pathlib.Path('/'.join(rest[1:]))

    return raw_tags, ent_name, rest_path


def prepare_passthrough(session, ent_name: str, rest_path: pathlib.Path, tags):
    try:
        entity = session.query(Entity).filter(Entity.name == ent_name).one()
    except NoResultFound:
        return None

    for tag in tags:
        if tag not in entity.tags:
            return None

    path = pathlib.Path(entity.path)
    if rest_path is not None:
        path = path / rest_path

    return path
