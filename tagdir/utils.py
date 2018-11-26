import pathlib
from typing import List

from sqlalchemy.orm.exc import NoResultFound

from .models import Entity


def parse_path(s) -> (List[str], str, pathlib.Path):
    if s == "/":
        return [], None, None

    s = s[1:]  # remove head "/"

    tag_strs = []
    ent_name = None
    rest_path = None

    ss = s.split("/")
    index = 0

    for s in ss:
        if len(s) > 0 and s[0] == "@":
            tag_strs.append(s[1:])
            index += 1
        else:
            break

    ss = ss[index:]

    if len(ss) > 0:
        ent_name = ss[0]

    if len(ss) > 1:
        rest_path = pathlib.Path('/'.join(ss[1:]))

    return tag_strs, ent_name, rest_path


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
