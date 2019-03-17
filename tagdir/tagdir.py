from errno import EINVAL, ENODATA, ENOENT, ENOTDIR
import logging
import os
from os.path import join
import pathlib
from typing import cast, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound

from . import ENTINFO_PATH
from .db import session_scope
from .fusepy.fuse import ENOTSUP
from .fusepy.exceptions import FuseOSError
from .fusepy.loopback import Loopback
from .models import Attr, Entity, Tag
from .watch import EntityPathChangeObserver


DELIMITER = "%%"


def encode_path(path):
    return path.replace("/", DELIMITER)


def decode_path(path):
    return path.replace(DELIMITER, "/")


class DebugFilter(logging.Filter):
    def __init__(self) -> None:
        self.id = 1

    def filter(self, record):
        if record.levelno == logging.DEBUG:
            record.id = self.id
            self.id += 1
            return True
        else:
            return False


def tagdir_debug_handler() -> logging.Handler:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    debug_formatter = logging.Formatter(
        "%(levelname)s:%(name)s:(%(id)d) %(op)s "
        "%(path)s %(arguments)s %(message)s")
    ch.setFormatter(debug_formatter)
    ch.addFilter(DebugFilter())
    return ch


def parse_path(raw_path: str) -> \
        Tuple[List[str], Optional[str], Optional[str]]:
    """
    Pre-condition: s[0] == "/"
    Expected form of path: /@tag_1/.../@tag_n/(ent_name)?/(rest_path)?
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

    if len(rest) >= 1:
        ent_name = rest[0]

    if len(rest) >= 2:
        rest_path = join(*rest[1:])

    return tag_names, ent_name, rest_path


def parse_path_for_tagging(raw_path: str) -> Tuple[List[str], Optional[str]]:
    # split at the first delimiter
    splited_path = raw_path.split(DELIMITER, 1)

    if len(splited_path) != 2:
        return [], None

    source = decode_path(DELIMITER + splited_path[1])

    tag_names = []
    parts = pathlib.Path(splited_path[0]).parts[1:]

    for part in parts:
        if part[0] == "@":
            tag_names.append(part[1:])
        else:
            return [], None

    return tag_names, source


class Tagdir(Loopback):
    def __init__(self):
        logger = logging.getLogger(__name__)
        logger.propagate = False
        logger.addHandler(tagdir_debug_handler())
        self.logger = logger

        with session_scope() as session:
            # Create root attr
            root_attr = Attr.get_root_attr(session)
            if not root_attr:
                session.add(Attr.new_root_attr())

        super().__init__()

    def __call__(self, op, path, *args):
        extra = {"op": str(op), "path": str(path), "arguments": repr(args)}
        self.logger.debug("", extra=extra)

        with session_scope() as session:
            self.session = session

            # Operations specific to tagdir
            if op in Tagdir.__dict__:
                return getattr(self, op)(path, *args)

            # Meaningless operations
            if op not in Loopback.__dict__:
                return super().__call__(op, path, *args)

            # Pass through
            tag_names, ent_name, rest_path = parse_path(path)

            if not tag_names:
                raise FuseOSError(ENOENT)

            try:
                tags = [Tag.get_by_name(session, tag_name)
                        for tag_name in tag_names]
            except NoResultFound:
                raise FuseOSError(ENOENT)

            if ent_name is None:
                raise FuseOSError(ENOENT)

            entity = Entity.get_if_valid(session, ent_name, tags)
            if entity is None:
                raise FuseOSError(ENOENT)

            # TODO: Investigate whether pass through is appropriate
            path = entity.path
            if rest_path is not None:
                path = join(path, rest_path)
            return super().__call__(op, path, *args)

    def access(self, path, mode):
        # TODO: change st_atim
        if path in ["/", ENTINFO_PATH]:
            return 0

        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names:
            raise FuseOSError(ENOENT)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            return 0

        entity = Entity.get_if_valid(self.session, ent_name, tags)

        if entity is None:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            return 0
        else:
            return super().access(join(entity.path, rest_path), mode)

    def getattr(self, path, fh=None):
        """
        Return an attr dict corresponding to the path
        """
        if path == "/":
            return Attr.get_root_attr(self.session).as_dict()

        if path == ENTINFO_PATH:
            return Attr.new_dummy_attr().as_dict()

        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names:
            raise FuseOSError(ENOENT)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            return tags[-1].attr.as_dict()

        entity = Entity.get_if_valid(self.session, ent_name, tags)

        if entity is None:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            # Return attribute for an entity
            return entity.attr.as_dict()
        else:
            return super().getattr(join(entity.path, rest_path), fh)

    def getxattr(self, path, name, position=0):
        # TODO: Implement pass through
        if path != ENTINFO_PATH:
            raise FuseOSError(ENOTSUP)

        try:
            entity = Entity.get_by_name(self.session, name)
        except NoResultFound:
            raise FuseOSError(ENODATA)

        reslist = [entity.path] + [tag.name for tag in entity.tags]
        return bytes(",".join(reslist), "utf-8")

    def listxattr(self, path):
        # TODO: Implement pass through
        if path != ENTINFO_PATH:
            raise FuseOSError(ENOTSUP)
        return [s for s, in self.session.query(Entity.name)]

    def mkdir(self, path, mode=0o777):
        """
        Create tags if path is /@tag_1/.../@tag_n,
        otherwise raise error.
        """

        tag_names, source = parse_path_for_tagging(path)

        # Do tagging
        if source:
            try:
                tags = [Tag.get_by_name(self.session, tag_name)
                        for tag_name in tag_names]
            except NoResultFound:
                raise FuseOSError(ENOENT)

            source = pathlib.Path(source)

            if not source.exists():
                raise FuseOSError(ENOENT)

            if not source.is_dir():
                raise FuseOSError(ENOTDIR)

            try:
                entity = Entity.get_by_name(self.session, source.name)
                if entity.path != str(source):
                    # Cannot create multiple links for one directory
                    raise FuseOSError(EINVAL)
            except NoResultFound:
                attr = Attr.new_entity_attr()
                entity = Entity(source.name, attr, str(source), [])
                observer = EntityPathChangeObserver.get_instance()
                observer.schedule_if_new_path(entity.path)
                self.session.add_all([entity, attr])

            for tag in tags:
                if tag not in entity.tags:
                    entity.tags.append(tag)
            return None

        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names or (ent_name is not None and rest_path is None):
            # Cannot create a directory
            raise FuseOSError(EINVAL)

        # Create new tags
        if ent_name is None:
            for tag_name in tag_names:
                try:
                    Tag.get_by_name(self.session, tag_name)
                except NoResultFound:
                    attr = Attr.new_tag_attr()
                    tag = Tag(tag_name, attr)
                    self.session.add_all([tag, attr])
            return None

        # Pass through
        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        entity = Entity.get_if_valid(self.session, ent_name, tags)
        if entity is None:
            raise FuseOSError(ENOENT)

        _rest_path = cast(pathlib.Path, rest_path)  # Never be None
        return super().mkdir(join(entity.path, _rest_path), mode=mode)

    def rmdir(self, path):
        """
        Remove @tag_1, ..., @tag_n.
        """
        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names:
            raise FuseOSError(EINVAL)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        # Remove tags
        if ent_name is None:
            for tag in tags:
                tag.remove(self.session)
            return None

        # Untagging
        if rest_path is None:
            entity = Entity.get_if_valid(self.session, ent_name, tags)
            if entity is None:
                raise FuseOSError(ENOENT)

            for tag in tags:
                entity.tags.remove(tag)

            if not entity.tags:
                self.session.delete(entity)
                observer = EntityPathChangeObserver.get_instance()
                observer.unschedule_redundant_handlers()
            return None

        # Pass through
        entity = Entity.get_if_valid(self.session, ent_name, tags)
        if entity is None:
            raise FuseOSError(ENOENT)

        _rest_path = cast(pathlib.Path, rest_path)
        return super().rmdir(join(entity.path, _rest_path))

    def readdir(self, path, fh):
        """
        If path is
        - /, then list all tags,
        - /@tag_1/../@tag_n, then list all entities filtered by the tags.
        """
        if path == "/":
            return ["@" + name[0] for name in self.session.query(Tag.name)]

        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names:
            raise FuseOSError(EINVAL)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        # Filter entity by tags
        if ent_name is None:
            tag_names = [tag.name for tag in tags]
            res = self.session.query(Entity.name).join(Entity.tags)\
                .filter(Tag.name.in_(tag_names))\
                .group_by(Entity.name)\
                .having(func.count(Entity.name) == len(tag_names))
            return [e for e, in res]

        # Pass through
        _ent_name = cast(str, ent_name)  # Never be None
        entity = Entity.get_if_valid(self.session, _ent_name, tags)
        if entity is None:
            raise FuseOSError(ENOENT)

        path = entity.path
        if rest_path:
            path = join(path, rest_path)
        return super().readdir(path, fh)

    def statfs(self, path):
        """
        It seems that this function is called only for "/" in normal use.
        Probably, we have to implement much more to deal with general use case
        of the statfs syscall.
        """
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in (
            'f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail',
            'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'))
