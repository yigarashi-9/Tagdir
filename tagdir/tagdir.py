from errno import EINVAL, ENODATA, ENOENT, ENOSYS, ENOTDIR
import logging
import pathlib

from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound

from .db import session_scope
from .fusepy.fuse import ENOTSUP, Operations
from .fusepy.exceptions import FuseOSError
from .logging import tagdir_debug_handler
from .models import Attr, Entity, Tag
from .utils import parse_path
from .watch import EntityPathChangeObserver


ENTINFO_PATH = "/.entinfo"


class Tagdir(Operations):
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
            if hasattr(self, op):
                # Operations specific to tagdir
                self.session = session
                return getattr(self, op)(path, *args)
            else:
                # In most cases, raise error for unsupported operations
                # return super().__call__(op, path, *args)
                raise FuseOSError(ENOSYS)

    def access(self, path, mode):
        # TODO: change st_atim
        if path in ["/", ENTINFO_PATH]:
            return 0

        tag_names, ent_name = parse_path(path)

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

        if entity:
            return 0
        else:
            raise FuseOSError(ENOENT)

    def getattr(self, path, fh=None):
        """
        Return an attr dict corresponding to the path
        """
        if path == "/":
            return Attr.get_root_attr(self.session).as_dict()

        if path == ENTINFO_PATH:
            return Attr.new_dummy_attr().as_dict()

        tag_names, ent_name = parse_path(path)

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

        if entity:
            return entity.attr.as_dict()
        else:
            raise FuseOSError(ENOENT)

    def getxattr(self, path, name, position=0):
        if path != ENTINFO_PATH:
            raise FuseOSError(ENOTSUP)

        try:
            entity = Entity.get_by_name(self.session, name)
        except NoResultFound:
            raise FuseOSError(ENODATA)

        reslist = [entity.path] + [tag.name for tag in entity.tags]
        return bytes(",".join(reslist), "utf-8")

    def listxattr(self, path):
        if path != ENTINFO_PATH:
            raise FuseOSError(ENOTSUP)
        return [s for s, in self.session.query(Entity.name)]

    def mkdir(self, path, mode=0o777):
        """
        Create tags if path is /@tag_1/.../@tag_n,
        otherwise raise error.
        """
        tag_names, ent_name = parse_path(path)

        if not tag_names or ent_name is not None:
            # Cannot create a directory
            raise FuseOSError(EINVAL)

        # Create new tags
        for tag_name in tag_names:
            try:
                Tag.get_by_name(self.session, tag_name)
            except NoResultFound:
                attr = Attr.new_tag_attr()
                tag = Tag(tag_name, attr)
                self.session.add_all([tag, attr])
        return None

    def rmdir(self, path):
        """
        Remove @tag_1, ..., @tag_n.
        """
        tag_names, ent_name = parse_path(path)

        if not tag_names or ent_name is not None:
            raise FuseOSError(EINVAL)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        # Remove tags
        for tag in tags:
            tag.remove(self.session)
        return None

    def symlink(self, target, source):
        """
        If target is /@tag_1/.../@tag_n/ent_name and the entity is a directory,
        - Create entity if the entity does not exists
        - Add tag_1 .. tag_n to the entity
        """
        tag_names, ent_name = parse_path(target)

        if not tag_names:
            raise FuseOSError(EINVAL)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            raise FuseOSError(EINVAL)

        # Do tagging
        source = pathlib.Path(source).resolve()

        if source.name != ent_name:
            raise FuseOSError(EINVAL)

        if not source.exists():
            raise FuseOSError(ENOENT)

        if not source.is_dir():
            raise FuseOSError(ENOTDIR)

        try:
            entity = Entity.get_by_name(self.session, ent_name)
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

    def unlink(self, path):
        """
        If path is /@tag_1/.../@tag_n/ent_name, remove tag_1 .. tag_n from the
        entity.
        """
        tag_names, ent_name = parse_path(path)

        if not tag_names:
            raise FuseOSError(ENOENT)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            raise FuseOSError(EINVAL)

        # Do untagging
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

    def readlink(self, path):
        tag_names, ent_name = parse_path(path)

        if not tag_names:
            raise FuseOSError(ENOENT)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            raise FuseOSError(EINVAL)

        entity = Entity.get_if_valid(self.session, ent_name, tags)
        if entity is None:
            raise FuseOSError(ENOENT)

        return entity.path

    def readdir(self, path, fh):
        """
        If path is
        - /, then list all tags,
        - /@tag_1/../@tag_n, then list all entities filtered by the tags.
        """
        if path == "/":
            return ["@" + name[0] for name in self.session.query(Tag.name)]

        tag_names, ent_name = parse_path(path)

        if not tag_names:
            raise FuseOSError(ENOENT)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is not None:
            raise FuseOSError(EINVAL)

        # Filter entity by tags
        tag_names = [tag.name for tag in tags]
        res = self.session.query(Entity.name).join(Entity.tags)\
            .filter(Tag.name.in_(tag_names))\
            .group_by(Entity.name)\
            .having(func.count(Entity.name) == len(tag_names))
        return [e for e, in res]
