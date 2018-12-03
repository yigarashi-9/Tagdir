from errno import EINVAL, ENOENT
import logging
from os.path import join
import pathlib

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from .db import session_scope
from .fusepy.exceptions import FuseOSError
from .fusepy.loopback import Loopback
from .logging import tagdir_debug_handler
from .models import Attr, Base, Entity, Tag
from .utils import parse_path


class Tagdir(Loopback):
    def __init__(self, engine):
        logger = logging.getLogger(__name__)
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        logger.addHandler(tagdir_debug_handler())
        self.logger = logger

        Base.metadata.create_all(engine)
        self.Session = sessionmaker(bind=engine)

        with session_scope(self.Session) as session:
            # Create root attr
            root_attr = Attr.get_root_attr(session)
            if not root_attr:
                session.add(Attr.new_root_attr())

        super().__init__()

    def __call__(self, op, path, *args):
        extra = {"op": str(op), "path": str(path), "arguments": repr(args)}
        self.logger.debug("", extra=extra)

        with session_scope(self.Session) as session:
            if hasattr(self, op):
                # Operations specific to tagdir
                self.session = session
                return getattr(self, op)(path, *args)

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

            entity = Entity.get_if_valid(self.session, ent_name, tags)
            if entity is None:
                raise FuseOSError(ENOENT)

            # TODO: Investigate whether pass through is appropriate
            path = entity.path
            if rest_path is not None:
                path = join(path, rest_path)
            return super().__call__(op, path, *args)

    def access(self, path, mode):
        # TODO: change st_atim
        if path == "/":
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

        # Pass through
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

    def mkdir(self, path, mode):
        """
        Create tags if path is /@tag_1/.../@tag_n,
        otherwise raise error or pass through.
        """
        # TODO: It may be OK to assume that path is checked by access
        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names or (ent_name is not None and rest_path is None):
            # Cannot make a directory
            # TODO: Is ENOENT suitable?
            raise FuseOSError(ENOENT)

        if ent_name is None:
            # Create new tags
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

        return super().mkdir(join(entity.path, rest_path), mode)

    def rmdir(self, path):
        """
        Remove tags if path is /@tag_1/.../@tag_n,
        otherwise raise error or pass through.
        """
        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names or (ent_name is not None and rest_path is None):
            # Cannot remove a directory
            # TODO: Is ENOENT suitable?
            raise FuseOSError(ENOENT)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            # Remove tags
            for tag in tags:
                tag.remove(self.session)
            return None

        # Pass through
        entity = Entity.get_if_valid(self.session, ent_name, tags)
        if entity is None:
            raise FuseOSError(ENOENT)

        return super().rmdir(join(entity.path, rest_path))

    def symlink(self, target, source):
        """
        If target is /@tag_1/.../@tag_n/ent_name and the entity is a directory,
        - Create entity if the entity does not exists
        - Add tag_1 .. tag_n to the entity
        """
        tag_names, ent_name, rest_path = parse_path(target)

        if not tag_names or ent_name is None:
            raise FuseOSError(EINVAL)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            # Do tagging
            source = pathlib.Path(source).resolve()

            if source.name != ent_name \
                    or not source.is_absolute()\
                    or not source.is_dir():
                raise FuseOSError(EINVAL)

            try:
                entity = Entity.get_by_name(self.session, ent_name)
            except NoResultFound:
                attr = Attr.new_entity_attr()
                entity = Entity(source.name, attr, str(source), [])
                self.session.add_all([entity, attr])

            if entity.path != str(source):
                # Buggy state: multiple paths for one entity
                # TODO: Is EINVAL suitable?
                raise FuseOSError(EINVAL)

            for tag in tags:
                if tag not in entity.tags:
                    entity.tags.append(tag)
            return None

        # Pass through
        entity = Entity.get_if_valid(self.session, ent_name, tags)
        if entity is None:
            raise FuseOSError(ENOENT)

        return super().symlink(join(entity.path, rest_path), source)

    def unlink(self, path):
        """
        If path is /@tag_1/.../@tag_n/ent_name, remove tag_1 .. tag_n from the
        entity.
        """
        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names or ent_name is None:
            raise FuseOSError(EINVAL)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            # Do untagging
            try:
                entity = Entity.get_by_name(self.session, ent_name)
            except NoResultFound:
                raise FuseOSError(ENOENT)

            if not entity.has_tags(tags):
                raise FuseOSError(ENOENT)

            for tag in tags:
                entity.tags.remove(tag)

            if not entity.tags:
                self.session.delete(entity)

            return None

        # Pass through
        entity = Entity.get_if_valid(self.session, ent_name, tags)
        if entity is None:
            raise FuseOSError(ENOENT)

        return super().unlink(join(entity.path, rest_path))

    def readlink(self, path):
        """
        Treat /@tag_1/.../@tag_n/ent_name as a symlink
        """
        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names or ent_name is None:
            raise FuseOSError(ENOENT)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        entity = Entity.get_if_valid(self.session, ent_name, tags)
        if entity is None:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            return entity.path
        else:
            # Pass through
            return super().readlink(join(entity.path, rest_path))

    def readdir(self, path, fh):
        """
        If path is
        - "/", then list all tags,
        - "/@tag_1/../@tag_n, then list all entities filtered by the tags,
        - otherwise pass through.
        """
        if path == "/":
            return ["@" + name[0] for name in self.session.query(Tag.name)]

        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names:
            raise FuseOSError(ENOENT)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None and rest_path is None:
            # Filter entity by tags
            tag_names = [tag.name for tag in tags]
            res = self.session.query(Entity.name).join(Entity.tags)\
                .filter(Tag.name.in_(tag_names))\
                .group_by(Entity.name)\
                .having(func.count(Entity.name) == len(tag_names))
            return [e for e, in res]

        # Pass through
        entity = Entity.get_if_valid(self.session, ent_name, tags)
        if entity is None:
            raise FuseOSError(ENOENT)

        path = entity.path
        if rest_path:
            path = join(path, rest_path)
        return super().readdir(path, fh)
