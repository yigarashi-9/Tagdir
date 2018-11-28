from errno import EINVAL, ENOENT
import pathlib
import stat

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from .db import session_scope
from .fusepy.fuse import FuseOSError
from .fusepy.logging import LoggingMixIn
from .fusepy.loopback import Loopback
from .models import Base, Entity, Tag
from .utils import get_entity_path, parse_path


class Tagdir(LoggingMixIn, Loopback):
    def __init__(self, engine):
        Base.metadata.create_all(engine)
        self.Session = sessionmaker(bind=engine)
        super().__init__()

    def __call__(self, op, path, *args):
        self.log(self, op, path, *args)

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

            path = get_entity_path(session, tags, ent_name, rest_path)
            if path is None:
                raise FuseOSError(ENOENT)
            return super().__call__(op, path, *args)

    def access(self, path, mode):
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
        path = get_entity_path(self.session, tags, ent_name, rest_path)
        if path is None:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            return 0
        else:
            return super().access(path, mode)

    def getattr(self, path, fh=None):
        """
        If path is
        - /@tag_1/../@tag_n, then treat as a directory
        - /@tag_1/../@tag_n/ent_name, then treat as a symlink
        - /@tag_1/../@tag_n/ent_name/rest_path, then pass through
        """
        # TODO: Return rich information
        st = {}

        if path == "/":
            st['st_mode'] = 0o0644 | stat.S_IFDIR
            return st

        tag_names, ent_name, rest_path = parse_path(path)

        if not tag_names:
            raise FuseOSError(ENOENT)

        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            # Return attribute for a tag
            st['st_mode'] = 0o0644 | stat.S_IFDIR
            return st

        path = get_entity_path(self.session, tags, ent_name, rest_path)
        if path is None:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            # Return attribute for a link to an entity
            st['st_mode'] = 0o0644 | stat.S_IFLNK
            return st
        else:
            # Pass through
            return super().getattr(path, fh)

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
                    self.session.add(Tag(tag_name))
            return None

        # Pass through
        try:
            tags = [Tag.get_by_name(self.session, tag_name)
                    for tag_name in tag_names]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        path = get_entity_path(self.session, tags, ent_name, rest_path)
        if path is None:
            raise FuseOSError(ENOENT)
        return super().mkdir(path, mode)

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
        path = get_entity_path(self.session, tags, ent_name, rest_path)
        if path is None:
            raise FuseOSError(ENOENT)
        return super().rmdir(path)

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
                entity = Entity(source.name, str(source), [])
                self.session.add(entity)

            if entity.path != str(source):
                # Buggy state: multiple paths for one entity
                # TODO: Is EINVAL suitable?
                raise FuseOSError(EINVAL)

            for tag in tags:
                if tag not in entity.tags:
                    entity.tags.append(tag)
            return None

        # Pass through
        path = get_entity_path(self.session, tags, ent_name, rest_path)
        if path is None:
            raise FuseOSError(ENOENT)
        return super().symlink(path, source)

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
            return None

        # Pass through
        path = get_entity_path(self.session, tags, ent_name, rest_path)
        if path is None:
            raise FuseOSError(ENOENT)
        return super().unlink(path)

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

        path = get_entity_path(self.session, tags, ent_name, rest_path)
        if path is None:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            return path
        else:
            # Pass through
            return super().readlink(path)

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
        path = get_entity_path(self.session, tags, ent_name, rest_path)
        if path is None:
            raise FuseOSError(ENOENT)
        return super().readdir(path, fh)
