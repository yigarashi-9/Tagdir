from errno import EINVAL, ENOENT
import pathlib
import stat

from sqlalchemy import and_, func
from sqlalchemy.orm.exc import NoResultFound

from .db import session_scope
from .fusepy.fuse import FuseOSError
from .fusepy.logging import LoggingMixIn
from .fusepy.loopback import Loopback
from .models import Entity, Tag
from .utils import parse_path, prepare_passthrough


class Tagdir(LoggingMixIn, Loopback):
    def __call__(self, op, path, *args):
        self.log(self, op, path, *args)

        with session_scope() as session:
            if hasattr(self, op):
                # Operations specific to tagdir
                self.session = session
                return getattr(self, op)(path, *args)

            # Pass through begins
            raw_tags, ent_name, rest_path = parse_path(path)

            if not raw_tags:
                raise FuseOSError(ENOENT)

            try:
                tags = [session.query(Tag).filter(Tag.name == tag_str).one()
                        for tag_str in raw_tags]
            except NoResultFound:
                raise FuseOSError(ENOENT)

            if ent_name is None:
                raise FuseOSError(ENOENT)

            path = prepare_passthrough(session, ent_name, rest_path, tags)
            if path is None:
                raise FuseOSError(ENOENT)

            return super().__call__(op, path, *args)

    def access(self, path, mode):
        if path == "/":
            return 0

        raw_tags, ent_name, rest_path = parse_path(path)

        if raw_tags == []:
            raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in raw_tags]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            return 0

        path = prepare_passthrough(self.session, ent_name, rest_path, tags)
        if path is None:
            raise FuseOSError(ENOENT)

        return super().access(path, mode)

    def getattr(self, path, fh=None):
        st = {}

        if path == "/":
            st['st_mode'] = 0o0644 | stat.S_IFDIR
            return st

        raw_tags, ent_name, rest_path = parse_path(path)

        if raw_tags == []:
            raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in raw_tags]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            st['st_mode'] = 0o0644 | stat.S_IFDIR
            return st

        if rest_path is None:
            try:
                entity = self.session.query(Entity)\
                    .filter(Entity.name == ent_name).one()
            except NoResultFound:
                raise FuseOSError(ENOENT)

            for tag in tags:
                if tag not in entity.tags:
                    raise FuseOSError(ENOENT)

            st['st_mode'] = 0o0644 | stat.S_IFLNK
            return st

        path = prepare_passthrough(self.session, ent_name, rest_path, tags)
        if path is None:
            raise FuseOSError(ENOENT)

        return super().getattr(path, fh)

    def mkdir(self, path, mode):
        raw_tags, ent_name, rest_path = parse_path(path)

        if raw_tags == []:
            # TODO: Is ENOENT suitable?
            raise FuseOSError(ENOENT)

        if ent_name is not None and rest_path is None:
            # TODO: Is ENOENT suitable?
            raise FuseOSError(ENOENT)

        if ent_name is None:
            # create new tags
            for tag_str in raw_tags:
                try:
                    self.session.query(Tag).filter(Tag.name == tag_str).one()
                except NoResultFound:
                    self.session.add(Tag(tag_str))
        else:
            # pass through
            try:
                tags = [self.session.query(Tag)
                            .filter(Tag.name == tag_str).one()
                        for tag_str in raw_tags]
            except NoResultFound:
                raise FuseOSError(ENOENT)

            path = prepare_passthrough(self.session, ent_name, rest_path, tags)
            if path is None:
                raise FuseOSError(ENOENT)

            return super().mkdir(path, mode)

    def rmdir(self, path):
        raw_tags, ent_name, rest_path = parse_path(path)

        if raw_tags == []:
            raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in raw_tags]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            # remove tags
            for tag in tags:
                tag.remove(self.session)
        else:
            # pass through
            path = prepare_passthrough(self.session, ent_name, rest_path, tags)
            if path is None:
                raise FuseOSError(ENOENT)

            return super().rmdir(path)

    def symlink(self, target, source):
        """
        Suppose source is an absolute path
        """
        raw_tags, ent_name, rest_path = parse_path(target)

        if raw_tags == []:
            raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in raw_tags]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is not None and rest_path is None:
            # do tagging
            source = pathlib.Path(source).resolve()

            if source.name != ent_name:
                raise FuseOSError(EINVAL)

            try:
                query = and_(Entity.name == ent_name,
                             Entity.path == str(source))
                entity = self.session.query(Entity).filter(query).one()
            except NoResultFound:
                entity = Entity(source.name, str(source), [])
                self.session.add(entity)

            for tag in tags:
                if tag not in entity.tags:
                    entity.tags.append(tag)
        else:
            # pass through
            path = prepare_passthrough(self.session, ent_name, rest_path, tags)
            if path is None:
                raise FuseOSError(ENOENT)

            return super().symlink(path, source)

    def unlink(self, path):
        raw_tags, ent_name, rest_path = parse_path(path)

        if raw_tags == [] or ent_name is None:
            raise FuseOSError(EINVAL)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in raw_tags]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            # do untagging
            try:
                entity = self.session.query(Entity)\
                    .filter(Entity.name == ent_name).one()
            except NoResultFound:
                raise FuseOSError(ENOENT)

            for tag in tags:
                if tag not in entity.tags:
                    raise FuseOSError(ENOENT)

            for tag in tags:
                entity.tags.remove(tag)
        else:
            # pass through
            path = prepare_passthrough(self.session, ent_name, rest_path, tags)
            if path is None:
                raise FuseOSError(ENOENT)

            return super().unlink(path)

    def readlink(self, path):
        raw_tags, ent_name, rest_path = parse_path(path)

        if raw_tags == []:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            raise FuseOSError(EINVAL)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in raw_tags]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if rest_path is None:
            try:
                entity = self.session.query(Entity)\
                    .filter(Entity.name == ent_name).one()
            except NoResultFound:
                raise FuseOSError(ENOENT)

            for tag in tags:
                if tag not in entity.tags:
                    raise FuseOSError(ENOENT)

            return entity.path
        else:
            # pass through
            path = prepare_passthrough(self.session, ent_name, rest_path, tags)
            if path is None:
                raise FuseOSError(ENOENT)

            return super().readlink(path)

    def readdir(self, path, fh):
        raw_tags, ent_name, rest_path = parse_path(path)

        if raw_tags == []:
            if ent_name is None and rest_path is None:
                # list all tags
                res = ["@" + name[0] for name in self.session.query(Tag.name)]
                return res
            else:
                raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in raw_tags]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None and rest_path is None:
            # filter entity by tags
            tag_names = [tag.name for tag in tags]
            res = self.session.query(Entity.name).join(Entity.tags)\
                .filter(Tag.name.in_(tag_names))\
                .group_by(Entity.name)\
                .having(func.count(Entity.name) == len(tag_names))
            return [e for e, in res]
        else:
            # pass through
            path = prepare_passthrough(self.session, ent_name, rest_path, tags)
            if path is None:
                raise FuseOSError(ENOENT)

            return super().readdir(path, fh)
