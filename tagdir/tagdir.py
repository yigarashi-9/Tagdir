from errno import EINVAL, ENOENT
import pathlib
import stat

from sqlalchemy import and_, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from .fusepy.fuse import FuseOSError
from .fusepy.logging import LoggingMixIn
from .fusepy.loopback import Loopback
from .models import Entity, Tag
from .utils import parse_path, prepare_passthrough


class Tagdir(LoggingMixIn, Loopback):
    def __init__(self, engine):
        super().__init__()
        self.engine = engine

    def __call__(self, op, path, *args):
        self.log(self, op, path, *args)

        self.session = sessionmaker(bind=self.engine)()

        try:
            if hasattr(self, op):
                res = getattr(self, op)(path, *args)
            else:
                tag_strs, ent_name, rest_path = parse_path(path)

                if tag_strs == []:
                    raise FuseOSError(ENOENT)

                try:
                    tags = [self.session.query(Tag)
                                .filter(Tag.name == tag_str).one()
                            for tag_str in tag_strs]
                except NoResultFound:
                    raise FuseOSError(ENOENT)

                if ent_name is None:
                    raise FuseOSError(ENOENT)

                path = prepare_passthrough(self.session, ent_name,
                                           rest_path, tags)
                if path is None:
                    raise FuseOSError(ENOENT)

                res = super().__call__(op, path, *args)
            return res
        finally:
            self.session.commit()
            self.session.close()

    def access(self, path, mode):
        if path == "/":
            return 0

        tag_strs, ent_name, rest_path = parse_path(path)

        if tag_strs == []:
            raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in tag_strs]
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

        tag_strs, ent_name, rest_path = parse_path(path)

        if tag_strs == []:
            raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in tag_strs]
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
        tag_strs, ent_name, rest_path = parse_path(path)

        if tag_strs == []:
            # TODO: Is ENOENT suitable?
            raise FuseOSError(ENOENT)

        if ent_name is not None and rest_path is None:
            # TODO: Is ENOENT suitable?
            raise FuseOSError(ENOENT)

        if ent_name is None:
            # create new tags
            for tag_str in tag_strs:
                try:
                    self.session.query(Tag).filter(Tag.name == tag_str).one()
                except NoResultFound:
                    self.session.add(Tag(tag_str))
        else:
            # pass through
            try:
                tags = [self.session.query(Tag)
                            .filter(Tag.name == tag_str).one()
                        for tag_str in tag_strs]
            except NoResultFound:
                raise FuseOSError(ENOENT)

            path = prepare_passthrough(self.session, ent_name, rest_path, tags)
            if path is None:
                raise FuseOSError(ENOENT)

            return super().mkdir(path, mode)

    def rmdir(self, path):
        tag_strs, ent_name, rest_path = parse_path(path)

        if tag_strs == []:
            raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in tag_strs]
        except NoResultFound:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            # remove tags
            for tag in tags:
                tag.remove()
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
        tag_strs, ent_name, rest_path = parse_path(target)

        if tag_strs == []:
            raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in tag_strs]
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
        tag_strs, ent_name, rest_path = parse_path(path)

        if tag_strs == [] or ent_name is None:
            raise FuseOSError(EINVAL)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in tag_strs]
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
        tag_strs, ent_name, rest_path = parse_path(path)

        if tag_strs == []:
            raise FuseOSError(ENOENT)

        if ent_name is None:
            raise FuseOSError(EINVAL)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in tag_strs]
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
        tag_strs, ent_name, rest_path = parse_path(path)

        if tag_strs == []:
            if ent_name is None and rest_path is None:
                # list all tags
                res = ["@" + name[0] for name in self.session.query(Tag.name)]
                return res
            else:
                raise FuseOSError(ENOENT)

        try:
            tags = [self.session.query(Tag).filter(Tag.name == tag_str).one()
                    for tag_str in tag_strs]
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
