import os
import stat
import time

from sqlalchemy import Column, ForeignKey, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.exc import NoResultFound


Base = declarative_base()

tagging = Table("tagging", Base.metadata,
                Column('entity_id', ForeignKey('entities.id'),
                       primary_key=True),
                Column('tag_id', ForeignKey('tags.id'), primary_key=True))


class Attr(Base):
    __tablename__ = "attrs"
    id = Column(Integer, primary_key=True)
    st_mode = Column(Integer)
    st_uid = Column(Integer)
    st_gid = Column(Integer)
    st_atimespec = Column(Integer)
    st_mtimespec = Column(Integer)
    st_ctimespec = Column(Integer)

    def __init__(self, st_mode):
        self.st_mode = st_mode
        self.st_uid = os.getuid()
        self.st_gid = os.getgid()
        now = int(time.time())
        self.st_atimespec = now
        self.st_mtimespec = now
        self.st_ctimespec = now

    @staticmethod
    def new_tag_attr():
        return Attr(0o644 | stat.S_IFDIR)

    @staticmethod
    def new_entity_attr():
        return Attr(0o644 | stat.S_IFLNK)

    @staticmethod
    def new_root_attr():
        return Attr(0o644 | stat.S_IFDIR)

    @staticmethod
    def get_root_attr(session):
        return session.query(Attr).get(1)

    def as_dict(self):
        from .fusepy.fuse import c_timespec
        return {"st_mode": self.st_mode, "st_uid": self.st_uid,
                "st_gid": self.st_gid,
                "st_atimespec": c_timespec(self.st_atimespec, 0),
                "st_mtimespec": c_timespec(self.st_mtimespec, 0),
                "st_ctimespec": c_timespec(self.st_ctimespec, 0)}


class NodeMixIn:
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    @declared_attr
    def attr_id(cls):
        return Column(Integer, ForeignKey('attrs.id'))

    @declared_attr
    def attr(cls):
        return relationship("Attr", cascade="delete",
                            backref=backref(cls.__tablename__, uselist=False))

    @classmethod
    def get_by_name(cls, session, name):
        return session.query(cls).filter(cls.name == name).one()


class Entity(NodeMixIn, Base):
    __tablename__ = "entities"
    path = Column(String, unique=True)
    tags = relationship("Tag", secondary=tagging, back_populates="entities")

    def __init__(self, name, attr, path, tags):
        self.name = name
        self.attr = attr
        self.path = path
        self.tags = tags

    def __repr__(self):
        return self.name

    @staticmethod
    def get_if_valid(session, ent_name, tags):
        """
        Return entity only if valid tags are specified
        """
        try:
            entity = Entity.get_by_name(session, ent_name)
        except NoResultFound:
            return None

        if not entity.has_tags(tags):
            return None

        return entity

    def has_tags(self, tags):
        for tag in tags:
            if tag not in self.tags:
                return False
        return True


class Tag(NodeMixIn, Base):
    __tablename__ = "tags"
    entities = relationship("Entity", secondary=tagging, back_populates="tags")

    def __init__(self, name, attr):
        self.name = name
        self.attr = attr

    def __str__(self):
        return "@" + self.name

    def remove(self, session):
        """
        remove redundant entities, too
        """
        session.delete(self)
        for entity in self.entities:
            if not entity.tags:
                session.delete(entity)
