from sqlalchemy import Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

tagging = Table("tagging", Base.metadata,
                Column('entity_id', ForeignKey('entities.id'),
                       primary_key=True),
                Column('tag_id', ForeignKey('tags.id'), primary_key=True))


class ModelUtils:
    @classmethod
    def get_by_name(cls, session, name):
        return session.query(cls).filter(cls.name == name).one()


class Entity(Base, ModelUtils):
    __tablename__ = "entities"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    path = Column(String, unique=True)
    tags = relationship("Tag", secondary=tagging, back_populates="entities")

    def __init__(self, name, path, tags):
        self.name = name
        self.path = path
        self.tags = tags

    def __repr__(self):
        return self.name

    def has_tags(self, tags):
        for tag in tags:
            if tag not in self.tags:
                return False
        return True


class Tag(Base, ModelUtils):
    __tablename__ = "tags"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    entities = relationship("Entity", secondary=tagging, back_populates="tags")

    def __init__(self, name):
        self.name = name

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
