import pathlib

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from watchdog import events
from watchdog.observers import Observer

from .db import session_scope
from .models import Entity


class EntityPathChangeObserver(Observer):  # type: ignore
    def schedule_if_new_path(self, session_cls, path):
        parent = str(pathlib.Path(path).parent)
        path_set = set(em.watch.path for em in self.emitters)
        if parent not in path_set:
            event_handler = EntityPathChangeHandler(session_cls, self)
            self.schedule(event_handler, parent)

    def unschedule_redundant_handlers(self, session_cls):
        with session_scope(session_cls) as session:
            ent_path_set = set(path for path, in session.query(Entity.path))

        delete_candidate = set()

        for em in self.emitters:
            if em.watch.path not in ent_path_set:
                delete_candidate.add(em.watch)

        for watch in delete_candidate:
            self.unschedule(watch)


class EntityPathChangeHandler(events.FileSystemEventHandler):  # type: ignore
    def __init__(self, session_cls, observer):
        self.session_cls = session_cls
        self.observer = observer
        super().__init__()

    def on_moved(self, event):
        if not isinstance(event, events.DirMovedEvent):
            return

        src_path = pathlib.Path(event.src_path)
        with session_scope(self.session_cls) as session:
            try:
                entity = session.query(Entity).filter(
                    Entity.name == src_path.name,
                    Entity.path == str(src_path)).one()
            except NoResultFound:
                return

            dest_path = pathlib.Path(event.dest_path)
            entity.name = dest_path.name
            entity.path = str(dest_path)
            self.observer.schedule_if_new_path(self.session_cls, dest_path)

    def on_deleted(self, event):
        if not isinstance(event, events.DirDeletedEvent):
            return

        src_path = pathlib.Path(event.src_path)
        with session_scope(self.session_cls) as session:
            try:
                entity = session.query(Entity).filter(
                    Entity.name == src_path.name,
                    Entity.path == str(src_path)).one()
            except NoResultFound:
                return
            session.delete(entity)
            self.observer.unschedule_redundant_handlers(self.session_cls)


def get_observer(engine):
    session_cls = sessionmaker(bind=engine)
    observer = EntityPathChangeObserver()
    event_handler = EntityPathChangeHandler(session_cls, observer)

    path_set = set()
    with session_scope(session_cls) as session:
        for path, in session.query(Entity.path):
            path_set.add(pathlib.Path(path))

    for path in path_set:
        observer.schedule(event_handler, path)

    return observer
