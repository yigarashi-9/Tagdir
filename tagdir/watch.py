import logging
import pathlib
import threading

from sqlalchemy.orm.exc import NoResultFound
from watchdog import events
from watchdog.observers import Observer

from .db import session_scope
from .models import Entity


class Singleton(type):
    _lock = threading.Lock()
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if not cls._instance:
                    cls._instance = \
                        super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance

    def get_instance(cls):
        return cls()


class EntityPathChangeObserver(Observer, metaclass=Singleton):  # type: ignore
    def __init__(self):
        super().__init__()
        path_set = set()
        with session_scope() as session:
            for path, in session.query(Entity.path):
                path_set.add(pathlib.Path(path))

        for path in path_set:
            self.schedule_if_new_path(path)

    def schedule(self, event_handler, path, recursive=False):
        logger = logging.getLogger(__name__)
        logger.debug("Add handler for {}".format(path))
        return super().schedule(event_handler, path, recursive)

    def schedule_if_new_path(self, path):
        parent = str(pathlib.Path(path).parent)
        path_set = set(em.watch.path for em in self.emitters)
        if parent not in path_set:
            event_handler = EntityPathChangeHandler()
            self.schedule(event_handler, parent)

    def unschedule_redundant_handlers(self):
        with session_scope() as session:
            ent_path_set = set(path for path, in session.query(Entity.path))

        delete_candidate = set()

        for em in self.emitters:
            if em.watch.path not in ent_path_set:
                delete_candidate.add(em.watch)

        for watch in delete_candidate:
            self.unschedule(watch)


class EntityPathChangeHandler(events.FileSystemEventHandler):  # type: ignore
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        super().__init__()

    def on_moved(self, event):
        if not isinstance(event, events.DirMovedEvent):
            return

        src_path = pathlib.Path(event.src_path)
        with session_scope() as session:
            try:
                entity = session.query(Entity).filter(
                    Entity.name == src_path.name,
                    Entity.path == str(src_path)).one()
            except NoResultFound:
                return

            dest_path = pathlib.Path(event.dest_path)
            entity.name = dest_path.name
            entity.path = str(dest_path)
            observer = EntityPathChangeObserver.get_instance()
            observer.schedule_if_new_path(dest_path)

            msg = "Destination of {} is changed from {} to {}".format(
                entity.name, event.src_path, event.dest_path)
            self.logger.debug(msg)

    def on_deleted(self, event):
        if not isinstance(event, events.DirDeletedEvent):
            return

        src_path = pathlib.Path(event.src_path)
        with session_scope() as session:
            try:
                entity = session.query(Entity).filter(
                    Entity.name == src_path.name,
                    Entity.path == str(src_path)).one()
            except NoResultFound:
                return
            session.delete(entity)
            observer = EntityPathChangeObserver.get_instance()
            observer.unschedule_redundant_handlers()

            msg = "{} is deleted because its destination {} is deleted".format(
                src_path.name, event.src_path)
            self.logger.debug(msg)
