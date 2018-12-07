import logging

from sqlalchemy import create_engine

from .fusepy.fuse import FUSE
from .models import Base
from .observer import get_observer
from .tagdir import Tagdir


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mount')
    args = parser.parse_args()

    import os
    try:
        os.remove("test.db")
    except OSError:
        pass

    engine = create_engine("sqlite:///test.db", echo=False)
    Base.metadata.create_all(engine)

    logging.basicConfig(level=logging.DEBUG)
    observer = get_observer(engine)
    observer.start()
    FUSE(Tagdir(engine, observer), args.mount, foreground=True,
         allow_other=True, fsname="Tagdir_test")
    observer.stop()
    observer.join()


if __name__ == '__main__':
    main()
