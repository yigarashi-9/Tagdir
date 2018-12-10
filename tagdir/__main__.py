import logging

from .db import setup_db
from .fusepy.fuse import FUSE
from .tagdir import Tagdir
from .watch import EntityPathChangeObserver


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

    setup_db("sqlite:///test.db")

    logging.basicConfig(level=logging.DEBUG)
    observer = EntityPathChangeObserver.get_instance()
    observer.start()
    FUSE(Tagdir(), args.mount, foreground=True,
         allow_other=True, fsname="Tagdir_test")
    observer.stop()
    observer.join()


if __name__ == '__main__':
    main()
