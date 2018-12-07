import logging

from .db import setup_db
from .fusepy.fuse import FUSE
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

    setup_db("sqlite:///test.db")

    logging.basicConfig(level=logging.DEBUG)
    observer = get_observer()
    observer.start()
    FUSE(Tagdir(observer), args.mount, foreground=True,
         allow_other=True, fsname="Tagdir_test")
    observer.stop()
    observer.join()


if __name__ == '__main__':
    main()
