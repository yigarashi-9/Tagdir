import logging

from .db import engine
from .fusepy.fuse import FUSE
from .models import Base
from .tagdir import Tagdir


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mount')
    args = parser.parse_args()

    import os
    try:
        os.remove("../test.db")
    except OSError:
        pass

    Base.metadata.create_all(engine)

    logging.basicConfig(level=logging.DEBUG)
    FUSE(Tagdir(), args.mount, foreground=True, allow_other=True)


if __name__ == '__main__':
    main()
