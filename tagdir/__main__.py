import logging

from sqlalchemy import create_engine

from .fusepy.fuse import FUSE
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

    logging.basicConfig(level=logging.DEBUG)
    FUSE(Tagdir(engine), args.mount, foreground=True, allow_other=True)


if __name__ == '__main__':
    main()
