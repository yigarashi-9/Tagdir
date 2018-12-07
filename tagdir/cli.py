import argparse
import os
import pathlib
import re
import subprocess
import sys
from typing import Optional

import psutil
import xattr

from .db import setup_db
from .fusepy.fuse import FUSE
from .observer import init_observer
from .tagdir import ENTINFO_PATH, Tagdir


def is_tagdir(disk) -> bool:
    parts = disk.device.split("_")
    if len(parts) != 2:
        return False
    return parts[0] == "Tagdir"


def get_mountpoint(name: Optional[str]) -> Optional[str]:
    tagdirs = list(filter(is_tagdir, psutil.disk_partitions(all=True)))

    if name is None and len(tagdirs) == 1:
        return tagdirs[0].mountpoint

    mountpoint = None

    for tagdir in tagdirs:
        _, _name = tagdir.device.split("_")
        if _name == name:
            mountpoint = tagdir.mountpoint
            break
    return mountpoint


def name_validator(s: str) -> str:
    r = re.compile(r"[a-z]+")
    if not r.match(s):
        raise argparse.ArgumentTypeError("[a-z]+ is required")
    else:
        return s


def mount(args: argparse.Namespace, mountpoint: Optional[str]) -> int:
    """
    TODO
    - Exception handling
    - Daemonizing
    """
    if args.name is None:
        print("name option is required.")
        return 0

    if mountpoint:
        print("{} already exists.".format(args.name))
        return 0

    setup_db("sqlite:///" + args.db)

    observer = init_observer()
    observer.start()
    FUSE(Tagdir(), args.mountpoint, foreground=True,
         allow_other=True, fsname="Tagdir_" + args.name)
    observer.stop()
    observer.join()
    return 0


def mktag(args: argparse.Namespace, mountpoint: Optional[str]) -> int:
    if mountpoint is None:
        print("mountpoint is not fonund.")
        return -1

    paths = [os.path.join(mountpoint, "@" + tag) for tag in args.tags]
    subprocess.run(["mkdir"] + paths, capture_output=True)
    return 0


def rmtag(args: argparse.Namespace, mountpoint: Optional[str]) -> int:
    if mountpoint is None:
        print("mountpoint is not fonund.")
        return -1

    paths = [os.path.join(mountpoint, "@" + tag) for tag in args.tags]
    subprocess.run(["rmdir"] + paths, capture_output=True)
    return 0


def tag(args: argparse.Namespace, mountpoint: Optional[str]) -> int:
    if mountpoint is None:
        print("mountpoint is not fonund.")
        return -1

    source = str(pathlib.Path(args.path).resolve())
    for tag in args.tags:
        tag_path = os.path.join(mountpoint, "@" + tag)
        subprocess.run(["ln", "-s", source, tag_path], capture_output=True)
    return 0


def untag(args: argparse.Namespace, mountpoint: Optional[str]) -> int:
    if mountpoint is None:
        print("mountpoint is not fonund.")
        return -1

    source = pathlib.Path(args.path).resolve()

    # TODO: Error handling
    # See https://github.com/xattr/xattr/blob/master/xattr/__init__.py
    attrs = xattr.xattr(mountpoint + ENTINFO_PATH)

    if source.name not in attrs:
        print("No tagged entry {}".format(source.name))
        return -1

    vals = attrs[source.name].decode("utf-8").split(",")

    if str(source) != vals[0]:
        print("Tagged entry {} is not {}".format(source.name, args.path))
        return -1

    path = os.path.join(mountpoint, *("@" + tag for tag in args.tags),
                        source.name)
    subprocess.run(["unlink", path], capture_output=True)
    return 0


def listag(args: argparse.Namespace, mountpoint: Optional[str]) -> int:
    if mountpoint is None:
        print("mountpoint is not fonund.")
        return -1

    if args.path is None:
        s = subprocess.check_output(["ls", "-1", mountpoint]).decode("utf-8")
        for tag in sorted(s.split("\n")[:-1]):
            print(tag[1:])
        return 0

    source = pathlib.Path(args.path).resolve()

    # TODO: Error handling
    # See https://github.com/xattr/xattr/blob/master/xattr/__init__.py
    attrs = xattr.xattr(mountpoint + ENTINFO_PATH)

    if source.name not in attrs:
        print("No tagged entry {}".format(source.name))
        return -1

    vals = attrs[source.name].decode("utf-8").split(",")

    if str(source) != vals[0]:
        print("Tagged entry {} is not {}".format(source.name, args.path))
        return -1

    for tag in sorted(vals[1:]):
        print(tag)

    return 0


def _main() -> int:
    name_parser = argparse.ArgumentParser(add_help=False)
    name_parser.add_argument("--name", type=name_validator, nargs="?",
                             default=None)

    tags_parser = argparse.ArgumentParser(add_help=False)
    tags_parser.add_argument("tags", type=str, nargs="+")

    path_parser = argparse.ArgumentParser(add_help=False)
    path_parser.add_argument("path", type=str)

    parser = argparse.ArgumentParser(description="Tagdir CLI tool")
    subparsers = parser.add_subparsers()

    parser_mount = subparsers.add_parser("mount")
    parser_mount.add_argument("name", type=name_validator)
    parser_mount.add_argument("db", type=str)
    parser_mount.add_argument("mountpoint", type=str)
    parser_mount.set_defaults(func=mount)

    parser_mktag = subparsers.add_parser(
        "mktag", parents=[name_parser, tags_parser])
    parser_mktag.set_defaults(func=mktag)

    parser_rmtag = subparsers.add_parser(
        "rmtag", parents=[name_parser, tags_parser])
    parser_rmtag.set_defaults(func=rmtag)

    parser_tag = subparsers.add_parser(
        "tag", parents=[name_parser, tags_parser, path_parser])
    parser_tag.set_defaults(func=tag)

    parser_tag = subparsers.add_parser(
        "untag", parents=[name_parser, tags_parser, path_parser])
    parser_tag.set_defaults(func=untag)

    parser_tag = subparsers.add_parser("listag", parents=[name_parser])
    path_parser.add_argument("path", type=str, nargs="?")
    parser_tag.set_defaults(func=listag)

    args = parser.parse_args()
    mountpoint = get_mountpoint(args.name)
    return args.func(args, mountpoint)


def main():
    sys.exit(_main())
