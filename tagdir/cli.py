import argparse
import os
import pathlib
import re
import subprocess
import sys
from typing import Optional

import psutil
from sqlalchemy import create_engine
import xattr

from .fusepy.fuse import FUSE
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

    engine = create_engine("sqlite:///" + args.db, echo=False)
    FUSE(Tagdir(engine), args.mountpoint, foreground=True, allow_other=True,
         fsname="Tagdir_" + args.name)
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


def _main() -> int:
    parser = argparse.ArgumentParser(description="Tagdir CLI tool")
    subparsers = parser.add_subparsers()

    parser_mount = subparsers.add_parser("mount")
    parser_mount.add_argument("name", type=name_validator)
    parser_mount.add_argument("db", type=str)
    parser_mount.add_argument("mountpoint", type=str)
    parser_mount.set_defaults(func=mount)

    parser_mktag = subparsers.add_parser("mktag")
    parser_mktag.add_argument("--name", type=name_validator, nargs="?",
                              default=None)
    parser_mktag.add_argument("tags", type=str, nargs="+")
    parser_mktag.set_defaults(func=mktag)

    parser_rmtag = subparsers.add_parser("rmtag")
    parser_rmtag.add_argument("--name", type=name_validator, nargs="?",
                              default=None)
    parser_rmtag.add_argument("tags", type=str, nargs="+")
    parser_rmtag.set_defaults(func=rmtag)

    parser_tag = subparsers.add_parser("tag")
    parser_tag.add_argument("--name", type=name_validator, nargs="?",
                            default=None)
    parser_tag.add_argument("tags", type=str, nargs="+")
    parser_tag.add_argument("path", type=str)
    parser_tag.set_defaults(func=tag)

    parser_tag = subparsers.add_parser("untag")
    parser_tag.add_argument("--name", type=name_validator, nargs="?",
                            default=None)
    parser_tag.add_argument("tags", type=str, nargs="+")
    parser_tag.add_argument("path", type=str)
    parser_tag.set_defaults(func=untag)

    # TODO: list sub-command

    args = parser.parse_args()
    mountpoint = get_mountpoint(args.name)
    return args.func(args, mountpoint)


def main():
    sys.exit(_main())
